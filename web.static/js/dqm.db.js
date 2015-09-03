var mod = angular.module('dqm.db', []);

// this is the connection object used by this package
// it should not be used directly, but rather via make_websocket
var Connection = function (uri) {
    var noop = function () {};
    var not_implemented  = function () { throw "Method not implemented." };

    this.uri = uri;

    // defines the priorities for connection state
    // the highest is always displayed
    this.state_priorities = {
        'error':  { 'p': 15, 'c': 'danger' },
        'closed': { 'p': 10, 'c': 'danger' },
        'open':   { 'p': 6,  'c': 'warning' },
        'sync':   { 'p': 5,  'c': 'warning' },
        'live':   { 'p': 1,  'c': 'success' },

        // http specific
        'download':   { 'p': 6,  'c': 'danger' },
    };

    // factory-face handlers
    // should be overriden
    this.x_onopen    = noop;
    this.x_onmessage = noop;
    this.x_onclose   = noop;
    this.x_onerror   = noop;

    // catch-all
    this.x_onevent   = noop;

    // this is for displaying stuff
    // should not be used as a state machine
    this.make_state = function (state, description) {
        this.state = state;

        if (description) {
            this.state_string = state + ": " + description;
        } else {
            this.state_string = state;
        }

        this.state_priority = this.state_priorities[state].p;
        this.state_class = this.state_priorities[state].c;
    };

    this.open =  not_implemented;
    this.send =  not_implemented;
    this.close = not_implemented;

    this.tick = noop;

};

Connection.make_websocket = function (uri) {
    // retry logic: always reconnect (we have 5s ticks)
    var me = new Connection(uri);

    me.retry_count = 0;
    me.open = function () {
        me.retry_count = me.retry_count + 1;

        me.ws = new WebSocket(me.uri);
        me.ws.onmessage = function (evt) {
            me.x_onmessage(evt);
            me.x_onevent(evt);
        };

        me.ws.onopen = function (evt) {
            me.make_state("open");

            me.x_onopen(evt);
            me.x_onevent(evt);
        };

        me.ws.onclose = function (evt) {
            console.log("WebSocket died: ", evt, me);
            me.ws = null;
            me.make_state("closed", "disconnected");

            me.x_onclose(evt);
            me.x_onevent(evt);
        };

        me.ws.onerrror = function (evt, reason) {
            console.log("WebSocket error: ", evt, reason, arguments);

            me.x_onerror(evt);
            me.x_onevent(evt);
        };

        me.make_state("closed", "waiting");
        console.log("Created connection object: ", me.uri, me);
    };

    me.tick = function () {
        if (me.ws) {
            // this means we are connected
            // do nothing
            return;
        }

        me.open();
    };

    me.send = function (msg) {
        if (! me.ws) {
            throw "WebSocket not connected.";
        }

        me.ws.send(msg);
    };

    me.close = function () {
        if (! me.ws) {
            console.warning("Tried to disconnect to non-existing WebSocket.");
        } else {
            me.ws.close();
        }
    };

    return me;
};


// unmaintained and untested
Connection.make_http_proxy = function (uri) {
    // retry logic: always reconnect (we have 5s ticks)
    var me = new Connection(uri);

    me.requests = 0;

    me.open = function () {
        console.log("Created connection object: ", me.uri, me);
        me.make_state("open", "http mode");

        // time it out, so this happend outside $apply
        setTimeout(function () {
            var fake_evt = { 'type': 'open' };
            me.x_onopen(fake_evt);
            me.x_onevent(fake_evt);
        }, 0);
    };

    me.update_state = function (delta) {
        me.requests = me.requests + delta;

        if (me.requests) me.make_state("download", "" + me.requests  + "");
        else me.make_state("open", "http mode");
    };

    me.send = function (msg) {
        me.update_state(1);

        jQuery.ajax({
            url: me.uri,
            method: 'POST',
            dataType: 'json',
            data: angular.toJson({ 'messages': [msg] }),
            success: function (body) {
                me.update_state(-1);
                _.each(body.messages, function (msg) {
                    var fake_evt = { 'type': 'message', 'data': msg };
                    me.x_onmessage(fake_evt);
                    me.x_onevent(fake_evt);
                });

                me.x_onevent({ 'type': 'notify' });
            },
            error: function () {
                me.update_state(-1);
                console.log("http2websocket proxy failed", arguments);

                me.x_onerror({ 'type': 'error' });
                me.x_onevent({ 'type': 'error' });
            }
        });
    };

    me.close = function () {};
    return me;
};


mod.factory('SyncPool', ['$http', '$window', '$rootScope', function ($http, $window, $rootScope) {
    var factory = {};

    factory._conn = {};
    factory._conn_event_handlers = [];

    factory._sync_headers = {};
    factory._sync_header_handlers = [];

    var max_rev = function () {
        var args = Array.prototype.slice.call(arguments);
        return _.reduce(args, function (a, b) {
            if (a === null) return b;
            if (b === null) return a;

            if (a > b)
                return a;
            else
                return b;
        });
    };

    // interfaces for the Connection
    factory._handle_connection = function (conn, evt) {
        conn.send(angular.toJson({
            'event': 'sync_request',
            'known_rev': conn._sync_last_rev,
        }));
    };

    factory._handle_message = function (conn, evt) {
        var msg = angular.fromJson(evt.data);

        if (msg["event"] == "update_headers") {
            conn._sync_last_rev = max_rev(conn._sync_last_rev, msg["rev"][1]);

            var headers = _.map(msg["headers"], function (head) {
                head["_source"] = conn.uri;
                factory._sync_headers[head["_id"]] = head;
                return head;
            });

            _.each(factory._sync_header_handlers, function (handler) {
                handler(headers, false);
            });

            // check progress
            // only for websocket
            if (conn.ws) {
                if (msg["sync_to_rev"] !== conn._sync_last_rev) {
                    var pc = msg["total_sent"]*100 / msg["total_avail"];
                    conn.make_state("sync", "" + pc.toFixed(1) + "%");
                } else {
                    conn.make_state("live");
                }
            }
        }
    };

    factory._handle_evt = function (conn, evt) {
        _.each(factory._conn_event_handlers, function (handler) {
            handler(conn, evt);
        });

        $rootScope.$apply();
    };

    factory.Connection = Connection;
    factory.connect = function (uri) {
        var conn;

        if (uri.slice(0, 4) === "http") {
            conn = Connection.make_http_proxy(uri);
        } else {
            conn = Connection.make_websocket(uri);
        }

        conn._sync_last_rev = null;
        conn.x_onopen = function (evt) { return factory._handle_connection(conn, evt); };
        conn.x_onmessage = function (evt) { return factory._handle_message(conn, evt); };
        conn.x_onevent = function (evt) { return factory._handle_evt(conn, evt); };


        factory._conn[uri] = conn;
        factory._conn[uri].open();
    };

    factory.disconnect = function (uri) {
        var conn = factory._conn[uri];
        factory._conn[uri] = undefined;
        delete factory._conn[uri];

        conn.close();

        // we have to delete the headers
        var keys = _.keys(factory._sync_headers);
        _.each(keys, function (key) {
            if (factory._sync_headers[key]._source == uri) {
                delete factory._sync_headers[key];
            }
        });

        factory.force_replay();
    };

    // used after connection setup to reset listeners
    factory.force_replay = function () {
        _.each(factory._sync_header_handlers, function (handler) {
            factory.replay_headers(handler);
        });
    };

    factory.send_message = function (uri, msg) {
        var c = factory._conn[uri];
        c.send(msg);
    };

    // subscribe for "headers"
    factory.subscribe_headers = function (callback) {
        factory._sync_header_handlers.push(callback);

        // we have to rotate the current buffer to it
        factory.replay_headers(callback);
    };

    factory.unsubscribe_headers = function (callback) {
        factory._sync_header_handlers =
            _.filter(factory._sync_header_handlers, function (x) { return x !== callback });
    };

    factory.replay_headers = function (callback) {
        var headers = _.values(factory._sync_headers);
        callback(headers, true);
    };

    // these are the document event handlers used by SyncDocument
    factory.subscribe_events = function (callback) {
        factory._conn_event_handlers.push(callback);
    };

    factory.unsubscribe_events = function (callback) {
        factory._conn_event_handlers =
            _.filter(factory._conn_event_handlers, function (x) { return x !== callback });
    };

    // timer for various things
    factory._ti = $window.setInterval(function () {
        _.each(factory._conn, function (e) {
            e.tick();
        });
    }, 3*1000);

    return factory;
}]);

mod.factory('SyncDocument', ['SyncPool', '$window', '$http', '$q', function (SyncPool, $window, $http, $q) {
    var factory = {};
    factory._requests = [];

    factory._make_request = function (request) {
        var source = request["source"];
        var msg = { 'event': "request_documents", 'ids': [request["id"]] };
        SyncPool.send_message(source, angular.toJson(msg));
    };

    factory._process_response = function (docs) {
        var id_map = _.indexBy(docs, "_id")

        factory._requests = _.filter(factory._requests, function (request) {
            var d = id_map[request["id"]];

            if (d === undefined) {
                return true; // leave this request
            }

            request.defer.resolve(d);
            return false;
        });
    };

    factory._process_reject = function (requests_to_reject, reason) {
        factory._requests = _.filter(factory._requests, function (request) {
            if (_.indexOf(requests_to_reject, request, false) !== -1) {
                request.defer.reject(reason);
                return false;
            }

            return true;
        });
    };

    factory._process_event = function (conn, evt) {
        if (evt["type"] == "message") {
            var msg = angular.fromJson(evt.data);
            if (msg["event"] !=  "update_documents")
                return;

            factory._process_response(msg.documents);
        } else if (evt["type"] == "close") {
            var source = conn.uri;
            var to_rej = _.filter(factory._requests, function (r) { return (r["source"] === source); });

            factory._process_reject(to_rej, "WebSocket closed before replying.");
        } else {
            // console.log("event", evt);
        }
    };

    factory._process_timeout = function () {
        factory._requests = _.filter(factory._requests, function (req) {
            req.timeout = req.timeout - 1;

            if (req.timeout <= 0) {
                req.defer.reject("timeout reached");
                return false;
            }

            return true;
        });
    };

    factory.fetch = function (id) {
        var deferred = $q.defer();

        var header = SyncPool._sync_headers[id];
        if (! header) {
            deferred.reject("Unknown id: " + id);
            return deferred.promise;
        }

        var req = {
            "id": id,
            "source": header._source,
            "timeout": 15,
            "defer": deferred,
        };

        factory._requests.push(req);
        try {
            factory._make_request(req);
        } catch (e) {
            //console.error("Failed to send request.", e);
            factory._process_reject([req], "Failed to send request.");
        };

        var promise = deferred.promise;
        promise._sd_request = req;
        return promise;
    };

    factory.cancel = function (promise) {
        // the whole reason is actually to delay page-load
        // if we request stuff too fast
        // so this does not have to be efficient or anything
        var request = promise._sd_request;
        request.defer.reject("Fetch cancelled.");

        factory._requests = _.without(factory._requests, request);
    };

    // timer for various things
    factory._ti = $window.setInterval(function () {
        factory._process_timeout();
    }, 1*1000);

    SyncPool.subscribe_events(factory._process_event);
    return factory;
}]);

mod.factory('CachedDocument', ['SyncPool', 'SyncDocument', '$window', '$http', '$q', function (SyncPool, SyncDocument, $window, $http, $q) {
    var me = {};

    me.make_cacher_obj = function () {
        var cacher = {};

        cacher._doc_map = {};
        cacher._doc_requests = {};

        cacher._enter_document = function (id) {
            // needs to be a clone
            cacher._doc_map[id] = _.clone(SyncPool._sync_headers[id]);
        };

        cacher._refresh_document = function (id) {
            // check if document needs updating
            var doc = cacher._doc_map[id];
            var header = SyncPool._sync_headers[id];

            if (header === undefined) {
                // we are in this state if
                // connection providing the header is removed

                // it is (temporary) rechable, however we can't really do much
                return;
            }

            // don't update if the update is in progress
            if (cacher._doc_requests[id] !== undefined)
                return;

            if (doc["$cd_full"]) {
                var doc_rev = doc._rev;
                var header_rev = header._rev;

                if (doc_rev === header_rev) {
                    // no update is needed
                    return;
                }
            }

            // at this step either doc is not existing or too old
            // create a promise for it
            var p = SyncDocument.fetch(id);
            cacher._doc_requests[id] = p;

            // some promises are cancelled, but they still return
            p.then(function (new_doc) {
                if ((cacher._doc_map[id] === undefined) || (cacher._doc_map[id] !== doc)) {
                    console.log("Received response for a dead request.", cacher._doc_map[id], doc);
                    throw "Received response for a dead request.";
                }

                new_doc["$cd_full"] = true;
                cacher._doc_map[id] = new_doc;
                cacher.update();

                delete cacher._doc_requests[id];
            }, function (reason) {
                // error callback
                if (reason !== "Fetch cancelled.") {
                  console.log("Fetch error", doc, reason);
                }

                // delete the marker, so it can be re-requested
                // (if not removed)
                delete cacher._doc_requests[id];
            });
        };

        cacher._exit_document = function (id) {
            var doc = cacher._doc_map[id];
            if (cacher._doc_requests[id] !== undefined) {
                SyncDocument.cancel(cacher._doc_requests[id]);
                delete cacher._doc_requests[id];
            }

            delete cacher._doc_map[id];
        };

        cacher._process_event = function () {
            // at the time this is called, id list is table
            // and can't/won't change

            // we don't care about actual events
            // but we get called after header update happens
            // a -perfect- time to check for updates
            _.each(cacher._doc_map, function (v, k) { cacher._refresh_document(k); });
        };

        SyncPool.subscribe_events(cacher._process_event);
        cacher.destroy = function () {
            SyncPool.unsubscribe_events(me._process_event);
        };

        cacher.update = function () {};

        cacher.set_ids = function (ids) {
            // this is called then we get a complete new list of ids
            // we check if we have some overlap (keep the overlapping ids)
            var old_ids = _.keys(cacher._doc_map);

            var new_ids = _.difference(ids, old_ids);
            var rejected_ids = _.difference(old_ids, ids);

            _.each(new_ids, cacher._enter_document);
            _.each(rejected_ids, cacher._exit_document);

            // this refreshes stuff for us
            cacher._process_event();
            cacher.update();
        };

        return cacher;
    };

    me.get_ids_no_tracking = function (ids) {
        var cacher = me.make_cacher_obj();
        var deferred = $q.defer();

        cacher.update = function () {
            // check if all is fetch

            var upd = _.every(cacher._doc_map, function (doc, k) {
                if (doc["$cd_full"]) {
                    return true;
                }

                return false;
            });

            if (upd) {
                cacher.destroy();
                deferred.resolve(_.values(cacher._doc_map));
                delete cacher;
                delete deferred;
            }
        };

        cacher.set_ids(ids);
        return deferred.promise;
    };

    return me;
}]);

mod.controller('CachedDocumentCtrl', ['$scope', '$attrs', 'CachedDocument', function($scope, $attrs, CachedDocument) {
    if ((!$attrs.docId) == (!$attrs.docIdList)) {
        throw "Please provide doc-id-list or (exclusively) doc-id attribute.";
    }

    var cacher = CachedDocument.make_cacher_obj();
    $scope.document_map = cacher._doc_map;

    cacher.update = function () {
        $scope.documents = _.values(cacher._doc_map);

        $scope.doc = null;

        if ($attrs.docId && _.size($scope.documents)) {
            $scope.doc = $scope.documents[0];
        }
    };

    if ($attrs.docId) {
        $scope.$watch($attrs.docId, function (v) {
            if (v) {
                cacher.set_ids([v]);
            } else {
                cacher.set_ids([]);
            }
        }, true);
    }

    if ($attrs.docIdList) {
        $scope.$watch($attrs.docIdList, function (v) {
            cacher.set_ids(v || []);
        }, true);
    }

    $scope.$on("$destroy", function () {
        cacher.destroy();
    });
}]);

mod.directive('syncState', function ($window, SyncPool, SyncDocument) {
    return {
        restrict: 'E',
        scope: {},
        link: function (scope, elm, attrs) {
            // this is for displaying
            var get_state = function () {
                var highest_p = undefined;
                var highest_c = null;

                _.each(SyncPool._conn, function (c, k) {
                    if ((highest_p === undefined) || (highest_p < c.state_priority)) {
                        highest_p = c.state_priority;
                        highest_c = c;
                    }
                });

                return highest_c;
            };

            scope.SyncDocument = SyncDocument;
            scope.$watch(get_state, function (conn) {
                scope.highest = conn;
            });
        },
        template: '' +
            '<span class="label label-{{ highest.state_class }}">{{ highest.state_string }}</span>' +
            '<span ng-show="SyncDocument._requests.length" class="label label-warning"> {{SyncDocument._requests.length}} </span>'
    };
});

// this module tracks runs:
//   - maintain list of runs
//   - maintain know header list per run
mod.factory('SyncRun', ['SyncPool', '$window', '$http', '$q', function (SyncPool, $window, $http, $q) {
    var me = {};

    me.parse_headers = function (headers, reload) {
        if (reload) {
            me.runs_dct = {};
            me.runs = [];
        };

        var runs_added = false;

        _.each(headers, function (head) {
            if (head.run !== null)
                me.runs.push(head.run);

            // get the run dictionary
            if (me.runs_dct[head.run] === undefined) {
                runs_added = true;
                me.runs_dct[head.run] = {
                    'items': {},
                    'ids': [],
                };
            }
            var rd = me.runs_dct[head.run];

            // fill in "items" in the run directory
            rd["items"][head["_id"]] = head;

            // check if we need to regen ids
            var ids = rd["ids"];
            var id = head["_id"];
            if (_.indexOf(ids, id, true) == -1) {
                ids.push(id);
                ids.sort();
            }
        });

        me.runs.sort();
        me.runs = _.uniq(me.runs, true);
        me.runs.reverse();
    };

    me.get_runs = function () {
        return me.runs;
    };

    me.get_run_dictionary = function (run) {
        return me.runs_dct[run];
    };

    me.parse_headers([], true);
    SyncPool.subscribe_headers(me.parse_headers);

    return me;
}]);

mod.factory('RunStats', ['SyncRun', 'CachedDocument', '$window', '$http', '$q', 'DataUtils', function (SyncRun, CachedDocument, $window, $http, $q, DataUtils) {
    var me = {};

    // cache key is the run number
    // $last_rev should match (or the cache is cleared)
    me.cache = {};

    me.calc_stats = function (run, docs) {
        var stats = {
            'run': run,

            'run_started': null,
            'run_stopped': null,

            'file_delivery_mean': 0,
            'file_delivery_sigma': 0,

            'file_delivery_lumi': -1,
            'file_delivery_evt_accepted': -1,
            'file_delivery_fsize': -1,

            'jobs_total': 0,
            'jobs_crashed': 0,

            'jobs_max_events_total': 0,
            'jobs_max_events_rate': 0,
        };

        _.each(docs, function (doc) {
            if (doc.type == 'dqm-files') {
                stats.run_started = doc.extra.global_start;

                // this calculates delays from timestamps
                var streams = doc.extra.streams;
                var parsed_streams = DataUtils.process_stream_data(doc, null, null);

                var total_nevents = 0;
                var total_size = 0;

                _.each(parsed_streams.streams, function (so) {
                    _.map(so.values, function (x) {
                        stats["file_delivery_evt_accepted"] += x['evt_accepted'];
                        stats["file_delivery_fsize"] += x['fsize'];
                    });

                    var ct = _.map(so.values, function (x) { return x['delay_ctime']; });

                    var mean = _.reduce(ct, function(m, b) { return m + b; }, 0) / ct.length;
                    var mean2 = _.reduce(ct, function(m, b) { return m + b*b; }, 0) / ct.length
                    var std_dev = Math.sqrt(mean2 - mean*mean);

                    //console.log("mean", mean, "mean2", mean2, "sigma", std_dev);
                    stats["file_delivery_mean:" + so.key] = mean;
                    stats["file_delivery_sigma:" + so.key] = std_dev;
                    if (so.key == "DQM") {
                        stats.file_delivery_mean = mean;
                        stats.file_delivery_sigma = std_dev;
                    }

                    if ((so.values.length) && (stats["file_delivery_lumi"] < so.values[so.values.length - 1].lumi)) {
                        stats["file_delivery_lumi"] = so.values[so.values.length - 1].lumi;
                    }
                });
            }

        });

        _.each(docs, function (doc) {
            if (doc.type == 'dqm-source-state') {
                stats.jobs_total += 1;
                if ((doc.exit_code !== undefined) && (doc.exit_code !== 0))
                    stats.jobs_crashed += 1;

                if (stats["jobs_max_events_total"] < doc.events_total)
                    stats["jobs_max_events_total"] =  doc.events_total;

                if (stats["jobs_max_events_rate"] < doc.events_rate)
                    stats["jobs_max_events_rate"] =  doc.events_rate;

            }

        });

        return stats;
    };

    me.load_cache = function () {
        if (typeof($window.localStorage) === "undefined")
            return;

        if ($window.localStorage.run_stats_cache) {
            me.cache = JSON.parse($window.localStorage.run_stats_cache);
            console.log("Loaded run stats cache:", me.cache);
        }
    };

    me.save_cache = function () {
        if (typeof($window.localStorage) === "undefined")
            return;

        $window.localStorage.run_stats_cache = JSON.stringify(me.cache);
    };

    me.get_cache_length = function () {
        return _.size(me.cache);
    };

    me.clear_cache = function () {
        me.cache = {};
        me.save_cache();
    };

    me.get_run_stats = function (run) {
        // find last rev
        var run_dct = SyncRun.get_run_dictionary(run);
        var ids = _.clone(run_dct.ids);
        var last_rev = null;
        _.each(ids, function (id) {
            var header = run_dct.items[id];
            if (last_rev < header["_rev"]) {
                last_rev = header["_rev"];
            }
        });

        if (me.cache[run] && (me.cache[run]["$last_rev"] === last_rev)) {
            var deferred = $q.defer();
            deferred.resolve(me.cache[run]);
            return deferred.promise;
        }

        var prom = CachedDocument.get_ids_no_tracking(ids);
        return prom.then(function (x) {
            var stats = me.calc_stats(run, x);
            stats["$last_rev"] = last_rev;
            me.cache[run] = stats;

            console.log("Stats done for run:", run, stats);
            return stats;
        });
    };

    me.get_stats_for_runs = function (runs) {
        var promises = _.map(runs, me.get_run_stats);
        var p = $q.all(promises).then(function (stats) {
            console.log("Stats done for all selected runs:", stats)
            me.save_cache();

            return stats;
        });

        return p;
    };

    me.load_cache();
    return me;
}]);

mod.factory('DataUtils', [function () {
    var me = {};

    me.LUMI = 23.310893056;
    me.process_stream_data = function (data, limit_lumi, old_graph_data, do_interpolate) {
        var ret = {};

        if ((!data) || (!data.extra) || (!data.extra.streams)) {
            // we have no data available
            // so just render "no_data"

            return null;
        };

        // we want to extract "disabled" field from the old data
        var preserve = {};
        if (old_graph_data) {
            _.each(old_graph_data.streams, function (obj) {
                preserve[obj.key] = _.clone(obj);
            });
        };

        var date_start = data.extra.global_start;
        var streams = data.extra.streams;

        var filter = function (arr) {
            if (!arr)
                return arr;

            if (!limit_lumi)
                return arr;

            return arr.slice(Math.max(arr.length - limit_lumi, 0))
        };

        var interpolate = function (arr) {
            if (!arr)
                return arr;

            var ret = [];

            _.each(arr, function (v) {
                var last_entry = ret[ret.length - 1] || null;
                if (last_entry) {
                    var i = parseInt(last_entry.lumi) + 1;
                    var d = parseInt(v.lumi);
                    for (;i < v.lumi; i++) {
                        ret.push({
                            'lumi': i,
                            'mtime': -1,
                            'ctime': -1,

                            'start_offset': -1,
                            'delay': 0,

                            'evt_accepted':  0,
                            'evt_processed': 0,
                            'fsize': 0,
                        });
                    }
                }

                ret.push(v);
            });

            return ret;
        };

        var pretty_key = function (key) {
            var m = /^stream([A-Za-z0-9]+).*$/.exec(key);

            if (m) {
                return m[1];
            }

            return key;
        };

        var min_lumi = NaN;
        var max_lumi = NaN;

        var graph_data = _.map(_.keys(streams), function (k) {
            var lumis = filter(streams[k].lumis);
            var mtimes = filter(streams[k].mtimes);
            var ctimes = filter(streams[k].ctimes);
            if (!ctimes)
                ctimes = mtimes;

            var evt_accepted = filter(streams[k].evt_accepted);
            var evt_processed = filter(streams[k].evt_processed);
            var fsize = filter(streams[k].fsize);

            var key = pretty_key(k);
            var e = preserve[key] || {};

            e["key"] = pretty_key(k);
            e["stream"] = k;
            e["values"] = _.map(lumis, function (_lumi, index) {
                var lumi = parseInt(lumis[index]);
                var mtime = mtimes[index];
                var ctime = ctimes[index];

                if (!(min_lumi <= lumi)) min_lumi = lumi;
                if (!(max_lumi >= lumi)) max_lumi = lumi;

                // timeout from the begging of the run
                var start_offset_mtime = mtime - date_start - me.LUMI;
                var start_offset_ctime = ctime - date_start - me.LUMI;
                var lumi_offset = (lumi - 1) * me.LUMI;

                // timeout from the time we think this lumi happenned
                var delay_mtime = start_offset_mtime - lumi_offset;
                var delay_ctime = start_offset_ctime - lumi_offset;

                return {
                    // 'x': lumi,
                    // 'y': delay,

                    'lumi': lumi,
                    'mtime': mtime,
                    'ctime': ctime,

                    'start_offset_mtime': start_offset_mtime,
                    'start_offset_ctime': start_offset_ctime,
                    'delay_mtime': delay_mtime,
                    'delay_ctime': delay_ctime,

                    'evt_accepted': evt_accepted[index],
                    'evt_processed': evt_processed[index],
                    'fsize': fsize[index],

                    'size': 1,
                    'shape': "circle",
                }
            });

            e["values"] = _.sortBy(e["values"], "lumi");
            if (do_interpolate) {
                e["values"] = interpolate(_.sortBy(e["values"], "lumi"));
            }
            e["global_start"] = date_start;

            return e;
        });

        // calculate ticks, lazy, inefficient way
        var x_scale = d3.scale.linear().domain([min_lumi, max_lumi]);
        var ticks = x_scale.ticks(10);

        return {
            'streams': graph_data,
            'global_start': date_start,
            'ticks': ticks,
        };
    };

    me.format_timestamp = function (timestamp) {
        var d = new Date(parseFloat(timestamp)*1000);
        return d.toLocaleString();
    };

    me.make_file_tooltip = function(k, _v1, _v2, o) {
        var p = o.point;
        var gs = o.series.global_start;

        return ""
            + "<h3>" + k + "</h3>"
            + "<span>"
            + "Lumi number: <strong>" + p.lumi + "</strong><br />"
            + "Stream: <strong>" + o.series.stream + "</strong><br />"
            + "Events accepted / processed: <strong>" + p.evt_accepted + " / " + p.evt_processed + "</strong><br />"
            + "File size: <strong>" + d3.format('.02f')(parseFloat(p.fsize) / 1024 / 1024) + " mb.</strong><br />"
            + "-<br />"
            + "File m-time: <strong>" + me.format_timestamp(p.mtime) + "</strong><br />"
            + "File c-time: <strong>" + me.format_timestamp(p.ctime) + "</strong><br />"
            + "Time offset from the expected first delivery [delivery_start_offset]: <strong>" + p.start_offset  + " (seconds)</strong><br />"
            + "Delay [delivery_start_offset - (lumi_number - 1)*23.3]: <strong>" + p.delay + " (seconds)</strong><br />"
            + "-<br />"
            + "Run start (m-time on .runXXXX.global): <strong>" + me.format_timestamp(gs) + "</strong><br />"
            + "Delivery start (run_start + 23.3): <strong>" + me.format_timestamp(gs + me.LUMI) + "</strong><br />"
            + "</span>";
    };

    return me;
}]);
