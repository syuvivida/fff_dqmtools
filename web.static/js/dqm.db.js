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
                    conn.make_state("sync", "" + msg["total_sent"] + " / " + msg["sync_to_rev"]);
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

mod.controller('CachedDocumentCtrl', ['$scope', '$attrs', 'SyncPool', 'SyncDocument', function($scope, $attrs, SyncPool, SyncDocument) {
    var me = {};

    if ((!$attrs.docId) == (!$attrs.docIdList)) {
        throw "Please provide doc-id-list or (exclusively) doc-id attribute.";
    }

    me._doc_map = {};
    me._doc_requests = {};

    me._enter_document = function (id) {
        // needs to be a clone
        me._doc_map[id] = _.clone(SyncPool._sync_headers[id]);
    };

    me._refresh_document = function (id) {
        // check if document needs updating
        var doc = me._doc_map[id];
        var header = SyncPool._sync_headers[id];

        if (header === undefined) {
            // we are in this state if
            // connection providing the header is removed

            // it is (temporary) rechable, however we can't really do much
            return;
        }

        // don't update if the update is in progress
        if (me._doc_requests[id] !== undefined)
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
        me._doc_requests[id] = p;

        // some promises are cancelled, but they still return
        p.then(function (new_doc) {
            if ((me._doc_map[id] === undefined) || (me._doc_map[id] !== doc)) {
                console.log("Received response for a dead request.", me._doc_map[id], doc);
                throw "Received response for a dead request.";
            }

            new_doc["$cd_full"] = true;
            me._doc_map[id] = new_doc;
            me._update_scope();

            delete me._doc_requests[id];
        }, function (reason) {
            // error callback
            if (reason !== "Fetch cancelled.") {
              console.log("Fetch error", doc, reason);
            }

            // delete the marker, so it can be re-requested
            // (if not removed)
            delete me._doc_requests[id];
        });
    };

    me._exit_document = function (id) {
        var doc = me._doc_map[id];
        if (me._doc_requests[id] !== undefined) {
            SyncDocument.cancel(me._doc_requests[id]);
            delete me._doc_requests[id];
        }

        delete me._doc_map[id];
    };

    me._process_event = function () {
        // at the time this is called, id list is table
        // and can't/won't change

        // we don't care about actual events
        // but we get called after header update happens
        // a -perfect- time to check for updates
        _.each(me._doc_map, function (v, k) { me._refresh_document(k); });
    };

    me._update_scope = function () {
        $scope.documents = _.values(me._doc_map);

        $scope.doc = null;

        if ($attrs.docId && _.size($scope.documents)) {
            $scope.doc = $scope.documents[0];
        }
    };

    me._update_ids = function (ids) {
        // this is called then we get a complete new list of ids
        // we check if we have some overlap (keep the overlapping ids)
        var old_ids = _.keys(me._doc_map);

        var new_ids = _.difference(ids, old_ids);
        var rejected_ids = _.difference(old_ids, ids);

        _.each(new_ids, me._enter_document);
        _.each(rejected_ids, me._exit_document);


        // this refreshes stuff for us
        me._process_event();
        me._update_scope();
    }

    $scope.document_map = me._doc_map;

    if ($attrs.docId) {
        $scope.$watch($attrs.docId, function (v) {
            if (v) {
                me._update_ids([v]);
            } else {
                me._update_ids([]);
            }
        }, true);
    }

    if ($attrs.docIdList) {
        $scope.$watch($attrs.docIdList, function (v) {
            me._update_ids(v || []);
        }, true);
    }

    SyncPool.subscribe_events(me._process_event);

    $scope.$on("$destroy", function () {
        SyncPool.unsubscribe_events(me._process_event);
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

//        me.update_run_ptr();
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
