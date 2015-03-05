var mod = angular.module('dqmDatabaseApp', []);

mod.factory('SyncPool', ['$http', '$window', '$rootScope', function ($http, $window, $rootScope) {
    var factory = {};

    factory._sync_conn = {};

    factory._sync_header_handlers = [];
    factory._sync_event_handlers = [];

    factory._sync_headers = {};

    var Connection = function (ws_uri) {
        var me = this;

        this.ws_uri = ws_uri;
        this.last_ref = null;
        this.retry_timeout = 5;
        this.retry_count = 0;

        // this is only to track progreess
        // ideally, it should be outside this service
        this.make_state = function (st) {
            me.state_string = st;
            var slc = st.slice(0, 4);
            if (slc == "clos") {
                me.state_priority = 10;
                me.state_class = "danger";
            } else if (slc == "open") {
                me.state_priority = 6;
                me.state_class = "warning";
            } else if (slc == "insy") {
                me.state_priority = 5;
                me.state_class = "warning";
            } else if (slc == "sync") {
                me.state_priority = 1;
                me.state_class = "success";
            } else {
                throw "Unknown state (" + slc + ")";
            }

            var c = _.values(factory._sync_conn);
            c = _.sortBy(c, "state_priority");

            factory.lowest_conn = c[0];
        }

        this.handle_event = function (evt) {
            _.each(factory._sync_event_handlers, function (handler) {
                handler(evt, me);
            });
        };

        this.reopen = function () {
            console.log("Created connection: ", me.ws_uri, me);
            me.make_state("open / waiting");

            me.retry_timeout = 1;
            me.retry_count = me.retry_count + 1;

            var ws = new WebSocket(me.ws_uri);
            me.ws = ws;

            ws.onmessage = function (evt) {
                var msg = angular.fromJson(evt.data);

                if (msg["event"] == "update_headers") {
                    me.last_ref = msg["rev"][1];

                    var headers = _.map(msg["headers"], function (head) {
                        head["_source"] = me.ws_uri;
                        factory._sync_headers[head["_id"]] = head;

                        return head;
                    });

                    _.each(factory._sync_header_handlers, function (handler) {
                        handler(headers);
                    });

                    // check progress
                    if (msg["sync_to_rev"] !== me.last_ref) {
                        me.make_state("insync: (" + me.last_ref + "/" + msg["sync_to_rev"] + ")");
                    } else {
                        me.make_state("synchronized");
                    }
                }

                me.handle_event(evt);
                $rootScope.$apply();
            };

            ws.onopen = function (evt) {
                me.make_state("open / insync");

                // request the list of sync objects and subscribe
                ws.send(angular.toJson({
                    'event': 'sync_request',
                    'known_rev': me.last_ref,
                }));

                me.handle_event(evt);
            };

            ws.onclose = function (evt) {
                console.log("WebSocket died: ", evt, me);
                me.ws = null;

                me.make_state("closed");
                me.handle_event(evt);
            };

            ws.onerrror = function (evt, reason) {
                console.log("WebSocket error: ", evt, reason, arguments);
            };
        };

        this.tick = function () {
            if (me.ws) {
                return;
            }

            if (me.retry_timeout > 0) {
                me.retry_timeout = me.retry_timeout - 1;
                return
            }

            me.reopen();
        };
    };

    // public api start
    factory.make_sync_uri = function (host, port) {
        var l = window.location;
        var proto = "ws:";

        if (l.protocol === "https:") proto = "wss:";
        if (host === undefined) host = l.hostname;
        if (port === undefined) port = l.port;

        return proto + "//" + host + ":" + port + "/sync";
    };


    factory.connect = function (ws_uri) {
        if (! factory._sync_conn[ws_uri]) {
            factory._sync_conn[ws_uri] = new Connection(ws_uri);
            factory._sync_conn[ws_uri].reopen();
        }

        return factory._sync_conn[ws_uri];
    };

    factory.send_message = function (ws_uri, msg) {
        var c = factory._sync_conn[ws_uri];
        if (!c.ws) {
            throw "WebSocket not connected.";
        }

        c.ws.send(angular.toJson(msg));
    };

    factory.subscribe_headers = function (callback) {
        factory._sync_header_handlers.push(callback);

        // we have to rotate the current buffer to it
        var headers = _.values(factory._sync_headers);
        callback(headers);
    };

    factory.unsubscribe_headers = function (callback) {
        factory._sync_header_handlers =
            _.filter(factory._sync_header_handlers, function (x) { return x !== callback });
    };

    // these are the document event handlers used by SyncDocument
    factory.subscribe_events = function (callback) {
        factory._sync_event_handlers.push(callback);
    };

    factory.unsubscribe_events = function (callback) {
        factory._sync_event_handlers =
            _.filter(factory._sync_event_handlers, function (x) { return x !== callback });
    };

    // timer for various things
    factory._ti = $window.setInterval(function () {
        _.each(factory._sync_conn, function (e) {
            e.tick();
        });
    }, 1*1000);


    var base_uri = factory.make_sync_uri();
    factory.connect(base_uri);

    return factory;
}]);

mod.factory('SyncDocument', ['SyncPool', '$window', '$rootScope', '$q', function (SyncPool, $window, $rootScope, $q) {
    var factory = {};

    // then the request is created it goes to _defer_requests
    // the timeout is activated (_.defer)
    // after the timeout passes, we take _defer_requests and make the request
    // this speeds up things, if we request 100 documents - they will be grouped into a single message
    // after the request is done, requests appear in factory._requests
    factory._requests = [];
    factory._defer_requests = [];

    factory._process_requests = function () {
        var request_list = factory._defer_requests;
        factory._defer_requests = [];

        _.each(request_list, function (r) {
            factory._requests.push(r);
        });

        var by_source = _.groupBy(request_list, 'source');
        _.each(by_source, function (requests, source) {
            var ids = _.pluck(requests, "id");

            var msg = {
                'event': "request_documents",
                'ids': ids
            };

            try {
                SyncPool.send_message(source, msg);
            } catch (e) {
                //console.error("Failed to send request.", e);
                _.each(requests, function (req) {
                    req.defer.reject("Failed to send request.");
                });
            };
        });
    };

    factory._process_event = function (evt, conn) {
        if (evt["type"] == "message") {
            var msg = angular.fromJson(evt.data);
            if (msg["event"] !=  "update_documents")
                return;

            var docs = msg.documents;
            var id_map = _.indexBy(docs, "_id")

            factory._requests = _.filter(factory._requests, function (request) {
                var d = id_map[request["id"]];

                if (d === undefined) {
                    return true; // leave this request
                }

                request.defer.resolve(d);
                return false;
            });
        } else if (evt["type"] == "close") {
            var source = conn.ws_uri;

            factory._requests = _.filter(factory._requests, function (request) {
                if (request["source"] === source) {
                    request.defer.reject("WebSocket closed before replying.");
                    return false;
                }

                return true;
            });
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

        factory._defer_requests.push(req);

        if (factory._defer_requests.length == 1) {
             $window.setTimeout(factory._process_requests, 100);
        }

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
        factory._defer_requests = _.without(factory._defer_requests, request);
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

    me._docs = {};

    me._enter_document = function (id) {
        // needs to be a clone
        me._docs[id] = _.clone(SyncPool._sync_headers[id]);
    };

    me._refresh_document = function (id) {
        // check if document needs updating
        var doc = me._docs[id];

        // don't update if the update is in progress
        if (doc["$cd_request"] !== undefined)
            return;

        if (doc["$cd_full"]) {
            var header = SyncPool._sync_headers[id];
            var doc_rev = doc._header._rev;
            var header_rev = header._rev;

            if (doc_rev === header_rev) {
                // no update is needed
                return;
            }
        }

        // at this step either doc is not existing or too old
        // create a promise for it
        var p = SyncDocument.fetch(id);
        doc["$cd_request"] = p;

        // some promises are cancelled, but they still return
        p.then(function (new_doc) {
            if ((me._docs[id] === undefined) || (me._docs[id] !== doc)) {
                console.log("Received response for a dead request.", me._docs[id], doc);
                throw "Received response for a dead request.";
            }

            new_doc["$cd_full"] = true;
            me._docs[id] = new_doc;
            me._update_scope();
        }, function (reason) {
            // error callback
            if (reason !== "Fetch cancelled.") {
              console.log("Fetch error", doc, reason);
            }

            // delete the marker, so it can be re-requested
            // (if not removed)
            delete doc["$cd_request"];
        });
    };

    me._exit_document = function (id) {
        var doc = me._docs[id];
        if (doc["$cd_request"]) {
            SyncDocument.cancel(doc["$cd_request"]);
        }

        delete me._docs[id];
    };

    me._process_event = function () {
        // at the time this is called, id list is table
        // and can't/won't change

        // we don't care about actual events
        // but we get called after header update happens
        // a -perfect- time to check for updates
        _.each(me._docs, function (v, k) { me._refresh_document(k); });
    };

    me._update_scope = function () {
        $scope.doc = null;

        if ($attrs.docId && _.size(me._docs)) {
            $scope.doc = _.values(me._docs)[0];
        }
    };

    me._update_ids = function (ids) {
        // this is called then we get a complete new list of ids
        // we check if we have some overlap (keep the overlapping ids)
        var old_ids = _.keys(me._docs);

        var new_ids = _.difference(ids, old_ids);
        var rejected_ids = _.difference(old_ids, ids);

        _.each(new_ids, me._enter_document);
        _.each(rejected_ids, me._exit_document);


        // this refreshes stuff for us
        me._process_event();
        me._update_scope();
    }

    $scope.documents = me._docs;

    if ($attrs.docId) {
        $scope.$watch($attrs.docId, function (v) {
            if (v) {
                me._update_ids([v]);
            } else {
                me._update_ids([]);
            }
        });
    }

    if ($attrs.docIdList) {
        $scope.$watch($attrs.docIdList, function (v) {
            me._update_ids(v || []);
        });
    }

    SyncPool.subscribe_events(me._process_event);

    $scope.$on("$destroy", function () {
        SyncPool.unsubscribe_events(me._process_event);
    });
}]);
