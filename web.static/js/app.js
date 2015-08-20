var dqmApp = angular.module('dqmApp', ['ngRoute', 'ui.bootstrap', 'dqm.graph', 'dqm.db', 'dqm.ui', 'dqm.utils']);

dqmApp.controller('NavigationCtrl', [
    '$scope', '$window', '$location', '$route', '$http', 'SyncPool', 'SyncDocument', 'LocParams',
    function($scope, $window, $location, $route, $http, SyncPool, SyncDocument, LocParams) {

    var me = this;
    $scope.$route = $route;

    $scope.setPage = function (str) {
        $location.path("/" + str);
    };

    $scope.dqm_number = 2;
    $scope.dqm_debug = true;

    $scope.reverse_log = function (s) {
        return s.split("\n").reverse().join("\n");
    };

    me.hosts_allowed = {};
    me.hosts_shortcuts = {};

    // token goes into title instead of uri, but has to be 1:1 identity
    var token2uri = function (tok) {
        var re = /^(ws|wss|http|https)\:(.+)\:(\d+)$/;
        var tokens = tok.match(re);
        if (! tokens)
            return null;

        if (! me.hosts_allowed[tokens[2]])
            return null;

        var proto = tokens[1];
        var path = "/sync";
        if (proto.slice(0,4) === "http") {
            path = "/sync_proxy"
        }

        return proto + "://" + tokens[2] + ":" + tokens[3] + path;
    };

    var uri2token = function (uri) {
        var re = /^(ws|wss|http|https)\:\/\/(.+)\:(\d+)\/(sync|sync_proxy)$/;
        var tokens = uri.match(re);
        if (! tokens)
            return null;

        return tokens[1] + ":" + tokens[2] + ":" + tokens[3];
    };

    var default_token = "ws:" + $location.host() + ":" + $location.port();
    var get_tokens_from_location = function () {
        var l = LocParams.p.hosts;
        if ((l === null) || (l === undefined))
            l = default_token;

        if ((l === "") || (l === true))
            return [];

        var splits = l.split(",");
        var all = [];
        _.each(splits, function (token) {
            if (me.hosts_shortcuts[token]) {
                _.each(me.hosts_shortcuts[token], function (t) {
                    all.push(t);
                });
            } else {
                all.push(token);
            }
        });
    
        return all;
    };

    var write_location_from_tokens = function (tokens) {
        tokens.sort();
        var loc = _.uniq(tokens).join(",");
        _.each(me.hosts_shortcuts, function (sc_value, sc_key) {
            loc = loc.replace(sc_value, sc_key);
        });

        LocParams.setKey("hosts", loc);
    };

    // fff_cluster uses hostnames
    // these checks if selected hosts are _all_ enabled
    var make_token = function (host) {
        return "ws:" + host + ":" + 9215;
    };

    me.check_hosts = function (hosts) {
        var tokens = get_tokens_from_location();
        var tokens_to_check = _.map(hosts, make_token);

        return _.difference(tokens_to_check, tokens).length === 0;
    };

    me.enable_hosts = function (hosts) {
        var tokens = _.union(get_tokens_from_location(), _.map(hosts, make_token));
        write_location_from_tokens(tokens);
    };

    me.disable_hosts = function (hosts, port) {
        var tokens = _.difference(get_tokens_from_location(), _.map(hosts, make_token));
        write_location_from_tokens(tokens);
    };

    var update_connections = function () {
        var new_uris = [];
        var old_uris = _.keys(SyncPool._conn);

        _.each(get_tokens_from_location(), function (tok) {
            var host = token2uri(tok);
            if (host)
                new_uris.push(host);
        });

        var to_connect = _.difference(new_uris, old_uris);
        var to_disconnect = _.difference(old_uris, new_uris);

        _.each(to_disconnect, function (uri) {
            SyncPool.disconnect(uri);
        });

        _.each(to_connect, function (uri) {
            SyncPool.connect(uri);
        });
    };

    //$scope.$watch(function () { return $http.pendingRequests.length; }, function (v) {
    //    $scope.NavigationCtrl.http_count = v;
    //    $scope.NavigationCtrl.http_state = v?"busy":"ready";
    //});

    var p = $http.get("/info");
    p.then(function (body) {
        me.cluster_info = body.data;

        _.each(me.cluster_info.cluster._all, function (v, k) {
            _.each(v, function (host) {
                me.hosts_allowed[host] = true;
            });
    
            // save the "shortcut"
            var sc_value = _.uniq(_.map(_.values(v), make_token));
            me.hosts_shortcuts[k] = sc_value;
        });
        update_connections();
    });

    $scope.SyncPool = SyncPool;
    $scope.SyncDocument = SyncDocument;
    $scope._ = _;

    $scope.$watch(LocParams.watchFunc('hosts'),  update_connections);
}]);

dqmApp.controller('LumiRunCtrl', ['$scope', '$rootScope', 'SyncPool', 'LocParams', 'SyncRun', function($scope, $rootScope, SyncPool, LocParams, SyncRun) {
    var me = this;

    me.update_run_ptr = function () {
        me.runs = SyncRun.get_runs();

        me.latest_run = me.runs[0];
        if (LocParams.p.trackRun) {
            if ((me.latest_run !== undefined) && (me.run != me.latest_run)) {
                LocParams.p.run = me.latest_run;
                return;
            }
        }

        var fi = function (i) {
            if ((me.runs.length == 0) || (i < 0) || (me.runs.length <= i))
                return null;

            return me.runs[i];
        };

        var ci = _.indexOf(me.runs, me.run);
        me.previous_run = fi(ci + 1);
        me.next_run = fi(ci - 1);

        // run_dct won't change the reference (at least not until this function is called)
        // so we can deep-watch and all
        me.run_dct = SyncRun.get_run_dictionary(me.run) || {};
        me.run0_dct = SyncRun.get_run_dictionary(null) || {};
    };

    // this is necessary not to overflow digests
    me.type_id_cache = {};
    me.get_type_ids = function (type) {
        var new_ids = [];
        _.each(me.run_dct.items || {}, function (item, key) {
            if (item["type"] === type) {
                new_ids.push(item._id);
            }
        });
        new_ids.sort();

        if (! _.isEqual(new_ids, me.type_id_cache[type])) {
            me.type_id_cache[type] = new_ids;
        }

        return me.type_id_cache[type];
    };

    $scope.$watch(LocParams.watchFunc('run'), function (run) {
        if ((run === undefined) && (LocParams.p.trackRun === undefined)) {
            // default value (no run set)
            LocParams.p.trackRun = true;
        }

        me.run = parseInt(run);
        me.run_ = parseInt(run);

        $rootScope.title = ": Run " + me.run;
        me.update_run_ptr();
    });

    $scope.$watch(SyncRun.get_runs, function (run_lst) {
        me.update_run_ptr();
    });

    $scope.$watch(LocParams.watchFunc('trackRun'),  function (run) {
        me.update_run_ptr();
    });

    $scope.LumiRunCtrl = me;
}]);

dqmApp.config(function($routeProvider, $locationProvider) {
  $routeProvider
    .when('/lumi/', { menu: 'lumi', templateUrl: 'templates/lumi.html', reloadOnSearch: false })
    //.when('/stats/', { menu: 'stats', templateUrl: 'templates/stats.html', reloadOnSearch: false })
    //.when('/lumi/:run/', { menu: 'lumi', templateUrl: 'templates/lumi.html', reloadOnSearch: false })
    .otherwise({ redirectTo: '/lumi' });

  // configure html5 to get links working on jsfiddle
  $locationProvider.html5Mode(false);
});
