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

	// token goes into title instead of uri, but has to be 1:1 identity
	var token2uri = function (tok) {
		var re = /^(ws{1,2})\:(.+)\:(\d+)$/;
		var tokens = tok.match(re);
		if (! tokens)
			return null;

		if (! me.hosts_allowed[tokens[2]])
			return null;

		return tokens[1] + "://" + tokens[2] + ":" + tokens[3] + "/sync";
	};

	var uri2token = function (uri) {
		var re = /^(ws{1,2})\:\/\/(.+)\:(\d+)\/sync$/;
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

		return l.split(",");
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
        LocParams.setKey("hosts", _.uniq(tokens).join(","));
    };

    me.disable_hosts = function (hosts, port) {
		var tokens = _.difference(get_tokens_from_location(), _.map(hosts, make_token));
        LocParams.setKey("hosts", _.uniq(tokens).join(","));
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
        });
        update_connections();
    });

    $scope.SyncPool = SyncPool;
    $scope.SyncDocument = SyncDocument;
    $scope._ = _;

    $scope.$watch(LocParams.watchFunc('hosts'),  update_connections);
}]);

dqmApp.controller('LumiRunCtrl', ['$scope', '$rootScope', 'SyncPool', 'LocParams', function($scope, $rootScope, SyncPool, LocParams) {
    var me = this;

    me.runs_dct = {};
    me.runs = [];

    var set_default = function (dct, key, value) {
        if (dct[key] === undefined)
            dct[key] = value;

        return dct[key];
    };

    me.update_run_ptr = function () {
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

        me.run_dct = me.runs_dct[me.run] || {};
        me.run_ids = _.pluck(me.run_dct.items, "_id");

        // template use per-type-filtering
        me.type_dct = _.groupBy(me.run_dct.items || {}, 'type');
        me.type_dct_id = _.mapObject(me.type_dct, function (val) {
            return _.pluck(val, "_id");
        });

        me.run0_dct = me.runs_dct[null] || {};
        me.run0_ids = _.pluck(me.run0_dct.items, "_id");
    };

    me.parse_headers = function (headers, reload) {
        if (reload) {
            me.runs_dct = {};
            me.runs = [];
        };

        _.each(headers, function (head) {
            if (head.run !== null)
                me.runs.push(head.run);

            var rd = set_default(me.runs_dct, head.run, {});
            var items = set_default(rd, 'items', {});
            items[head["_id"]] = head;
        });

        me.runs.sort();
        me.runs = _.uniq(me.runs, true);
        me.runs.reverse();

        me.update_run_ptr();
    };

    SyncPool.subscribe_headers(me.parse_headers);
    $scope.$on("$destroy", function () {
        SyncPool.unsubscribe_headers(me.parse_headers);
    });

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
