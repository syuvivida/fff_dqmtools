var dqmApp = angular.module('dqmApp', ['ngRoute', 'ui.bootstrap', 'dqm.graph', 'dqm.db', 'dqm.ui', 'dqm.utils']);

dqmApp.controller('NavigationCtrl', [
    '$scope', '$window', '$location', '$route', '$http', 'SyncPool', 'SyncDocument',
    function($scope, $window, $location, $route, $http, SyncPool, SyncDocument) {

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

    //$scope.$watch(function () { return $http.pendingRequests.length; }, function (v) {
    //    $scope.NavigationCtrl.http_count = v;
    //    $scope.NavigationCtrl.http_state = v?"busy":"ready";
    //});

    var p = $http.get("/info");
	p.then(function (body) {
		me.cluster_info = body.data;
	});

    $scope.SyncPool = SyncPool;
    $scope.SyncDocument = SyncDocument;
	$scope._ = _;
}]);

dqmApp.controller('LumiRunCtrl', ['$scope', '$rootScope', 'SyncPool', 'LocParams', function($scope, $rootScope, SyncPool, LocParams) {
    var me = this;

    me.runs_dct = {};
    me.runs = [];

    me.run = null;
    me.run_dct = {};

    var set_default = function (dct, key, value) {
        if (dct[key] === undefined)
            dct[key] = value;

        return dct[key];
    };

    me.update_run_ptr = function () {
		me.latest_run = me.runs[0];
		if (LocParams.p.trackRun) {
			if ((me.latest_run !== undefined) && (me.run != me.latest_run)) {
				LocParams.setKey('run', me.latest_run);
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

        // template use per-type-filtering
        me.type_dct = _.groupBy(me.run_dct.items || {}, 'type');
		me.type_dct_id = _.mapObject(me.type_dct, function (val) {
			return _.pluck(val, "_id");
		});

		me.run0_dct = me.runs_dct[null] || {};
		me.run0_ids = _.pluck(me.run0_dct.items, "_id");
    };

    me.parse_headers = function (headers) {
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

    //me.get_sorted_headers = function

    SyncPool.subscribe_headers(me.parse_headers);
    $scope.$on("$destroy", function () {
        SyncPool.unsubscribe_headers(me.parse_headers);
    });

    $scope.$watch(LocParams.watchFunc('run'), function (run) {
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
