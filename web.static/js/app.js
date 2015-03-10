var dqmApp = angular.module('dqmApp', ['ngRoute', 'ui.bootstrap', 'dqm.graph', 'dqm.db', 'dqm.ui']);

dqmApp.controller('AlertCtrl', ['$scope', '$window', function ($scope, $window) {
    this.alerts = [];

    this.addAlert = function(body) {
        this.alerts.push(body);
        $window.scrollTo(0, 0);
    };

    this.closeAlert = function(index) {
        this.alerts.splice(index, 1);
    };
}]);

dqmApp.controller('ParamsCtrl', ["$scope", "$window", "$location", function ($scope, $window, $location) {
    var me = this;

    me.params = {};
    me._params = {};

	var value = function (v) {
		if (v === undefined) {
			return null;
		} else if (v === false) {
			return null;
		} else if (v === true) {
			return true;
		} else {
			return v;
		}
	};

    me.setKey = function (k, v) {
        $location.search(k, value(v));
    };

    me.update = function () {
        var s = $location.search();

        me.params = {};
        me._params = {};

        _.each(s, function (v, k) {
            //console.log("New param:", k, v);
            me.params[k] = v;
            me._params[k] = v;
        });
    };

    me.update();

    $scope.$on("$locationChangeSuccess", function (event) {
        me.update();
    });

    $scope.$watch("ParamsCtrl.params", function () {
        _.each(me.params, function (v, k) {
            var old = me._params[k];
            if (old !== v) {
                $location.search(k, value(v)).replace();
            };
        });
    }, true);

    $scope.ParamsCtrl = me;
}]);

dqmApp.controller('NavigationCtrl', [
    '$scope', '$window', '$location', '$route', '$http', 'SyncPool', 'SyncDocument',
    function($scope, $window, $location, $route, $http, SyncPool, SyncDocument) {

    $scope.$route = $route;

    $scope.setPage = function (str) {
        $location.path("/" + str);
    };

    $scope.dqm_number = 2;

    $scope.reverse_log = function (s) {
        return s.split("\n").reverse().join("\n");
    };

    //$scope.$watch(function () { return $http.pendingRequests.length; }, function (v) {
    //    $scope.NavigationCtrl.http_count = v;
    //    $scope.NavigationCtrl.http_state = v?"busy":"ready";
    //});

    $scope.SyncPool = SyncPool;
    $scope.SyncDocument = SyncDocument;
	$scope._ = _;
	$scope.parseInt = parseInt;
}]);

dqmApp.controller('ClusterCtrl', ['$scope', '$http', function($scope, $http) {
    var ctrl = {};
    $scope.ClusterCtrl = ctrl;

    var p = $http.get("/info");
	p.then(function (body) {
		ctrl.info_data = body.data;
	});
}]);

dqmApp.controller('DeleteCtrl', ['$scope', '$http', '$modal', '$window', function ($scope, $http, $modal, $window) {
    var ctrl = {};

    var dropRun = function (run) {
        var body = { run: run };
        var rs = "[" + run + "] ";
        var p = $http.post("/utils/drop_run", body);

        p.then(function (resp) {
            $scope.AlertCtrl.addAlert({ type: 'success', strong: rs + "Success!", msg: resp.data });
        }, function (resp) {
            $scope.AlertCtrl.addAlert({ type: 'danger', strong: rs + "Failure!", msg: resp.data });
        });
    };

    ctrl.openDeleteDialog = function () {
        var instance = $modal.open({
            templateUrl: 'templates/modalDropDialog.html',
            controller: 'SimpleDataDialogCtrl',
            scope: $scope,
            resolve: {
                data: function () { return {}; }
            }
        });

        instance.result.then(function (ret) {
            var runs = _.map(ret.runs.split(" "), function(x) {
                return parseInt($window.S(x).s);
            });

            var runs = _.filter(runs, function (x) { return !isNaN(x); });

            _.each(runs, dropRun);
        }, function () {
            // aborted, do nothing
        });
    };

    $scope.DeleteCtrl = ctrl;
}]);

dqmApp.controller('LumiRunCtrl', ['$scope', '$rootScope', 'SyncPool', function($scope, $rootScope, SyncPool) {
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
		if ($scope.ParamsCtrl.params.trackRun) {
			if ((me.latest_run !== undefined) && (me.run != me.latest_run)) {
				$scope.ParamsCtrl.setKey('run', me.latest_run);
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

    $scope.$watch("ParamsCtrl.params.run", function (run) {
        me.run = parseInt(run);
        me.run_ = parseInt(run);

		$rootScope.title = ": Run " + me.run;
        me.update_run_ptr();
    });

    $scope.$watch("ParamsCtrl.params.trackRun", function (run) {
        me.update_run_ptr();
    });

	$scope.LumiRunCtrl = me;
}]);

    //lumi.openKillDialog = function (hit) {
    //    var instance = $modal.open({
    //        templateUrl: 'templates/modalKillLumi.html',
    //        controller: 'SimpleDataDialogCtrl',
    //        scope: $scope,
    //        resolve: {
    //            data: function () {
    //                return { hit: hit };
    //            }
    //        }
    //    });

    //    instance.result.then(function (ret) {
    //        var body = { pid: hit.pid, signal: ret.signal };
    //        var p = $http.post("/utils/kill_proc/" + hit._id, body);

    //        p.then(function (resp) {
    //            $scope.AlertCtrl.addAlert({ type: 'success', strong: "Success!", msg: resp.data });
    //        }, function (resp) {
    //            $scope.AlertCtrl.addAlert({ type: 'danger', strong: "Failure!", msg: resp.data });
    //        });
    //    }, function () {
    //        // aborted, do nothing
    //    });
    //};

dqmApp.controller('SimpleDataDialogCtrl', function ($scope, $modalInstance, data) {
    $scope.data = data;
})

dqmApp.config(function($routeProvider, $locationProvider) {
  $routeProvider
    .when('/lumi/', { menu: 'lumi', templateUrl: 'templates/lumi.html', reloadOnSearch: false })
    //.when('/stats/', { menu: 'stats', templateUrl: 'templates/stats.html', reloadOnSearch: false })
    //.when('/lumi/:run/', { menu: 'lumi', templateUrl: 'templates/lumi.html', reloadOnSearch: false })
    .otherwise({ redirectTo: '/lumi' });

  // configure html5 to get links working on jsfiddle
  $locationProvider.html5Mode(false);
});
