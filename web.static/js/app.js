var dqmApp = angular.module('dqmApp', ['ngRoute', 'ui.bootstrap', 'dqmGraphApp', 'dqmDatabaseApp']);

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
    var me = {};

    me.params = {};
    me._params = {};

    me.setKey = function (k, v) {
        $location.search(k, v);
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
                $location.search(k, v).replace();
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
}]);

dqmApp.controller('ClusterCtrl', ['$scope', '$http', 'DynamicQuery', function($scope, $http, DynamicQuery) {
    var ctrl = {};
    $scope.ClusterCtrl = ctrl;

    // hostname
    $http.get("/").then(function (body) {
        var data = body.data;
        var host = data.name;
        ctrl.hostname = host;
    });

    DynamicQuery.repeated_search_dct("info", {
        url: "/info",
        method: "get",
        _es_callback: function (body) {
            ctrl.info_data = body;
            console.log(ctrl, body);
        }
    });

    $scope.$on("$destroy", function handler() {
        DynamicQuery.delete_search("info");
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

dqmApp.controller('StatsController', ['$scope', 'DynamicQuery', function($scope, DynamicQuery) {
    var ctrl = {};

    DynamicQuery.repeated_search("list-stats", "list/stats", null, function (body) {
        $scope.hits = body.hits;
        console.log(body);
        try {
            _.each($scope.hits, function (hit) {
                var pc = parseInt(hit.disk_used) * 100 / hit.disk_total;
                hit.$disk_pc = pc;

                if (hit._type == "dqm-diskspace") {
                    ctrl.lumi = hit.extra.files_seen;
                }
            });
        } catch (e) {
            console.log("Error", e);
        }
    });

    $scope.$on("$destroy", function handler() {
        DynamicQuery.delete_search("list-stats");
    });

    $scope.StatsCtrl = ctrl;
}]);

dqmApp.controller('LumiRunCtrl', ['$scope', '$location', '$routeParams', 'SyncPool', function($scope, $location, $routeParams, SyncPool) {
    var me = this;

    me.runs_dct = {};
    me.runs = [];

    me.run = null;
    me.run_dct = {};

    me.set_run = function (v) {
        $location.path("/lumi/" + v + "/");
    };

    var set_default = function (dct, key, value) {
        if (dct[key] === undefined)
            dct[key] = value;

        return dct[key];
    };

    me.update_run_ptr = function () {
        var fi = function (i) {
            if ((me.runs.length == 0) || (i < 0) || (me.runs.length <= i))
                return null;

            return me.runs[i];
        };

        var ci = _.indexOf(me.runs, me.run);
        me.previous_run = fi(ci + 1);
        me.next_run = fi(ci - 1);

        me.run_dct = me.runs_dct[me.run];

        if (me.run_dct) {
            // template use per-type-filtering
            // filter here for the sake of performance
            me.type_dct = _.groupBy(me.run_dct.items, 'type');
			me.type_dct_id = _.mapObject(me.type_dct, function (val) {
				return _.pluck(val, "_id");
			});
        } else {
			me.type_dct = null;
			me.type_dct_id = null; 
		}
    };

    me.parse_run_header = function (headers) {
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

    SyncPool.subscribe_headers(me.parse_run_header);
    $scope.$on("$destroy", function () {
        SyncPool.unsubscribe_headers(me.parse_run_header);
    });

    $scope.$watch("ParamsCtrl.params.run", function (run) {
        me.run = parseInt(run);
        me.run_ = parseInt(run);
        me.update_run_ptr();
    });
}]);

dqmApp.filter("dqm_exitcode_class", function() {
    return function (input) {
		if (!input)
			return "info";

        // document is a header if it does not have "_header" attribute
        if ((input.exit_code === undefined) && (input._header === undefined))
            return "info";

		var ec = input.exit_code;
		if ((ec === null) || (ec === undefined)) {
    	    return "success";
    	} else if ((ec === 0) || (ec === "0")) {
    	    return "warning";
    	} else {
    	    return "danger";
    	}
    };
});

dqmApp.filter("dqm_megabytes", function() {
    return function(input) {
        var s = input || '';

        if (s.indexOf && s.indexOf(" kB") > -1) {
            s.replace(" kB", "");
            s = parseInt(s) * 1024;
        }

        s = (parseInt(s) / 1024 / 1024).toFixed(0) + ' ';
        s = s + "mb";

        return s;
    };
});

dqmApp.filter("dqm_lumi_seen", function() {
    return function (lumi_seen) {
		if (!lumi_seen)
			return "null";

		var ls = lumi_seen;
    	var skeys = _.keys(ls);
    	skeys.sort();

    	var sorted = _.map(skeys, function (key) {
    	    // key is "lumi00032"
    	    return "[" + key.substring(4) + "]: " + ls[key];
    	});

		return sorted.join("\n");
    };
});

dqmApp.controller('LumiCtrl', ['$scope', '$modal', '$attrs',
        function($scope, $modal, $attrs) {

	var me = this;


    //var lumi = {
    //    run: $routeParams.run
    //};

    //lumi.ec_hide = {};
    //lumi.ec_toggle = function (c) {
    //    lumi.ec_hide[c] = !(lumi.ec_hide[c]);
    //};

    //$scope.$watch("LumiCtrl.run", function (v) {
    //    if (!v)
    //        return;

    //    DynamicQuery.repeated_search("lumi-data", "list/run/" + parseInt(v), null, function (body) {
    //        lumi.hits = body.hits;
    //        lumi.logs = {};

    //        // this block sorts the lumiSeen (originally, it's a dictionary,
    //        // but we want to see it as a list)
    //        try {
    //            _.each(lumi.hits, function (hit) {
    //                var lines = [];
    //                var ls = hit.extra.lumi_seen;
    //                var skeys = _.keys(ls);
    //                skeys.sort();

    //                hit.$sortedLumi = _.map(skeys, function (key) {
    //                    // key is "lumi00032"
    //                    return "[" + key.substring(4) + "]: " + ls[key];
    //                });

    //                if (skeys.length) {
    //                    hit.$lastLumi = parseInt(skeys[skeys.length - 1].substring(4));
    //                }
    //            });
    //        } catch (e) {
    //            console.log("Error", e);
    //        }

    //        // pick out the log entry
    //        // so it does not polute "source" button output
    //        _.each(lumi.hits, function (hit) {
    //            if (hit.extra && hit.extra.stdlog) {
    //                var log = hit.extra.stdlog;
    //                lumi.logs[hit._id] = log;

    //                delete hit.extra.stdlog;
    //            }
    //        });



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
}]);

dqmApp.controller('SimpleDataDialogCtrl', function ($scope, $modalInstance, data) {
    $scope.data = data;
})

dqmApp.factory('DynamicQuery', ['$http', '$window', function ($http, $window) {
    var factory = {
        base: "/",
        _searches: {},
        _ti: null
    };

    factory.repeated_search_dct = function (name, dct) {
        this._searches[name] = dct;
        this.do_the_query(dct);
    };

    factory.repeated_search = function (name, url, query, cb) {
        this.repeated_search_dct(name, {
            url: this.base + url,
            data: query,
            _es_callback: cb,
            method: "post",
        });
    };

    factory.delete_search = function (name) {
        delete this._searches[name];
    };

    factory.do_the_query = function (value) {
        var p = $http(value);
        p.success(value._es_callback);
    };

    factory.try_update = function () {
        if ($http.pendingRequests.length == 0) {
            angular.forEach(this._searches, this.do_the_query, this);
        };
    };

    factory.update_now = factory.try_update;

    factory.start = function (timeout_sec) {
        if (this._ti) {
            $window.clearInterval(this._ti);
            this._ti = null;
        }

        var ts = parseInt(timeout_sec) * 1000;
        if (ts) {
            this._ti = $window.setInterval(function () { factory.try_update(); }, ts);
        }

        console.log("Restarted watcher: " + ts);
    };

    return factory;
}]);



dqmApp.directive('prettifySource', function ($window) {
    return {
        restrict: 'A',
        scope: { 'prettifySource': '=' },
        link: function (scope, elm, attrs) {
            scope.$watch('prettifySource', function (v) {
                var lang = attrs.lang || "javascript";
                var x = hljs.highlight(lang, v || "") .value;
                elm.html(x);
            });
        }
    };
});

dqmApp.directive('dqmTimediffField', function ($window) {
    return {
        restrict: 'E',
        scope: { 'time': '=', 'diff': '=' },
        link: function (scope, elm, attrs) {
            var update = function () {
                if (! scope.time)
                    return;
	
				var ref = scope.diff;
                if (! ref) {
					ref = Math.floor(Date.now() / 1000);
				}

                var diff_s = ref - scope.time;

                scope.diff_s = diff_s;
                if (diff_s < 60) {
                    scope.diff_class = "label-success";
                } else if (diff_s < 60*5) {
                    scope.diff_class = "label-info";
                } else if (diff_s < 60*10) {
                    scope.diff_class = "label-warning";
                } else {
                    scope.diff_class = "label-danger";
                };
            }

            scope.$watch('time', update);
            scope.$watch('diff', update);
            update();
        },
        template: '<span class="label label-success" ng-class="diff_class">{{ diff_s | number:0 }}&nbsp;s.</span>'
    };
});

dqmApp.directive('dqmSortedTable', function () {
    return {
        restrict: 'A',
        scope: { 
            'key': '=',
            'reversed': '='
        },
        controller: function ($scope) {
            var me = this;

            this.sort_key = "delay";
            this.sort_reversed = false;

            this.toggleKey = function (key) {
                if (me.sort_key != key) {
                    $scope.key = key;
                } else {
                    $scope.reversed = !$scope.reversed;
                }
            };

            $scope.$watch("key", function (v) {
                me.sort_key = v;
            });

            $scope.$watch("reversed", function (v) {
                me.sort_reversed = v;
            });
        }
    };
});

dqmApp.directive('dqmSortHeader', function () {
    return {
        require: '^dqmSortedTable',
        restrict: 'A',
        replace: true,
        transclude: true,
        scope: { 'key': "@dqmSortHeader" },
        link: function(scope, element, attrs, ctrl) {
            scope.ctrl = ctrl;
        },
        template: ""
            + "<th class='sort-header' ng-class='{ \"sort-key\": ctrl.sort_key == key, \"sort-reversed\": ctrl.sort_reversed }' ng-click='ctrl.toggleKey(key)'>"
            + "<span ng-transclude />"
            + "<span ng-show='(ctrl.sort_key == key) && ctrl.sort_reversed' class='sort-carret glyphicon glyphicon-chevron-up'></span>"
            + "<span ng-show='(ctrl.sort_key == key) && (!ctrl.sort_reversed)' class='sort-carret glyphicon glyphicon-chevron-down'></span>"
            + "</th>",
    };
});

dqmApp.config(function($routeProvider, $locationProvider) {
  $routeProvider
    .when('/lumi/', { menu: 'lumi', templateUrl: 'templates/lumi.html', reloadOnSearch: false })
    .when('/stats/', { menu: 'stats', templateUrl: 'templates/stats.html', reloadOnSearch: false })
    .when('/lumi/:run/', { menu: 'lumi', templateUrl: 'templates/lumi.html', reloadOnSearch: false })
    .otherwise({ redirectTo: '/lumi' });

  // configure html5 to get links working on jsfiddle
  $locationProvider.html5Mode(false);
});
