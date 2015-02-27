var dqmApp = angular.module('dqmApp', ['ngRoute', 'ui.bootstrap']);

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

    this.params = {};
    this._params = {};

    this.setKey = function (k, v) {
        $location.search(k, v);
    };

    this.update = function () {
        var s = $location.search();

        this.params = {};
        this._params = {};

        _.each(s, function (v, k) {
            //console.log("New param:", k, v);
            this.params[k] = v;
            this._params[k] = v;
        }, this);
    };

    this.navigate = function () {
        _.each(this.params, function (v, k) {
            var old = this._params[k];
            if (old !== v) {
                //console.log("setting", k, v);
                this.setKey(k, v);
            };
        }, this);
    };

    this.update();

    $scope.$on("$locationChangeSuccess", function (event) {
        me.update();
    });
}]);

dqmApp.controller('NavigationCtrl', [
    '$scope', '$window', '$location', '$route', '$http', 'DynamicQuery',
    function($scope, $window, $location, $route, $http, DynamicQuery) {

    $scope.$route = $route;

    $scope.setPage = function (str) {
        $location.path("/" + str);
    };

    $scope.NavigationCtrl = {};
    $scope.DynamicQuery = DynamicQuery;
    $scope.dqm_number = 2;

    $scope.toJson = function (v) {
        return angular.toJson(v, true);
    };

    $scope.reverse_log = function (s) {
        return s.split("\n").reverse().join("\n");
    };

    $scope.debug_object = function (c) {
        console.log("Debug object: ", c);
        return c;
    };

    $scope.$watch(function () { return $http.pendingRequests.length; }, function (v) {
        $scope.NavigationCtrl.http_count = v;
        $scope.NavigationCtrl.http_state = v?"busy":"ready";
    });

    $scope.$watch('ParamsCtrl.params.search_interval', function (x) {
        DynamicQuery.start(x);
    });
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

    var query = {
        "query": {
            "match_all": {}
        },
        "fields" : ["_timestamp"],
        "size": 1024,
        "sort": [
            { "type": { "order": "asc" }},
            { "_timestamp": { "order": "desc" }}
        ]
    };

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

dqmApp.controller('LumiRunCtrl', ['$scope', '$location', '$routeParams', 'DynamicQuery', function($scope, $location, $routeParams, DynamicQuery) {
	var me = this;

	me.current_run = parseInt($routeParams.run);
	me.current_run_ = parseInt($routeParams.run);

    me.set_run = function (v) {
        $location.path("/lumi/" + v + "/");
    };

    DynamicQuery.repeated_search("list-runs", "list/runs", null, function (body) {
        var runs = _.uniq(body.runs);
        runs.sort();
        runs.reverse();

        me.runs = runs;
        me.runs_loaded = true;

		// calculate previous and next runs
		var fi = function (i) {
			if (me.runs.length == 0)
				return null;

			if (i < 0)
				return null;

			if (me.runs.length <= i)
				return null;

			return me.runs[i];
		}

		var ci = _.indexOf(me.runs, me.current_run);
		me.previous_run = fi(ci + 1);
		me.next_run = fi(ci - 1);
    });

    $scope.$on("$destroy", function () {
        DynamicQuery.delete_search("list-runs");
    });
}]);

dqmApp.controller('LumiCtrl', ['$scope', '$http', 'DynamicQuery', '$routeParams', '$modal',
        function($scope, $http, DynamicQuery, $routeParams, $modal) {

    var lumi = {
        run: $routeParams.run
    };

    lumi.ec_hide = {};

    lumi.ec_toggle = function (c) {
        lumi.ec_hide[c] = !(lumi.ec_hide[c]);
    };

    $scope.$watch("LumiCtrl.run", function (v) {
        if (!v)
            return;

        DynamicQuery.repeated_search("lumi-data", "list/run/" + parseInt(v), null, function (body) {
            lumi.hits = body.hits;
            lumi.logs = {};

            // this block sorts the lumiSeen (originally, it's a dictionary,
            // but we want to see it as a list)
            try {
                _.each(lumi.hits, function (hit) {
                    var lines = [];
                    var ls = hit.extra.lumi_seen;
                    var skeys = _.keys(ls);
                    skeys.sort();

                    hit.$sortedLumi = _.map(skeys, function (key) {
                        // key is "lumi00032"
                        return "[" + key.substring(4) + "]: " + ls[key];
                    });

                    if (skeys.length) {
                        hit.$lastLumi = parseInt(skeys[skeys.length - 1].substring(4));
                    }
                });
            } catch (e) {
                console.log("Error", e);
            }

            // pick out the log entry
            // so it does not polute "source" button output
            _.each(lumi.hits, function (hit) {
                if (hit.extra && hit.extra.stdlog) {
                    var log = hit.extra.stdlog;
                    lumi.logs[hit._id] = log;

                    delete hit.extra.stdlog;
                }
            });

            // parse exit code
            try {
                var timestamps = [];
                _.each(lumi.hits, function (hit) {
                    // filter by exit code
                    var ec = hit.exit_code;

                    if ((ec === null) || (ec === undefined)) {
                        hit.$ec_class = "success";
                    } else if ((ec === 0) || (ec === "0")) {
                        hit.$ec_class = "warning";
                    } else {
                        hit.$ec_class = "danger";
                    }
                });
            } catch (e) {
                console.log("Error", e);
            }

            // remove aux/not cmsRun entries
            var groups = _.groupBy(lumi.hits, "type");
            lumi.hits = groups["dqm-source-state"] || [];

            if (groups["dqm-timestamps"]) {
                lumi.timestamps_doc = groups["dqm-timestamps"][0];
            }
        });

    });

    $scope.$on("$destroy", function () {
        DynamicQuery.delete_search("lumi-data");
    });

    lumi.openKillDialog = function (hit) {
        var instance = $modal.open({
            templateUrl: 'templates/modalKillLumi.html',
            controller: 'SimpleDataDialogCtrl',
            scope: $scope,
            resolve: {
                data: function () {
                    return { hit: hit };
                }
            }
        });

        instance.result.then(function (ret) {
            var body = { pid: hit.pid, signal: ret.signal };
            var p = $http.post("/utils/kill_proc/" + hit._id, body);
            
            p.then(function (resp) {
                $scope.AlertCtrl.addAlert({ type: 'success', strong: "Success!", msg: resp.data });
            }, function (resp) {
                $scope.AlertCtrl.addAlert({ type: 'danger', strong: "Failure!", msg: resp.data });
            });
        }, function () {
            // aborted, do nothing
        });
    };

    $scope.LumiCtrl = lumi;
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
        scope: { 'prettifySource': '@' },
        link: function (scope, elm, attrs) {
            scope.$watch('prettifySource', function (v) {
                var lang = attrs.lang || "javascript";
                var x = hljs.highlight(lang, v || "") .value;
                elm.html(x);
            });
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

dqmApp.directive('dqmTimediffField', function ($window) {
    return {
        restrict: 'E',
        scope: { 'time': '=', 'diff': '=' },
        link: function (scope, elm, attrs) {
            var update = function () {
                if (! scope.time)
                    return;

                if (! scope.diff)
                    return;

                var diff_s = scope.diff - scope.time;

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

dqmApp.directive('dqmMemoryGraph', function ($window) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@' },
        link: function (scope, elm, attrs) {
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);

            var div = d3.select(elm[0]).append("div");
            div.attr("style", "position: relative");

            var svg = div.append("svg");
            svg.attr("width", width).attr("height", height);

            var chart = nv.models.lineChart()
                .margin({left: 100})
                .useInteractiveGuideline(false)
                .showLegend(true)
                .transitionDuration(350)
                .showYAxis(true)
                .showXAxis(true)
                .xScale(d3.time.scale());
            ;

            chart.interactiveLayer.tooltip.enabled(false);
            chart.interactiveLayer.tooltip.position({"left": 0, "top": 0});

            chart.xAxis
                .axisLabel('Time')
                .tickFormat(d3.time.format('%X'));

            chart.yAxis
                .axisLabel('Mem (mb)')
                .tickFormat(d3.format('.02f'));

            scope.$watch("data", function (data) {
                if (!data)
                    return;

                // unpack the data
                // we get "timestamp" -> "statm"
                var keys = _.keys(data);
                keys.sort()

                // this is the content labels
                // from /proc/<pid>/statm
                // it's in pages, so we convert to megabytes
                var labels = ["size", "resident", "share", "text", "lib", "data", "dt"];
                var displayed = {"size": 1, "resident": 1 };
                var streams = {};

                _.each(labels, function (l) {
                    var d = (displayed[l] === undefined);
                    streams[l] = { key: l, values: [], disabled: d };
                });

                _.each(keys, function (key) {
                    var time = new Date(parseInt(key)*1000);

                    var unpacked = data[key].split(" ");
                    _.each(unpacked, function (v, index) {
                        var l = labels[index];
                        streams[l].values.push({
                            'y': parseFloat(v) * 4 / 1024,
                            'x': time,
                        });
                    });
                });

                var display = _.values(streams);
                svg
                    .datum(display)
                    .transition().duration(500)
                    .call(chart)
                ;
            });
        }
    };
});

dqmApp.directive('graphDqmTimestampsLumi', function ($window) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;
    var LUMI = 23.310893056;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@' },
        link: function (scope, elm, attrs) {
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);

            var div = d3.select(elm[0]).append("div");
            var svg = div.append("svg");
            svg.attr("width", width).attr("height", height);

            var chart = nv.models.scatterChart()
                .showDistX(true)
                .showDistY(true)
                .x(function (x) { return x['lumi']; })
                .y(function (x) { return x['delay']; })
                .forceY([0, 60])
                .forceX([0, 1])
                .useVoronoi(false)
                .transitionDuration(350)
                .color(d3.scale.category10().range());

            var tformat = function (timestamp) {
                var d = new Date(parseFloat(timestamp)*1000);
                return d.toLocaleString();
            };

            chart.tooltipContent(function(k, _v1, _v2, o) {
                var p = o.point;
                var gs = o.series.global_start;

                return ""
                    + "<h3>" + k + "</h3>"
                    + "<span>"
                    + "Lumi number: <strong>" + p.lumi + "</strong><br />"
                    + "File m-time: <strong>" + tformat(p.mtime) + "</strong><br />"
                    + "Time offset from the expected first delivery [delivery_start_offset]: <strong>" + p.start_offset  + " (seconds)</strong><br />"
                    + "Delay [delivery_start_offset - (lumi_number - 1)*23.3]: <strong>" + p.delay + " (seconds)</strong><br />"
                    + "-<br />"
                    + "Run start (m-time on .runXXXX.global): <strong>" + tformat(gs) + "</strong><br />"
                    + "Delivery start (run_start + 23.3): <strong>" + tformat(gs + LUMI) + "</strong><br />"
                    + "</span>";
            });

            chart.scatter.onlyCircles(false);

            // Axis settings
            chart.xAxis
                .axisLabel("Lumisection")
                .tickFormat(d3.format('.00f'));

            chart.yAxis
                .axisLabel("Delay (s)")
                .tickFormat(d3.format('.01f'));

            scope.$watch("data", function (data) {
                if ((!data) || (!data.extra) || (!data.extra.streams))
                    return;

                var date_start = data.extra.global_start;
                var streams = data.extra.streams;

                // from key:value, make a [value], and key being an entry in value

                var rest = _.map(_.keys(streams), function (k) {
                    var lumis = streams[k].lumis;
                    var mtimes = streams[k].mtimes;

                    var e = {};
                    e["key"] = k;
                    e["values"] = _.map(lumis, function (_lumi, index) {
                        var lumi = lumis[index];
                        var mtime = mtimes[index];

                        // timeout from the begging of the run
                        var start_offset = mtime - date_start - LUMI;
                        var lumi_offset = (lumi - 1) * LUMI;

                        // timeout from the time we think this lumi happenned
                        var delay = start_offset - lumi_offset;

                        return {
                            // 'x': lumi,
                            // 'y': delay,

                            'lumi': lumi,
                            'mtime': mtime,

                            'start_offset': start_offset,
                            'delay': delay,
                            'size': 100,
                            'shape': "circle",
                        }
                    });
                    e["global_start"] = date_start;

                    return e;
                });

                scope.graph_data = rest;
            });

            scope.$watch("graph_data", function (data) {
                if (!data)
                    return;

                svg.datum(data).call(chart);
                chart.update();
            });
        }
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
