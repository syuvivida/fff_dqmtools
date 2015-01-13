var dqmApp = angular.module('dqmApp', ['ngRoute']);

dqmApp.controller('NavigationController', [
    '$scope', '$location', '$route', '$http', 'ElasticQuery',
    function($scope, $location, $route, $http, ElasticQuery) {

    $scope.$route = $route;

    $scope.setPage = function (str) {
        $location.path("/" + str);
    };

    $scope.NavigationController = {};
    $scope.ElasticQuery = ElasticQuery;
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
        $scope.NavigationController.http_count = v;
        $scope.NavigationController.http_state = v?"busy":"ready";
    });

    $scope.$watch('NavigationController.search_interval', function (x) {
        ElasticQuery.start(x);
    });
}]);

dqmApp.controller('ClusterCtrl', ['$scope', '$http', 'ElasticQuery', function($scope, $http, ElasticQuery) {
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

    ElasticQuery.repeated_search_dct("cluster-state", {
        url: "/_cluster/stats/",
        method: "get",
        _es_callback: function (body) {
            ctrl.cluster_name = body.cluster_name;
            ctrl.cluster_timestamp = body.timestamp;
            ctrl.cluster_data = body;

      console.log(ctrl);
        }
    });

    ElasticQuery.repeated_search("dqm-stats", "dqm-stats/_search/", query, function (body) {
        ctrl.hits = body.hits.hits;
    });

    $scope.$on("$destroy", function handler() {
        ElasticQuery.delete_search("dqm-stats");
    });
}]);

dqmApp.controller('StatsController', ['$scope', 'ElasticQuery', function($scope, ElasticQuery) {
    var ctrl = {};
    var query = {
        "query": {
            "match_all": {}
        },
        "fields" : ["_source", "_timestamp"],
        "size": 1024,
        "sort": [
            { "type": { "order": "asc" }},
            { "_timestamp": { "order": "desc" }}
        ]
    };

    ElasticQuery.repeated_search("stats", "dqm-diskspace,dqm-stats/_search/", query, function (body) {
        $scope.hits = body.hits.hits;
        try {
            _.each($scope.hits, function (hit) {
                var pc = parseInt(hit._source.disk_used) * 100 / hit._source.disk_total;
                hit._source._disk_pc = pc;

                if (hit._type == "dqm-diskspace") {
                    ctrl.lumi = hit._source.extra.files_seen;
                }
            });
        } catch (e) {
            console.log("Error", e);
        }
    });

    $scope.$on("$destroy", function handler() {
        ElasticQuery.delete_search("stats");
    });

    $scope.StatsCtrl = ctrl;
}]);

dqmApp.controller('LumiRunCtrl', ['$scope', 'ElasticQuery', function($scope, ElasticQuery) {
    var run_query = {
        "query": {
            "match_all": {}
        },
        "aggregations" : {
            "runs" : { "terms" : { "field" : "run", "size": 0 } }
        },
    };

    ElasticQuery.repeated_search("lumi-run", "dqm-source-state/_search/", run_query, function (body) {
        var runs = _.map(body.aggregations.runs.buckets, function (v) {
            return parseInt(v.key);
        });

        runs = _.uniq(runs);
        runs.sort();
        runs.reverse();

        $scope.runs = runs;
        $scope.runs_loaded = true;
    });

    $scope.$on("$destroy", function () {
        ElasticQuery.delete_search("lumi-run");
    });
}]);

dqmApp.controller('LumiCtrl', ['$scope', 'ElasticQuery', '$location', '$routeParams', function($scope, ElasticQuery, $location, $routeParams) {
    var lumi = {
        run: $routeParams.run
    };

    lumi.set_run = function (v) {
        $location.path("/lumi/" + v + "/");
    };

    lumi.sortLumiSeen = function (v) {
        return _.sortBy(v, function(val, key, object) {
            return key;
        });
    };

    $scope.$watch("LumiCtrl.run", function (v) {
        if (!v)
            return;

        console.log("Monitoring run", v);
        var lumi_query = {
            "query": {
                "match": {
                    "run": v
                }
            },
            "fields" : ["_source", "_timestamp", "_uid"],
            "size": 1024,
            "sort": [
                { "tag": "asc" }
                //{ "_timestamp": { "order": "desc" }}
            ]
        };

        ElasticQuery.repeated_search("lumi-data", "dqm-source-state/_search/", lumi_query, function (body) {
            lumi.hits = body.hits.hits;
            lumi.logs = {};

            try {
                _.each(lumi.hits, function (hit) {
                    hit.$sortedLumi = lumi.sortLumiSeen(hit._source.extra.lumi_seen);
                    if (hit.$sortedLumi.length)
                        hit.$lastLumi = hit.$sortedLumi[hit.$sortedLumi.length - 1].file_ls;
                });
            } catch (e) {
                console.log("Error", e);
            }

            _.each(lumi.hits, function (hit) {
                if (hit._source && hit._source.extra && hit._source.extra.stdlog) {
                    var log = hit._source.extra.stdlog;
                    lumi.logs[hit._id] = log;

                    delete hit._source.extra.stdlog;
                }
            });

            // parse exit code
            try {
                var timestamps = [];
                _.each(lumi.hits, function (hit) {
                    // filter by exit code
                    var ec = hit._source.exit_code;
                    if ((ec === null) || (ec === undefined)) {
                        hit.$ec_class = "";
                    } else if ((ec === 0) || (ec === "0")) {
                        hit.$ec_class = "warning";
                    } else {
                        hit.$ec_class = "danger";
                    }
                });
            } catch (e) {
                console.log("Error", e);
            }

            $scope.$broadcast("LumiUpdated");
        });

    });

    $scope.$on("$destroy", function () {
        ElasticQuery.delete_search("lumi-data");
    });

    $scope.LumiCtrl = lumi;
}]);


dqmApp.factory('ElasticQuery', ['$http', '$window', function ($http, $window) {
    var factory = {
        base: "/dqm_online_monitoring/",
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

dqmApp.filter("dqm_statuscode", function() {
    return function(input) {
        if ((input === undefined) || (input === null))
            return "running";

        return input;
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

                var diff_s = (scope.diff - scope.time) / 1000;

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

dqmApp.directive('dqmLumiGraph', function ($window) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'lumi': '=', 'width': '@', 'height': '@' },
        link: function (scope, elm, attrs) {
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);
            var radius = Math.min(width, height) / 2;
            var color = d3.scale.category20c();

            var svg = d3.select(elm[0]).append("svg");
            svg.attr("width", width).attr("height", height);

            var g = svg.append("g");
            g.attr("transform", "scale(" + width / 800 + "," + height / 600 + ")");

			g.append("line")
				.attr("x1", 0).attr("x2", 0)
				.attr("y1", 0).attr("y2", 100)
                .attr("stroke-width", 2)
				.attr("stroke", "black");

            //console.log([2 * Math.PI, radius * radius]);

            //var partition = d3.layout.partition()
            //    .sort(null)
            //    .size([2 * Math.PI, radius * radius])
            //    .value(function(d) { console.log('access', d); return 1; });

            //var arc = d3.svg.arc()
            //    .startAngle(function(d) { return d.x; })
            //    .endAngle(function(d) { return d.x + d.dx; })
            //    .innerRadius(function(d) { return Math.sqrt(d.y); })
            //    .outerRadius(function(d) { return Math.sqrt(d.y + d.dy); });

            //var arcTween = function(a) {
            //    console.log("a", a);
            //    var i = d3.interpolate({x: a.x0, dx: a.dx0}, a);

            //    return function(t) {
            //      var b = i(t);
            //      a.x0 = b.x;
            //      a.dx0 = b.dx;
            //      return arc(b);
            //    };
            //}

			var unpack_streams = function (lumi) {
				var keys = _.keys(lumi);
				return _.map(lumi, function (lst, name) {
					var obj = {
						'name': name,
						'lumi': [],
						'size': [],
						'ts': [],
					};

					_.map(lst, function (x) {
						var splits = x.split(":");
						var tags = splits[1].split(" ");

						splits = splits[0].split(" ");
						obj.lumi.push(parseInt(splits[0]));
						obj.size.push(parseInt(splits[1]));
						obj.ts.push(parseFloat(splits[2]));
					});

					return obj;
				});

			};

            scope.$watch('lumi', function (lumi) {
                if (!lumi) return;

				lumi = unpack_streams(lumi);
                console.log('new data', lumi);
	
                //g.selectAll("g").data(keys).enter().append('g').each(function (x) {
				//	var packed = lumi[x];
				//	var elm = d3.select(this);
				//	var perLumi = (Math.PI*2) / 100;

				//	var data = _.map(packed, function (x) {
				//		var splits = x.split(":");
				//		var tags = splits[1].split(" ");
				//		console.log(splits, tags);

				//		splits = splits[0].split(" ");
				//		var lumi = parseInt(splits[0]);
				//		var size = parseInt(splits[1]);
				//		var ts = parseFloat(splits[2]);

				//		return [lumi, size, ts];
				//	});
	
				//	console.log(data, perLumi);
				//	var startAngle = function (x) {


				//	};

				//	elm.selectAll("path")
				//		.data(data)
				//		.enter().append("svg:path")
				//		.attr("d", d3.svg.arc()
				//			.innerRadius(radius / 4)
				//			.outerRadius(radius / 3)
				//			.startAngle(startAngle)
				//			.endAngle(function (x) { return startAngle(x) + perLumi; })
				//		)
				//		.style("fill", function(d, i) { return color(i); });
                //});

				//svg.selectAll("path")
				//	.data(d3.layout.pie())
				//	.enter().append("svg:path")
				//	.attr("d", d3.svg.arc()
				//	.innerRadius(r / 2)
				//	.outerRadius(r))
				//	.style("fill", function(d) { return color(1); });


                //var path = svg.datum(lumi).selectAll("path")
                //    .data(partition.nodes)
                //    .enter().append("path")
                //    .attr("display", function(d) { return d.depth ? null : "none"; }) // hide inner ring
                //    .attr("d", arc)
                //    .style("stroke", "#fff")
                //    .style("fill", function(d) { console.log('x', d); return color(2); })
                //    .style("fill-rule", "evenodd");

                //path
                //    .data(partition..nodes)
                //    .transition()
                //    .duration(1500)
                //    .attrTween("d", arcTween);

                //D3.selectAll("input").on("change", function change() {
                //    var value = this.value === "count"
                //      ? function() { return 1; }
                //      : function(d) { return d.size; };

                //    path
                //        .data(partition.value(value).nodes)
                //        .transition()
                //        .duration(1500)
                //        .attrTween("d", arcTween);
                //});

            });
        },
    };
});


dqmApp.config(function($routeProvider, $locationProvider) {
  $routeProvider
    .when('/lumi/', { menu: 'lumi', templateUrl: 'templates/lumi.html' })
    .when('/stats/', { menu: 'stats', templateUrl: 'templates/stats.html' })
    .when('/lumi/:run/', { menu: 'lumi', templateUrl: 'templates/lumi.html' })
    .otherwise({ redirectTo: '/lumi' });

  // configure html5 to get links working on jsfiddle
  $locationProvider.html5Mode(false);
});
