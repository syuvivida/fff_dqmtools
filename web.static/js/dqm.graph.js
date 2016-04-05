var mod = angular.module('dqm.graph', ['dqm.db']);

mod.directive('dqmMemoryGraph', function ($window) {
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

mod.directive('dqmLumiGraph', function ($window) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@' },
        link: function (scope, elm, attrs) {
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);

            var div = d3.select(elm[0]).append("div");
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

            var make_tooltip = function(k, _v1, _v2, o) {
                var dt = o.point._other;

                return ""
                    + "<h3>" + k + "</h3>"
                    + "<span>"
                    + "Lumi number: <strong>" + dt.n + "</strong><br />"
                    + "Lumi processing time (ms.): <strong>" + dt.nmillis + "</strong><br />"
                    + "Lumi events: <strong>" + dt.nevents + "</strong><br />"
                    + "Lumi rate: <strong>" + dt.rate + "</strong><br />"
                    + "-<br />"
                    //+ "I know this tooltip is placed incorrectly, <br />but I have no idea how to properly position it."
                    + "</span>";
            };


            chart.tooltipContent(make_tooltip);

            chart.xAxis
                .axisLabel('Time')
                .tickFormat(d3.time.format('%X'));

            chart.yAxis
                .axisLabel('Events processed last lumi')
                .tickFormat(d3.format('.02f'));

            scope.$watch("data", function (data) {
                if (!data)
                    return;

                // unpack the data
                // we get "timestamp" -> "statm"
                var keys = _.keys(data);
                keys.sort()

                var streams = {
                    'nevents': {
                        'key': 'Events',
                        'values': []
                    }
                };

                _.each(keys, function (key) {
                    var time = new Date(parseInt(key)*1000);
                    streams["nevents"].values.push({
                        'x': time,
                        'y': parseInt(data[key]["nevents"]),
                        '_other': data[key]
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


mod.directive('graphDqmTimestampsLumi', function ($window, DataUtils) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@', 'showAll': "=", 'metric': '=', 'tag': '=' },
        link: function (scope, elm, attrs) {
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);

            var div = d3.select(elm[0]).append("div");
            var svg = div.append("svg");
            svg.attr("width", width).attr("height", height);

            var f_mtime = function (x) { return x['delay_mtime']; };
            var f_ctime = function (x) { return x['delay_ctime']; };

            var chart = nv.models.scatterChart()
                .showDistX(true)
                .showDistY(true)
                .x(function (x) { return x['lumi']; })
                .y(f_mtime)
                .forceY([0, 60])
                .forceX([0, 1])
                .useVoronoi(false)
                //.transitionDuration(350)
                .color(d3.scale.category10().range())
                .margin({left: 100});

            chart.tooltipContent(DataUtils.make_file_tooltip);
            chart.scatter.onlyCircles(false);

            // Axis settings
            chart.xAxis
                .axisLabel("Lumisection (source: " + scope.tag + ")")
                .tickFormat(d3.format('.00f'));

            chart.yAxis
                .axisLabel("Delay (s)")
                .tickFormat(d3.format('.01f'));

            var setData = function () {
                var d = scope.data;
                var v = scope.showAll;

                if (v) {
                    scope.graph_data = DataUtils.process_stream_data(d, null, scope.graph_data);
                    chart.forceX(null)
                } else {
                    scope.graph_data = DataUtils.process_stream_data(d, 100, scope.graph_data);
                    chart.forceX(null)
                }
            };

            var updateGraph = function () {
                var data = scope.graph_data;

                var y_f = f_mtime;
                if (scope.metric == 'ctime') {
                    y_f = f_ctime;
                }

                chart.y(y_f);

                if (data && data.streams) {
                    svg.datum(data.streams).call(chart);
                } else {
                    // nvd3 does not clear the graph properly, we do it for them
                    svg.selectAll("*").remove();
                    svg.datum([]).call(chart);
                }

                chart.update();
            };

            scope.$watch("data", setData);
            scope.$watch("showAll", setData);

            scope.$watch("graph_data", updateGraph);
            scope.$watch("metric", updateGraph);

        }
    };
});

mod.directive('graphDqmEventsLumi', function ($window, DataUtils) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@', 'showAll': "=", 'tag': '=' },
        link: function (scope, elm, attrs) {
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);

            var div = d3.select(elm[0]).append("div");
            var svg = div.append("svg");
            svg.attr("width", width).attr("height", height);

            var chart = nv.models.multiBarChart()
                .x(function (x) { return x['lumi']; })
                .y(function (x) { return x['evt_accepted']; })
                .margin({left: 100})
                //.forceY([0, 60])
                //.forceX([0, 1])
                //.useVoronoi(false)
                //.transitionDuration(350)
                //.color(d3.scale.category10().range());
                .showControls(true)
                .transitionDuration(0)
                .reduceXTicks(false ) // default is true and we don't want that
                .rotateLabels(0)      //Angle to rotate x-axis labels.
                .groupSpacing(0.1)    //Distance between each group of bars.
                .color(d3.scale.category10().range());

            chart.tooltipContent(DataUtils.make_file_tooltip);

            var tickValues = function (datum) {
                // for an unknown reason multiBar uses ordinal scale
                // but we wan't to aligh with the linear scale (scatter plot)
                //var x = d3.scale.linear();
                return [401, 405, 500];
            };

            // Axis settings
            chart.xAxis
                .axisLabel("Lumisection (source: " + scope.tag + ")")
                //.tickValues(tickValues)
                .tickFormat(d3.format('.00f'));

            chart.yAxis
                .axisLabel("Events accepted")
                .tickFormat(d3.format('.00f'));

            var setData = function () {
                var d = scope.data;
                var v = scope.showAll;

                if (v) {
                    scope.graph_data = DataUtils.process_stream_data(d, null, scope.graph_data, true);
                    //chart.forceX([0, 1])
                } else {
                    scope.graph_data = DataUtils.process_stream_data(d, 100, scope.graph_data, true);
                    //chart.forceX(null)
                }
            };

            scope.$watch("graph_data", function (data) {
                if (data && data.streams) {
                    chart.xAxis.tickValues(data.ticks);
                    svg.datum(data.streams).call(chart);
                } else {
                    // nvd3 does not clear the graph properly, we do it for them
                    svg.selectAll("*").remove();
                    svg.datum([]).call(chart);
                }

                chart.update();
            });

            scope.$watch("data", setData);
            scope.$watch("showAll", function (newv, oldv) {
                if (newv === oldv)
                    return;

                setData();
            });
        }
    };
});
