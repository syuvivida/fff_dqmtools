var mod = angular.module('dqmGraphApp', []);

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

mod.directive('graphDqmTimestampsLumi', function ($window) {
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
