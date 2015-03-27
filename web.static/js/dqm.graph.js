var mod = angular.module('dqm.graph', []);

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

var LUMI = 23.310893056;

var process_steam_data = function (data, limit_lumi, old_graph_data) {
    var ret = {};

    if ((!data) || (!data.extra) || (!data.extra.streams)) {
        // we have no data available
        // so just render "no_data"

        return null;
    };

    // we want to extract "disabled" field from the old data
    var preserve = {};
    if (old_graph_data) {
        _.each(old_graph_data, function (obj) {
            preserve[obj.key] = _.clone(obj);
        });
    };

    var date_start = data.extra.global_start;
    var streams = data.extra.streams;

    // from key:value, make a [value], and key being an entry in value
    var filter = function (arr) {
        if (!arr)
            return arr;

        if (!limit_lumi)
            return arr;

        return arr.slice(Math.max(arr.length - limit_lumi, 0))
    };

    var pretty_key = function (key) {
        var m = /^stream([A-Za-z0-9]+).*$/.exec(key);

        if (m) {
            return m[1];
        }

        return key;
    };

    var graph_data = _.map(_.keys(streams), function (k) {
        var lumis = filter(streams[k].lumis);
        var mtimes = filter(streams[k].mtimes);
        var evt_accepted = filter(streams[k].evt_accepted);
        var evt_processed = filter(streams[k].evt_processed);
        var fsize = filter(streams[k].fsize);

        var key = pretty_key(k);
        var e = preserve[key] || {};

        e["key"] = pretty_key(k);
        e["stream"] = k;
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
                'size': 1,
                'shape': "circle",

                'evt_accepted': evt_accepted[index],
                'evt_processed': evt_processed[index],
                'fsize': fsize[index],
            }
        });
        e["global_start"] = date_start;

        return e;
    });

    console.log("graph_data", graph_data);
    return graph_data;
};

var format_timestamp = function (timestamp) {
    var d = new Date(parseFloat(timestamp)*1000);
    return d.toLocaleString();
};

var process_steam_tooltip = function(k, _v1, _v2, o) {
    var p = o.point;
    var gs = o.series.global_start;

    return ""
        + "<h3>" + k + "</h3>"
        + "<span>"
        + "Lumi number: <strong>" + p.lumi + "</strong><br />"
        + "Stream: <strong>" + o.series.stream + "</strong><br />"
        + "Events accepted / processed: <strong>" + p.evt_accepted + " / " + p.evt_processed + "</strong><br />"
        + "File size: <strong>" + d3.format('.02f')(parseFloat(p.fsize) / 1024 / 1024) + " mb.</strong><br />"
        + "-<br />"
        + "File m-time: <strong>" + format_timestamp(p.mtime) + "</strong><br />"
        + "Time offset from the expected first delivery [delivery_start_offset]: <strong>" + p.start_offset  + " (seconds)</strong><br />"
        + "Delay [delivery_start_offset - (lumi_number - 1)*23.3]: <strong>" + p.delay + " (seconds)</strong><br />"
        + "-<br />"
        + "Run start (m-time on .runXXXX.global): <strong>" + format_timestamp(gs) + "</strong><br />"
        + "Delivery start (run_start + 23.3): <strong>" + format_timestamp(gs + LUMI) + "</strong><br />"
        + "</span>";
};


mod.directive('graphDqmTimestampsLumi', function ($window) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@', 'showAll': "=" },
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
                //.transitionDuration(350)
                .color(d3.scale.category10().range());

            chart.tooltipContent(process_steam_tooltip);
            chart.scatter.onlyCircles(false);

            // Axis settings
            chart.xAxis
                .axisLabel("Lumisection")
                .tickFormat(d3.format('.00f'));

            chart.yAxis
                .axisLabel("Delay (s)")
                .tickFormat(d3.format('.01f'));

            var setData = function () {
                var d = scope.data;
                var v = scope.showAll;

                if (v) {
                    scope.graph_data = process_steam_data(d, null, scope.graph_data);
                    chart.forceX([0, 1])
                } else {
                    scope.graph_data = process_steam_data(d, 100, scope.graph_data);
                    chart.forceX(null)
                }
            };

            scope.$watch("graph_data", function (data) {
                var datum = data;

                if (!datum) {
                    datum = [];
                    // nvd3 does not clear the graph properly, we do it for them
                    svg.selectAll("*").remove();
                }

                svg.datum(datum).call(chart);
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

mod.directive('graphDqmEventsLumi', function ($window) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@', 'showAll': "=" },
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
                .reduceXTicks(true)   //If 'false', every single x-axis tick label will be rendered.
                .rotateLabels(0)      //Angle to rotate x-axis labels.
                .groupSpacing(0.1)    //Distance between each group of bars.

            chart.tooltipContent(process_steam_tooltip);

            // Axis settings
            chart.xAxis
                .axisLabel("Lumisection")
                .tickFormat(d3.format('.00f'));

            chart.yAxis
                .axisLabel("Events accepted")
                .tickFormat(d3.format('.00f'));

            var setData = function () {
                var d = scope.data;
                var v = scope.showAll;

                if (v) {
                    scope.graph_data = process_steam_data(d, null, scope.graph_data);
                    //chart.forceX([0, 1])
                } else {
                    scope.graph_data = process_steam_data(d, 100, scope.graph_data);
                    //chart.forceX(null)
                }
            };

            scope.$watch("graph_data", function (data) {
                var datum = data;

                if (!datum) {
                    datum = [];
                    // nvd3 does not clear the graph properly, we do it for them
                    svg.selectAll("*").remove();
                }

                svg.datum(datum).call(chart);
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
