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

var process_stream_data = function (data, limit_lumi, date_metric, old_graph_data, do_interpolate) {
    var ret = {};

    if ((!data) || (!data.extra) || (!data.extra.streams)) {
        // we have no data available
        // so just render "no_data"

        return null;
    };

    // we want to extract "disabled" field from the old data
    var preserve = {};
    if (old_graph_data) {
        _.each(old_graph_data.streams, function (obj) {
            preserve[obj.key] = _.clone(obj);
        });
    };

    var date_start = data.extra.global_start;
    var streams = data.extra.streams;

    var filter = function (arr) {
        if (!arr)
            return arr;

        if (!limit_lumi)
            return arr;

        return arr.slice(Math.max(arr.length - limit_lumi, 0))
    };

    var interpolate = function (arr) {
        if (!arr)
            return arr;

        var ret = [];

        _.each(arr, function (v) {
            var last_entry = ret[ret.length - 1] || null;
            if (last_entry) {
                var i = parseInt(last_entry.lumi) + 1;
                var d = parseInt(v.lumi);
                for (;i < v.lumi; i++) {
                    ret.push({
                        'lumi': i,
                        'mtime': -1,
                        'ctime': -1,

                        'start_offset': -1,
                        'delay': 0,

                        'evt_accepted':  0,
                        'evt_processed': 0,
                        'fsize': 0,
                    });
                }
            }

            ret.push(v);
        });

        return ret;
    };

    var pretty_key = function (key) {
        var m = /^stream([A-Za-z0-9]+).*$/.exec(key);

        if (m) {
            return m[1];
        }

        return key;
    };

    var min_lumi = NaN;
    var max_lumi = NaN;

    var graph_data = _.map(_.keys(streams), function (k) {
        var lumis = filter(streams[k].lumis);
        var mtimes = filter(streams[k].mtimes);
        var ctimes = filter(streams[k].ctimes);
        if (!ctimes)
            ctimes = mtimes;

        var evt_accepted = filter(streams[k].evt_accepted);
        var evt_processed = filter(streams[k].evt_processed);
        var fsize = filter(streams[k].fsize);

        var key = pretty_key(k);
        var e = preserve[key] || {};

        e["key"] = pretty_key(k);
        e["stream"] = k;
        e["values"] = _.map(lumis, function (_lumi, index) {
            var lumi = parseInt(lumis[index]);
            var mtime = mtimes[index];
            var ctime = ctimes[index];

            if (!(min_lumi <= lumi)) min_lumi = lumi;
            if (!(max_lumi >= lumi)) max_lumi = lumi;

            // timeout from the begging of the run
            var dtime = mtime;
            if (date_metric == "ctime")
                dtime = ctime;

            var start_offset = dtime - date_start - LUMI;
            var lumi_offset = (lumi - 1) * LUMI;

            // timeout from the time we think this lumi happenned
            var delay = start_offset - lumi_offset;

            return {
                // 'x': lumi,
                // 'y': delay,

                'lumi': lumi,
                'mtime': mtime,
                'ctime': ctime,

                'start_offset': start_offset,
                'delay': delay,

                'evt_accepted': evt_accepted[index],
                'evt_processed': evt_processed[index],
                'fsize': fsize[index],

                'size': 1,
                'shape': "circle",
            }
        });

        e["values"] = _.sortBy(e["values"], "lumi");
        if (do_interpolate) {
            e["values"] = interpolate(_.sortBy(e["values"], "lumi"));
        }
        e["global_start"] = date_start;

        return e;
    });

    // calculate ticks, lazy, inefficient way
    var x_scale = d3.scale.linear().domain([min_lumi, max_lumi]);
    var ticks = x_scale.ticks(10);

    return {
        'streams': graph_data,
        'global_start': date_start,
        'ticks': ticks,
    };
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
        + "File c-time: <strong>" + format_timestamp(p.ctime) + "</strong><br />"
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
        scope: { 'data': '=', 'width': '@', 'height': '@', 'showAll': "=", 'metric': '=' },
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
                .color(d3.scale.category10().range())
                .margin({left: 100});

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
                    scope.graph_data = process_stream_data(d, null, scope.metric, scope.graph_data);
                    chart.forceX(null)
                } else {
                    scope.graph_data = process_stream_data(d, 100, scope.metric, scope.graph_data);
                    chart.forceX(null)
                }
            };

            scope.$watch("graph_data", function (data) {
                if (data && data.streams) {
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

            scope.$watch("metric", function (newv, oldv) {
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
                .reduceXTicks(false ) // default is true and we don't want that
                .rotateLabels(0)      //Angle to rotate x-axis labels.
                .groupSpacing(0.1)    //Distance between each group of bars.

            chart.tooltipContent(process_steam_tooltip);

            var tickValues = function (datum) {
                // for an unknown reason multiBar uses ordinal scale
                // but we wan't to aligh with the linear scale (scatter plot)
                //var x = d3.scale.linear();
                return [401, 405, 500];
            };

            // Axis settings
            chart.xAxis
                .axisLabel("Lumisection")
                //.tickValues(tickValues)
                .tickFormat(d3.format('.00f'));

            chart.yAxis
                .axisLabel("Events accepted")
                .tickFormat(d3.format('.00f'));

            var setData = function () {
                var d = scope.data;
                var v = scope.showAll;

                if (v) {
                    scope.graph_data = process_stream_data(d, null, 'mtime', scope.graph_data, true);
                    //chart.forceX([0, 1])
                } else {
                    scope.graph_data = process_stream_data(d, 100, 'mtime', scope.graph_data, true);
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
