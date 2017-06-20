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


mod.directive('graphDqmDelaysLumi', function ($window, $timeout, DataUtils) {
    // the "drawing" code is taken from http://bl.ocks.org/mbostock/4063423
    var d3 = $window.d3;

    return {
        restrict: 'E',
        scope: { 'data': '=', 'width': '@', 'height': '@', 'type': '@', 'metric': '=', 'tag': '=', 'zoom': '=', 'showKeys': '=' },
        link: function (scope, elm, attrs) {
            var me = {};
            var width = parseInt(scope.width);
            var height = parseInt(scope.height);

            elm.append("<canvas></canvas>");
            elm.append("<div class='chartjs-custom-tooltip'></div>");

            // needed for chart.js / responsive to work
            // oterwise .parent() will haven no width
            elm.css("display", "block");

            me.canvas = elm.find("canvas")[0];
            me.canvas.width = width || 500;
            me.canvas.height = height || 500;

            // create chartjs object
            me.chart = Chart.Scatter(me.canvas, {
                data: { datasets: [], source: null },
                type: 'line',
                options: {
                    events: ["click", "mouseout", "touchstart"], // shut up events such as mousemove
                    animation : false,
                    pointHitDetectionRadius: 1,
                    responsive: true,
                    hover:  { mode: null },
                    tooltips: { enabled: false },
                    title: {
                        display: true,
                        text: "(set me)",
                        fontSize: 18,
                    },
                    scales: {
                        xAxes: [{
                            position: "bottom",
                            gridLines: {
                                zeroLineColor: "rgba(0,0,0,1)"
                            },
                            ticks: {
                                max: 15,
                            },
                            scaleLabel: {
                                display: false,
                                labelString: "Lumisection Number",
                            },
                            beforeSetDimensions: function (scale) {
                                // hack - otherwise we get super ugly spacing right
                                // this requires data to have max_lumi
                                scale.options.ticks.max = scale.chart.data.max_lumi;
                            },
                        }],
                        yAxes: [{
                            type: "linear",
                            position: "left",
                            id: "y-axis-1",
                            ticks: {
                                beginAtZero: true,
                                fontFamily: "'Lucida Console', Monaco, monospace",
                                callback: function(label, index, labels) {
                                    return ("          " + label).slice(-8);
                                },
                                min: 0,
                                maxTicksLimit: 5,
                            },
                            scaleLabel: {
                                fontSize: 16,
                                display: true,
                                labelString: "(set me)",
                            }
                        }],
                    }
                }
            });

            // implement tooltip, the one from chart.js is slow and does not really do what I want
            angular.element(me.canvas).mousemove(function (ev) {
                var div = elm.find("div.chartjs-custom-tooltip");
                var elements = me.chart.getElementAtEvent(ev);

                if (! elements.length) {
                    div.css({ "opacity": 0.0, "visibility": "hidden" });
                    return;
                }

                var element = elements[0];
                var dataset = element._chart.config.data.datasets[element._datasetIndex];
                var point = dataset.data[element._index];

                var data = element._chart.config.data.source;
                var stream_key = dataset.stream_key;
                var index = point.index;
                div.html(DataUtils.make_file_tooltip(data, stream_key, index));

                var div_height = div.height();
                var transform = (div_height < (ev.clientY - 100)) ? "translate(-50%, -105%)" : "translate(-50%, 5%)";

                var offset_left = ev.pageX;
                var offset_top = ev.pageY;;
                div.css({
                  "opacity": 1,
                  "visibility": "visible",
                  "left": offset_left + 'px',
                  "top": offset_top + 'px',
                  "transform": transform,
                });
            });

            angular.element(me.canvas).mouseout(function (ev) {
                var div = elm.find("div.chartjs-custom-tooltip");
                div.css({ "opacity": 0.0, "visibility": "hidden" });
            });

            angular.element(me.canvas).dblclick(function (ev) {
                var new_zoom = null;
                if (! scope.zoom) {
                    var elements = me.chart.getElementAtEvent(ev);
                    if (elements.length > 0) {
                        var dataset = elements[0]._chart.config.data.datasets[elements[0]._datasetIndex];
                        var center = dataset.data[elements[0]._index].x;

                        var min = Math.max(0, center - 50);
                        var max = center + 50;

                        new_zoom = "" + min + ".." + max;
                    }
                }

                $timeout(function () { scope.zoom = new_zoom; });
            });

            angular.element(me.canvas).click(function (ev) {
                $timeout(function () { updateLabelsFromChart(); }, 20);
            });

            var setData = function (data, metric, zoom) {
                var colors = d3.scale.category10().range();
                var metric = metric || "delay_mtime";

                // set tittles
                var titles = {
                    'evt_accepted': "DQM Event Rate",
                    'delay_mtime': "DQM File Delivery",
                    'delay_ctime': "DQM File Delivery",
                };
                var title = titles[metric] || "DQM Lumi Graph (" + metric + ")";
                title = title + " [" + scope.tag + "]";

                // set y axis label
                var ytitles = {
                    'evt_accepted': "Accepted Events per lumi",
                    'delay_mtime': "File delivery delay (mtime, s.)",
                    'delay_ctime': "File delivery delay (ctime, s.)",
                };
                var ytitle = ytitles[metric] || "Unknown (" + metric + ")";
                me.chart.options.scales.yAxes[0].scaleLabel.labelString = ytitle;

                // if we need to show a line
                var showLine = ((metric != 'delay_mtime') && (metric != 'delay_ctime'));

                // figure out the filter, if any
                var filter = null;
                var zoom_re = /^(\d+)..(\d+)$/.exec(zoom);
                if (zoom_re) {
                    filter = { 'min': parseInt(zoom_re[1]), 'max': parseInt(zoom_re[2]) };
                    me.chart.options.title.text = title + " (Lumisections " + filter.min + " .. " + filter.max + ")";
                } else {
                    me.chart.options.title.text = title;
                }


                // check for showKeys
                me.chart.data.showKeys = scope.showKeys;
                var showKeysEnabled = (scope.showKeys !== null) && (scope.showKeys !== undefined);
                var showKeysFilter = (scope.showKeys || "").split(",");

                // create a dataset for each stream
                var max_lumi = 0;
                var datasets = [];

                var stream_keys = _.sortBy(_.keys(data.extra.streams), function (key) {
                    return DataUtils.pretty_stream_key(key);
                });

                _.each(stream_keys, function (stream_key, stream_index) {
                    var label = DataUtils.pretty_stream_key(stream_key);

                    var dataset = {
                        stream_key: stream_key,
                        label: label,
                        xAxisID: "x-axis-1",
                        yAxisID: "y-axis-1",
                        pointRadius: showLine ? 3 : 3,
                        pointHitRadius: 10,
                        data: [],
                        showLine: showLine,
                        fill: false,
                        backgroundColor: colors[stream_index],
                        borderColor: d3.rgb(colors[stream_index]).darker(1),
                        hidden: showKeysEnabled?(_.indexOf(showKeysFilter, label) == -1):false,
                    };

                    // unpack lumi data
                    _.each(data.extra.streams[stream_key].lumis, function (_lumi, index) {
                        if ((filter) && ((_lumi < filter.min) || (_lumi > filter.max)))
                            return;

                        var v = DataUtils.calc_extended(data, stream_key, index);
                        var y = v[metric];

                        // since the scale is set, they won't be displayed otherwise
                        if (((metric == "delay_mtime") || (metric == "delay_ctime")) && (y < 0))
                            y = 0;

                        dataset.data.push({ 'x': v.lumi, 'y': y, "index": index });

                        if (max_lumi < v.lumi)
                            max_lumi = v.lumi;
                    });

                    datasets.push(dataset);
                });

                me.chart.data.datasets = datasets;
                me.chart.data.source = data;
                me.chart.data.max_lumi = max_lumi;

                me.chart.update();
                console.log("Graph update:", scope.data, me.chart.data.datasets);
            };

            scope.$on('$destroy', function () {
                angular.element(me.canvas).off();
                me.chart.destroy();
            });

            var updateData = function () {
                if (me.redraw_promise)
                    return;

                me.redraw_promise = $timeout(function () {
                    setData(scope.data, scope.metric, scope.zoom);
                }, 50);

                me.redraw_promise.then(function () {
                    me.redraw_promise = null;
                    delete me.redraw_promise;
                });
            };

            // this runs from $timeout after click handler
            var updateLabelsFromChart = function () {
                if (! me.chart.data) return;
                if (! me.chart.data.datasets) return;

                var enabled = [];
                var filter_enabled = false;
                _.each(me.chart.data.datasets, function (elm, index) {
                    var visible = me.chart.isDatasetVisible(index);
                    if (visible) {
                        enabled.push(elm.label);
                    } else {
                        filter_enabled = true;
                    }
                });
                enabled.sort();

                scope.showKeys = filter_enabled?(enabled.join(",")):null;;
                me.chart.data.showKeys = scope.showKeys;
            };

            var updateLabelsFromScope = function () {
                if (! me.chart.data) return;

                // skip if it was set from updateLabelsFromChart
                if (scope.showKeys == me.chart.data.showKeys) return;

                me.chart.data.showKeys = scope.showKeys;
                updateData();
            };

            scope.$watch("data", updateData);
            scope.$watch("metric", updateData);
            scope.$watch("zoom", updateData)
            scope.$watch("showKeys", updateLabelsFromScope)
        }
    };

});
