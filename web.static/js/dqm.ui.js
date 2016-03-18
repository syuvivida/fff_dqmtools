var mod = angular.module('dqm.ui', ['ngAnimate', 'ngRoute', 'ui.bootstrap']);

mod.factory('Alerts', ['$window', function($window) {
    var me = {};

    me.alerts = [];

    me.addAlert = function(body) {
        me.alerts.push(body);
        $window.scrollTo(0, 0);
    };

    me.closeAlert = function(index) {
        me.alerts.splice(index, 1);
    };

    return me;
}]);

mod.directive('alertBrowser', ['Alerts', function (Alerts) {
    return {
        restrict: 'A',
        scope: {},
        link: function(scope, element, attrs, ctrl) {
            scope.Alerts = Alerts;
        },
        template: ""
            + '<alert ng-repeat="alert in Alerts.alerts" type="{{ alert.type }}" close="Alerts.closeAlert($index)">'
            + '  <strong ng-show="alert.strong">{{ alert.strong }}</strong>'
            + '  {{ alert.msg }}'
            + '  <br />{{ alert.comment}}'
            + '</alert>',
    };
}]);

mod.factory('LocParams', ['$location', '$rootScope', function ($location, $rootScope) {
    var me = {};

    me._value = function (v) {
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

    me._clear_object = function (obj) {
        for (var k in obj) {
            if (obj.hasOwnProperty(k))
                delete obj[k];
        }
    };

    // parameters inside a locaton (what we know)
    // cannot be modified by the scope
    me._params_location = {};

    // params inside the scope, we can modify this directly
    me._params = {};

    me._update_from_location  = function () {
        var s = $location.search();

        me._clear_object(me._params_location);
        me._clear_object(me._params);

        _.each(s, function (v, k) {
            me._params_location[k] = v;
            me._params[k] = v;
        });

        //console.log("params", me);
    };

    // change parameter with history
    me.setKey = function (k, v) {
        // this will propage to the _params on location event
        $location.search(k, me._value(v));
    };

    //// these are special "flags", they still modify the _params
    me.setFlag = function (flag_key, flag_char, value_bool) {
        var s = me._params[flag_key] || "";

        if ((value_bool) && (s.indexOf(flag_char) === -1))
            s += flag_char;

        if ((!value_bool) && (s.indexOf(flag_char) !== -1))
            s = s.replace(flag_char, '');

        me._params[flag_key] = s || null;
    };

    me.getFlag = function (flag_key, flag_char) {
        var s = me._params[flag_key] || "";
        return s.indexOf(flag_char) !== -1;
    };

    me.toggleFlag = function (flag_key, flag_char) {
        me.setFlag(flag_key, flag_char, !me.getFlag(flag_key, flag_char));
    };

    // short for function () { return LocParams.p.x; }
    me.watchFunc = function (key) {
        return function () { return me.p[key]; };
    };

    // watcher for async changer (history not advanced)
    $rootScope.$watch(function () { return me._params; }, function () {
        _.each(me._params, function (v, k) {
            var old = me._params_location[k];
            if (old !== v) {
                $location.search(k, me._value(v)).replace();
            };
        });
    }, true);

    $rootScope.$on("$locationChangeSuccess", me._update_from_location);
    me._update_from_location();

    me.p = me._params;
    $rootScope.LocParams = me;

    return me;
}]);

mod.filter("dqm_exitcode_class", function() {
    return function (input) {
        if (!input)
            return "info";

        // document is a header if it does not have "$cd_full" attribute (CachedDocumentCtrl)
        if ((input.exit_code === undefined) && (input.$cd_full === undefined))
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

mod.filter("dqm_megabytes", function() {
    return function(input) {
        var s = input || '';

        if (s.indexOf && s.indexOf(" kB") > -1) {
            s.replace(" kB", "");
            s = parseInt(s) * 1024;
        }

        return parseInt(s) / 1024 / 1024;
    };
});

mod.filter("dqm_disk_pc", function () {
    return function(input) {
        var hit = input || {};
        return parseInt(hit.disk_used) * 100 / hit.disk_total;
    };
});

mod.filter("dqm_int", function() {
    return function(input) {
        var s = input || 0;
        return parseInt(s);
    };
});

mod.filter("dqm_float", function() {
    return function(input) {
        var s = input || 0;
        return parseFloat(s);
    };
});

mod.filter("dqm_lumi_seen", function() {
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

mod.filter("dqm_shorten_tag", function() {
    return function(input) {
        var s = input || '';
        s = s.replace(/(dqm_)/g, '');
        s = s.replace(/(-live$)/g, '');
        s = s.replace(/(_sourceclient$)/g, '');
        return s;
    };
});

mod.filter("dqm_exitcode_filter", function() {
    return function (docs, hide_str) {
        var hs = hide_str || "";

        if (!docs)
            return docs;

        return _.filter(docs, function (doc) {
            var ec = doc.exit_code;
            var f;

            if ((ec === null) || (ec === undefined)) {
                f = "r";
            } else if ((ec === 0) || (ec === "0")) {
                f = "s";
            } else {
                f = "c";
            }

            return (hs.indexOf(f) == -1);
        });
    };
});

mod.directive('prettifySource', function ($window) {
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

mod.directive('dqmLog', function ($window, $interval) {
    return {
        restrict: 'E',
        scope: { 'log': '=' },
        link: function (scope, elm, attrs) {
            var format_log = function (log) {
                if (_.isArray(log)) {
                    var rlog = _.clone(log);
                    return rlog.join("");
                }

                if ((log.slice(0, 2) == "[\n") && (log.slice(-2) == "\n]")) {
                    // esMonitoring bug, puts json in a text field
                    console.log("hey");
                    var arr = JSON.parse(log);
                    return arr.join("");
                }

                return "logfile not available";
            };

            var update = function () {
                scope.flog = format_log(scope.log);
            }

            scope.$watch('log', update);
        },
        template: '<pre class="log" ng-bind="flog"></pre>'
    };
});

mod.directive('dqmLumiState', function ($window, $interval) {
    return {
        restrict: 'E',
        scope: { 'state': '=', 'lumi': '=' },
        link: function (scope, elm, attrs) {
            var error_dct = {
                "open: file iterator": 0,
                "close: eof": 0,
                "close: forced end-of-run": 0,
                "close: skipping to another file": 0,
                "close: not loading": 0,
                "close: ok": 0,
                "skipped: fast-forward to the latest lumi": 0,

                "": 0 // ignore these, i don't know what causes them
            };

            scope.$watch('state', function (arr) {
                scope.cls = "label-success";
                scope.title = "";

                if (!arr) return;

                var analyzeEntry = function (value, key) {
                    var n = error_dct[value];
                    if (n === 0) {
                        return false;
                    }

                    scope.title = "[" + key + "]" + ": " + value + " (check the log)";
                    scope.cls = "label-warning";
                    return true;
                };

                _.find(arr, analyzeEntry);
            });
        },
        template: '<span class="label" ng-class="cls" title="{{ title }}">{{ lumi }}</span>'
    };
});





mod.directive('dqmTimediffField', function ($window, $interval) {
    return {
        restrict: 'E',
        scope: { 'time': '=' },
        link: function (scope, elm, attrs) {
            var getNow = function () { return Math.floor(Date.now() / 1000); };

            var update = function () {
                var ref = getNow();
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

            update();
            var updater = $interval(update, 1000);
            scope.$on('$destroy', function() {
                $interval.cancel(updater);
            });
        },
        template: '<span class="label label-success delay" ng-class="diff_class">{{ diff_s | number:0 }}</span>'
    };
});

mod.directive('dqmSortedTable', function () {
    return {
        restrict: 'A',
        scope: {
            'key': '=',
            'path': '=',
        },
        controller: function ($scope) {
            var me = this;

            this.sort_paths = {};

            this.register = function (key, path, default_value) {
                var dct = {
                    'key': key,
                    'path': path
                };

                me.sort_paths[key] = dct;

                if (default_value) {
                    me.sort_paths["__default"] = dct;
                }

                me.update();
            };

            this.toggleKey = function (key) {
                if ($scope.key != key) {
                    $scope.key = key;
                } else if ($scope.key[0] == "-") {
                    $scope.key = $scope.key.slice(1);
                } else {
                    $scope.key = "-" + $scope.key;
                }
            };

            this.update = function () {
                var sliced = $scope.key || "";
                var reversed = false;
                var path_dct = {};

                if (sliced[0] === "-") {
                    sliced = sliced.slice(1);
                    reversed = true;
                }

                if (me.sort_paths[sliced]) {
                    path_dct = me.sort_paths[sliced];
                    $scope.path = (reversed?"-":"+") + path_dct.path;
                } else {
                    // put default value
                    path_dct = me.sort_paths["__default"] || {};
                    $scope.path = (reversed?"-":"+") + path_dct.path;
                };

                // this only needed for dqmSortHeader
                me.sort_key = path_dct.key;
                me.sort_reversed = reversed;
            }

            $scope.$watch("key", this.update);
        }
    };
});

mod.directive('dqmSortHeader', function () {
    return {
        require: '^dqmSortedTable',
        restrict: 'A',
        replace: true,
        transclude: true,
        scope: { 'key': "@dqmSortHeader", 'path': '@', 'defaultKey': '@' },
        link: function(scope, element, attrs, ctrl) {
            ctrl.register(scope.key, scope.path || scope.key, scope.defaultKey);
            scope.ctrl = ctrl;
        },
        template: ""
            + "<th class='sort-header' ng-class='{ \"sort-key\": ctrl.sort_key == key, \"sort-reversed\": ctrl.sort_reversed }' ng-click='ctrl.toggleKey(key)'>"
            + "<span ng-transclude />"
            + "<span ng-show='(ctrl.sort_key == key) && (!ctrl.sort_reversed)' class='sort-carret glyphicon glyphicon-chevron-up'></span>"
            + "<span ng-show='(ctrl.sort_key == key) && (ctrl.sort_reversed)' class='sort-carret glyphicon glyphicon-chevron-down'></span>"
            + "</th>",
    };
});

mod.directive('dqmRefresh', function ($interval, $window) {
    return {
        restrict: 'A',
        scope: { 'doc': '=dqmRefresh'},
        link: function (scope, element, attrs) {
            element.addClass("dqm-refresh");

            var created = new Date();

            var update = function () {
                if (element.hasClass("dqm-refresh-on"))
                    return;
                
                // first few updates should be ignored
                if (((new Date()) - created) < 3000)
                    return;

                element.addClass("dqm-refresh-on");
                element.removeClass("dqm-refresh-off");
                $window.setTimeout(function () {
                    element.addClass("dqm-refresh-off");
                    element.removeClass("dqm-refresh-on");
                }, 2000);
            };

            scope.$watch("doc._rev", update);
        },
    };

});
