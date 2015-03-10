var mod = angular.module('dqm.ui', ['ngRoute', 'ui.bootstrap']);

mod.filter("dqm_exitcode_class", function() {
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
        var s = input || '';
        return parseInt(s);
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
            var updater = $interval(update, 5000);
            scope.$on('$destroy', function() {
                $interval.cancel(updater);
            });
        },
        template: '<span class="label label-success" ng-class="diff_class">{{ diff_s | number:0 }}&nbsp;s.</span>'
    };
});

mod.directive('dqmSortedTable', function () {
    return {
        restrict: 'A',
        scope: { 
            'key': '=',
            'reversed': '=',
			'path': '=',
        },
        controller: function ($scope) {
            var me = this;

			this.sort_paths = {};

			this.register = function (key, path) {
				me.sort_paths[key] = path;
				me.update();
			};

            this.toggleKey = function (key) {
                if ($scope.key != key) {
                    $scope.key = key;
                } else {
                    $scope.reversed = !$scope.reversed;
                }
            };

			this.update = function () {
				var key = $scope.key;

				if (! me.sort_paths[key]) {
					$scope.path = "";
				} else {
					$scope.path = ($scope.reversed?"-":"+") + me.sort_paths[key];
				}

				// this only needed for dqmSortHeader
				me.sort_key = $scope.key;
				me.sort_reversed = $scope.reversed;
			}

            $scope.$watch("key", this.update);
            $scope.$watch("reversed", this.update);
        }
    };
});

mod.directive('dqmSortHeader', function () {
    return {
        require: '^dqmSortedTable',
        restrict: 'A',
        replace: true,
        transclude: true,
        scope: { 'key': "@dqmSortHeader", 'path': '@' },
        link: function(scope, element, attrs, ctrl) {
			ctrl.register(scope.key, scope.path || scope.key);
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
