var mod = angular.module('dqm.utils', ['dqm.ui', 'ui.bootstrap']);

mod.controller('UtilsCtrl', ['$scope', '$http', '$modal', '$window', 'Alerts', function ($scope, $http, $modal, $window, Alerts) {
    $scope.openDeleteDialog = function (ids_) {
        var ids = _.clone(ids_);
        ids.sort();

        var instance = $modal.open({
            templateUrl: 'templates/modalDropDialog.html',
            controller: 'SimpleDataDialogCtrl',
            scope: $scope,
            resolve: {
                data: function () { return { ids: ids }; }
            }
        });

        instance.result.then(function (ret) {
            // perform the actual drop
            var body = { ids: ids};
            var p = $http.post("/utils/drop_ids", body);

            p.then(function (resp) {
                Alerts.addAlert({ type: 'success', strong: "Success!", msg: resp.data + " Please reload the page (ctrl+r)." });
            }, function (resp) {
                Alerts.addAlert({ type: 'danger', strong: "Failure!", msg: resp.data });
            });
        }, function () {
            // aborted, do nothing
        });
    };

    $scope.openKillDialog = function (doc) {
        var instance = $modal.open({
            templateUrl: 'templates/modalKillLumi.html',
            controller: 'SimpleDataDialogCtrl',
            scope: $scope,
            resolve: {
                data: function () {
                    return { doc: doc };
                }
            }
        });

        instance.result.then(function (ret) {
            var body = { pid: doc.pid, signal: ret.signal };
            var p = $http.post("/utils/kill_proc/" + doc._id, body);

            p.then(function (resp) {
                Alerts.addAlert({ type: 'success', strong: "Success!", msg: resp.data });
            }, function (resp) {
                Alerts.addAlert({ type: 'danger', strong: "Failure!", msg: resp.data });
            });
        }, function () {
            // aborted, do nothing
        });
    };

	$scope.maxTime = function (doc) {
		if ((!doc) || (!doc.extra))
			return 0;

		var to_max = []
		_.map(doc.extra.streams, function (v, k) {
			if (v.mtimes.length)
				to_max.push(v.mtimes[v.mtimes.length - 1]);

			if (v.ctimes.length)
				to_max.push(v.mtimes[v.mtimes.length - 1]);
		});

		return _.max(to_max);
	};
}]);

mod.controller('SimpleDataDialogCtrl', function ($scope, $modalInstance, data) {
    $scope.data = data;
});
