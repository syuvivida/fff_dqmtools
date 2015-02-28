var mod = angular.module('dqmDatabaseApp', []);

mod.factory('SyncPool', ['$http', '$window', function ($http, $window) {
	var factory = {};

	var make_sync_uri = function (host, port) {
		var l = window.location;
		var ws_uri;

		if (l.protocol === "https:") {
		    ws_uri = "wss:";
		} else {
		    ws_uri = "ws:";
		}

		if (host === undefined)
			host = l.hostname;

		if (port === undefined)
			port = l.port;

		ws_uri += "//" + host + ":" + port + "/sync";
		return ws_uri;
	};

	var base_uri = make_sync_uri();

	var connection_class = function (ws_uri) {
		var me = this;

		this.ws_uri = ws_uri;
		this.last_ref = null;
		this.retry_timeout = 5;
		this.retry_count = 0;

		this.reopen = function () {
			console.log("(Re-)Created connection: ", me.ws_uri, me);

			me.retry_timeout = 5;
			me.retry_count = me.retry_count + 1;

			var ws = new WebSocket(me.ws_uri);
			me.ws = ws;

			ws.onopen = function() {
			    //ws.send("Hello, world");
				me.state = "open";
			};

			ws.onmessage = function (evt) {
			    alert(evt.data);
			};

			ws.onclose = function () {
				console.log("WebSocket died: ", me, arguments);

				me.ws = null;
				me.state = "closed";
			};
		};

		this.tick = function () {
			if (me.ws) {
				return;
			}

			if (me.retry_timeout > 0) {
				me.retry_timeout = me.retry_timeout - 1;
				return
			}

			me.reopen();
		};

		this.reopen();
	};

	var c = new connection_class(base_uri);
	factory.connections = [c];

    factory._ti = $window.setInterval(function () {
		_.each(factory.connections, function (e) {
			e.tick();
		});
	}, 1*1000);

	console.log("conn:", factory);
    return factory;
}]);

mod.factory('HeaderDatabase', ['SyncPool', function (SyncPool) {


}]);
