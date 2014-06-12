#!/usr/bin/env node
/* vim: set syntax=javascript ts=8 sts=8 sw=8 noet: */

var mod_restify = require('restify');

function
create_http_server(inflights, ip, log, callback)
{
	var server = mod_restify.createServer({
		name: 'urconn',
		log: log
	});

	function lookup_inflight(req, res, next) {
		var inflight_id = req.params.reqid;
		if (!inflight_id) {
			next(new Error('need request id'));
			return;
		}

		req.inflight = inflights.lookup(inflight_id);
		if (!req.inflight) {
			next(new Error('could not find request ' +
			    inflight_id));
			return;
		}

		/*
		 * Emit either a http_get or a http_put event, or fail
		 * the request if we have no registered listeners.
		 */
		var event = 'http_' + req.method.toLowerCase();
		if (req.inflight.listeners(event).length < 1) {
			next(new Error('invalid request'));
			return;
		}
		req.inflight.emit(event, req, res, next);
	}

	server.on('uncaughtException', function (req, res, route, err) {
		var __panic = {
			__restify_panic_error: err
		};
		log.error({
			err: __panic.__restify_panic_error
		}, 'httpserver restify panic');
		process.abort();
	});

	server.put('/file/:reqid', lookup_inflight);
	server.get('/file/:reqid', lookup_inflight);

	server.listen(0, ip, function () {
		log.debug({
			address: server.address()
		}, 'http server listening');
		callback(null, server);
	});
}

module.exports = {
	create_http_server: create_http_server
};
