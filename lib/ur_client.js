/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016 Joyent, Inc.
 */

var mod_path = require('path');
var mod_fs = require('fs');
var mod_events = require('events');
var mod_util = require('util');

var mod_assert = require('assert-plus');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var lib_inflight = require('./inflight');
var lib_mq = require('./mq');
var lib_httpserver = require('./httpserver');
var lib_ur_disco = require('./ur_disco');

var VError = mod_verror.VError;

var SECONDS = 1000;
var PROXIED_EVENTS = [
	'snoop',
	'broadcast',
	'heartbeat',
	'error'
];

var VALID_CONSUMER_NAME = /^[a-zA-Z0-9_]+$/;

function
URClient(options)
{
	var self = this;
	mod_events.EventEmitter.call(self);

	mod_assert.object(options, 'options');
	mod_assert.object(options.log, 'options.log');
	mod_assert.number(options.connect_timeout, 'options.connect_timeout');
	mod_assert.bool(options.enable_http, 'options.enable_http');
	mod_assert.object(options.amqp_config, 'options.amqp_config');
	mod_assert.optionalBool(options.reconnect, 'options.reconnect');
	mod_assert.optionalBool(options.promiscuous, 'options.promiscuous');
	mod_assert.optionalString(options.consumer_name,
	    'options.consumer_name');

	self.ur_log = options.log;
	self.ur_amqp_config = options.amqp_config;

	/*
	 * In the past, our listen queue was named "ur.oneachnode.$UUID", where
	 * the UUID was randomly generated for each connection.  As more
	 * software makes use of this library, it may be advantageous for the
	 * queue to be named for the consuming software.
	 */
	var consumer_name = 'oneachnode';
	if (options.consumer_name) {
		mod_assert.ok(VALID_CONSUMER_NAME.test(options.consumer_name));

		consumer_name = options.consumer_name;
	}

	/*
	 * Track whether each subsystem is in a ready state or not.  We wish to
	 * emit a single "ready" event for each transition from unready to
	 * ready, and a single "unready" event if our readiness is subsequently
	 * interrupted.
	 */
	self.ur_startup = true;
	self.ur_unready = [];

	var make_ready = function (name) {
		var idx = self.ur_unready.indexOf(name);
		mod_assert.notStrictEqual(idx, -1);
		self.ur_unready.splice(idx, 1);
		if (self.ur_unready.length === 0) {
			if (self.ur_startup) {
				/*
				 * Startup is now complete.
				 */
				self.ur_startup = false;
			}
			self.emit('ready');
		}
	};

	var make_unready = function (name) {
		if (!self.ur_startup && self.ur_unready.length === 0) {
			self.emit('unready');
		}
		if (self.ur_unready.indexOf(name) === -1) {
			self.ur_unready.push(name);
		}
	};

	/*
	 * We are not initialised until all of our subcomponents are, also.
	 */
	if (options.enable_http) {
		make_unready('httpserver');
	}
	make_unready('URConnection');

	/*
	 * Initialise the inflight request tracker:
	 */
	self.ur_inflights = new lib_inflight.InflightRegister();

	/*
	 * Connect to the AMQP Broker:
	 */
	self.ur_urconn = new lib_mq.URConnection({
		log: options.log.child({
			component: 'URConnection'
		}),
		inflights: self.ur_inflights,
		amqp_config: self.ur_amqp_config,
		connect_timeout: options.connect_timeout,
		reconnect: options.reconnect ? true : false,
		promiscuous: options.promiscuous ? true : false,
		consumer_name: consumer_name
	});
	self.ur_urconn.on('ready', function () {
		make_ready('URConnection');
	});
	self.ur_urconn.on('unready', function () {
		make_unready('URConnection');
	});

	/*
	 * Some events are passed directly from the underlying connection to
	 * our consumer:
	 */
	PROXIED_EVENTS.forEach(function (fwd) {
		self.ur_urconn.on(fwd, self.emit.bind(self, fwd));
	});

	/*
	 * If requested, enable the HTTP Server:
	 */
	self.ur_http = null;
	if (options.enable_http) {
		mod_assert.string(options.bind_ip, 'options.bind_ip');
		var http_log = options.log.child({
			component: 'httpserver'
		});
		lib_httpserver.create_http_server(self.ur_inflights,
		    options.bind_ip, http_log, function (err, server) {
			if (err) {
				err = new VError(err, 'could not init http');
				self.emit('error', err);
				return;
			}
			self.ur_http = server;
			make_ready('httpserver');
		});
	}
}
mod_util.inherits(URClient, mod_events.EventEmitter);

URClient.prototype._assert_http_enabled = function
_assert_http_enabled()
{
	var self = this;

	mod_assert.ok(self.ur_http, 'HTTP was not enabled; cannot send files');
};

URClient.prototype._assert_ready = function
_assert_ready()
{
	var self = this;

	mod_assert.ok(self.ready(), 'URConnection not ready');
};

/*
 * Clean up this connection.
 */
URClient.prototype.close = function
close()
{
	var self = this;

	self.ur_urconn.close();
};

/*
 * Check if the connection is ready to make new requests.
 */
URClient.prototype.ready = function
ready()
{
	var self = this;

	return (self.ur_unready.length === 0 && self.ur_urconn.ready());
};

URClient.prototype.ping = function
ping(options, callback)
{
	var self = this;
	var timeout = 10 * SECONDS;

	self._assert_ready();

	mod_assert.object(options, 'options');
	mod_assert.uuid(options.server_uuid, 'options.server_uuid');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	if (options.timeout) {
		mod_assert.ok(!isNaN(options.timeout) && options.timeout > 0);
		timeout = options.timeout;
	}

	var infl = self.ur_urconn.send_ping(options.server_uuid);

	infl.once('timeout', function () {
		infl.complete();
		callback(new Error('ping timeout'));
	});
	infl.once('ping_reply', function (msg) {
		infl.complete();
		callback(null, msg);
	});
	infl.once('amqp_error', function (err) {
		infl.complete();
		callback(new VError(err, 'ping interrupted'));
	});

	infl.start_timeout(options.timeout);
};

URClient.prototype.discover = function
discover(options)
{
	var self = this;

	self._assert_ready();

	mod_assert.optionalArrayOfString(options.node_list,
	    'options.node_list');
	mod_assert.optionalBool(options.exclude_headnode,
	    'options.exclude_headnode');
	mod_assert.number(options.timeout, 'options.timeout');

	/*
	 * Trigger sysinfo broadcast via AMQP:
	 */
	var infl = self.ur_urconn.send_sysinfo_broadcast();

	return (new lib_ur_disco.URDiscovery(infl, options));
};

URClient.prototype.send_file = function
send_file(options, callback)
{
	var self = this;

	self._assert_ready();
	self._assert_http_enabled();

	mod_assert.object(options, 'options');
	mod_assert.func(callback, 'callback');
	mod_assert.string(options.server_uuid, 'options.server_uuid');
	mod_assert.number(options.timeout, 'options.timeout');
	mod_assert.string(options.src_file, 'options.src_file');
	mod_assert.string(options.dst_dir, 'options.dst_dir');
	mod_assert.optionalBool(options.clobber, 'options.clobber');

	var path = mod_path.join(options.dst_dir,
	    mod_path.basename(options.src_file));
	var addr = self.ur_http.address();
	var url = 'http://' + addr.address + ':' + addr.port + '/file/%%ID%%';

	var script = [
		'#!/bin/bash -x',
		'',
		/*
		 * If we are not clobbering an existing file, and one
		 * exists, exit now:
		 */
		'if [[ $3 != "clobber" && -f "$2" ]]; then',
		'    exit 50',
		'fi',
		'',
		/*
		 * Check for the existence of the target directory:
		 */
		'if [[ ! -d "$(dirname "$2")" ]]; then',
		'    exit 55',
		'fi',
		'',
		/*
		 * Download to a temporary file:
		 */
		'tmpnam="$(dirname "$2")/.oneachnode.$$.$(basename "$2")"',
		'rm -f "$tmpnam"',
		'',
		'if ! /usr/bin/curl -gsSf "$1" -o "$tmpnam"; then',
		'    rm -f "$tmpnam"',
		'    exit 60',
		'fi',
		'',
		/*
		 * Move the temporary file into place if we succeed:
		 */
		'if ! mv "$tmpnam" "$2"; then',
		'    rm -f "$tmpnam"',
		'    exit 60',
		'fi',
		'',
		'exit 0'
	].join('\n');
	var args = [
		url,
		path,
		options.clobber ? 'clobber' : ''
	];

	var fin_called = false;
	var fin = function (err) {
		if (fin_called)
			return;
		fin_called = true;

		if (!infl.is_complete())
			infl.complete();

		callback(err);
	};

	var b = options._barrier = mod_vasync.barrier();
	b.on('drain', fin);
	b.start('exec');
	b.start('http_get');

	var infl = self.ur_urconn.send_command(options.server_uuid,
	    script, args, {}, options);
	infl.start_timeout(options.timeout);
	infl.once('timeout', function () {
		var err = new VError('timed out');
		err.server_uuid = options.server_uuid;
		fin(err);
	});
	infl.once('command_reply', function (reply) {
		if (reply.exit_status === 0) {
			b.done('exec');
			return;
		}

		/*
		 * Handle the various error conditions:
		 */
		var err;
		if (reply.exit_status === 50) {
			err = new VError('file exists already');
			err.code = 'EEXIST';
		} else if (reply.exit_status === 55) {
			err = new VError('target is not a directory');
			err.code = 'ENOTDIR';
		} else {
			err = new VError('curl failed to receive file');
			err.stderr = reply.stderr.trim();
		}
		err.server_uuid = options.server_uuid;
		fin(err);
	});
	infl.once('http_get', function (req, res, next) {
		/*
		 * When curl has connected to us, cancel the execution timeout,
		 * which was set by exec().  From now on we will use the
		 * socket inactivity timeout on this HTTP connection.
		 */
		infl.cancel_timeout();
		var to_hdlr = function () {
			var err = new VError('timed out');
			err.server_uuid = options.server_uuid;
			fin(err);
		};
		req.connection.setTimeout(options.timeout);
		req.connection.on('timeout', to_hdlr);

		/*
		 * Handle HTTP errors:
		 */
		req.on('error', function (err) {
			fin(new VError(err, 'request error for host "%s"',
			    options.server_uuid));
		});
		res.on('error', function (err) {
			fin(new VError(err, 'response error for host "%s"',
			    options.server_uuid));
		});

		/*
		 * Send our nominated file to the remote host.
		 */
		var input = mod_fs.createReadStream(options.src_file);
		input.on('error', function (err) {
			res.send(500);
			next(false);
			fin(new VError(err, 'could not read input file "%s"',
			    options.src_file));
		});
		input.pipe(res);

		/*
		 * The request has finished, so the socket timeout no longer
		 * applies.  Activate a final timeout to ensure we get a
		 * response from curl(1):
		 */
		res.on('finish', function () {
			b.done('http_get');
			req.connection.removeListener('timeout', to_hdlr);
			infl.start_timeout(options.timeout);
			next();
		});
	});

	return (infl);
};

URClient.prototype.recv_file = function
recv_file(options, callback)
{
	var self = this;

	self._assert_ready();
	self._assert_http_enabled();

	mod_assert.object(options, 'options');
	mod_assert.func(callback, 'callback');
	mod_assert.string(options.server_uuid, 'options.server_uuid');
	mod_assert.number(options.timeout, 'options.timeout');
	mod_assert.string(options.src_file, 'options.src_file');
	mod_assert.string(options.dst_dir, 'options.dst_dir');

	var path = mod_path.join(options.dst_dir, options.server_uuid);
	var addr = self.ur_http.address();
	var url = 'http://' + addr.address + ':' + addr.port + '/file/%%ID%%';

	var script = [
		'#!/bin/bash -x',
		'',
		/*
		 * Check if our file exists, and if not, fail:
		 */
		'if [[ ! -f "$2" ]]; then',
		'    exit 40',
		'fi',
		'',
		/*
		 * Upload it with curl:
		 */
		'if ! /usr/bin/curl -X PUT -gsSf -T "$2" "$1"; then',
		'    exit 60',
		'fi',
		'',
		'exit 0'
	].join('\n');
	var args = [
		url,
		options.src_file
	];

	var fin_called = false;
	var fin = function (err) {
		if (fin_called)
			return;
		fin_called = true;

		if (!infl.is_complete())
			infl.complete();

		callback(err);
	};

	var b = options._barrier = mod_vasync.barrier();
	b.on('drain', fin);
	b.start('exec');
	b.start('http_put');

	var infl = self.ur_urconn.send_command(options.server_uuid,
	    script, args, {}, options);
	infl.start_timeout(options.timeout);
	infl.once('timeout', function () {
		var err = new VError('timed out');
		err.server_uuid = options.server_uuid;
		fin(err);
	});
	infl.once('command_reply', function (reply) {
		if (reply.exit_status === 0) {
			b.done('exec');
			return;
		}

		/*
		 * Handle the various error conditions:
		 */
		var err;
		if (reply.exit_status === 40) {
			err = new VError('file does not exist');
			err.code = 'ENOENT';
		} else {
			err = new VError('curl failed to send file');
			err.stderr = reply.stderr.trim();
		}
		err.server_uuid = options.server_uuid;
		fin(err);
	});
	infl.once('http_put', function (req, res, next) {
		/*
		 * When curl has connected to us, cancel the execution timeout,
		 * which was set by exec().  From now on we will use the
		 * socket inactivity timeout on this HTTP connection.
		 */
		infl.cancel_timeout();
		var to_hdlr = function () {
			var err = new VError('timed out');
			err.server_uuid = options.server_uuid;
			fin(err);
		};
		req.connection.setTimeout(options.timeout);
		req.connection.on('timeout', to_hdlr);

		/*
		 * Handle HTTP errors:
		 */
		req.on('error', function (err) {
			fin(new VError(err, 'request error for host "%s"',
			    options.server_uuid));
		});
		res.on('error', function (err) {
			fin(new VError(err, 'response error for host "%s"',
			    options.server_uuid));
		});

		/*
		 * Send our nominated file to the remote host.
		 */
		var output = mod_fs.createWriteStream(path);
		output.on('error', function (err) {
			res.send(500);
			next(false);
			fin(new VError(err, 'could not read input file "%s"',
			    options.src_file));
		});
		req.pipe(output);

		/*
		 * The request has finished, so the socket timeout no longer
		 * applies.  Activate a final timeout to ensure we get a
		 * response from curl(1):
		 */
		output.on('close', function () {
			/*
			 * XXX This should probably be a 'finish' event,
			 * but it doesn't seem to exist in node 0.8.  Fix
			 * when we get to 0.10.
			 */
			b.done('http_put');
			req.connection.removeListener('timeout', to_hdlr);
			infl.start_timeout(options.timeout);
			res.send(200);
			next();
		});
	});

	return (infl);
};

URClient.prototype.exec = function
exec(options, callback)
{
	var self = this;

	self._assert_ready();

	mod_assert.object(options, 'options');
	mod_assert.func(callback, 'callback');
	mod_assert.string(options.script, 'options.script');
	mod_assert.string(options.server_uuid, 'options.server_uuid');
	mod_assert.number(options.timeout, 'options.timeout');
	mod_assert.optionalObject(options.env, 'options.env');
	mod_assert.optionalArrayOfString(options.args, 'options.args');

	var infl = self.ur_urconn.send_command(options.server_uuid,
	    options.script, options.args || [], options.env || {}, options);
	infl.start_timeout(options.timeout);
	infl.on('command_reply', function (reply) {
		infl.complete();
		callback(null, reply);
	});
	infl.on('timeout', function () {
		infl.complete();
		callback(new VError('timeout for host %s',
		    options.server_uuid));
	});

	return (infl);
};


module.exports = {
	URClient: URClient
};

/* vim: set ts=8 sts=8 sw=8 noet: */
