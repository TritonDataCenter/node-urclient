/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
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

var VError = mod_verror.VError;

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

	self.ur_log = options.log;
	self.ur_amqp_config = options.amqp_config;

	self.ur_discovery_gen = 0;
	self.ur_discovery = null;

	self.ur_ready = false;

	/*
	 * We are not initialised until all of our subcomponents are, also.
	 */
	self.ur_barrier = mod_vasync.barrier();
	self.ur_barrier.once('drain', function () {
		self.ur_ready = true;
		self.emit('ready');
	});

	/*
	 * Initialise the inflight request tracker:
	 */
	self.ur_inflights = new lib_inflight.InflightRegister();

	/*
	 * Connect to the AMQP Broker:
	 */
	self.ur_barrier.start('URConnection');
	self.ur_urconn = new lib_mq.URConnection(options.log.child({
		component: 'URConnection'
	}), self.ur_inflights, self.ur_amqp_config, options.connect_timeout);
	self.ur_urconn.on('ready', function () {
		self.ur_barrier.done('URConnection');
	});
	self.ur_urconn.on('server', self._on_server.bind(self));

	/*
	 * If requested, enable the HTTP Server:
	 */
	self.ur_http = null;
	if (options.enable_http) {
		mod_assert.string(options.bind_ip, 'options.bind_ip');
		self.ur_barrier.start('httpserver');
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
			self.ur_barrier.done('httpserver');
		});
	}
}
mod_util.inherits(URClient, mod_events.EventEmitter);

URClient.prototype._assert_http_enabled = function
_assert_http_enabled()
{
	mod_assert.ok(this.ur_http, 'HTTP was not enabled; cannot send files');
};

URClient.prototype._assert_ready = function
_assert_ready()
{
	mod_assert.ok(this.ur_ready, 'URConnection not yet ready');
};

/*
 * Clean up this connection.
 */
URClient.prototype.close = function
close()
{
	var self = this;

	if (self.ur_urconn) {
		try {
			self.ur_urconn.destroy();
		} catch (ex) {
		}
		self.ur_urconn = null;
	}
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

	var gen = ++self.ur_discovery_gen;

	mod_assert.ok(!self.ur_discovery, 'concurrent discover()');
	self.ur_discovery = {
		urd_gen: gen,
		urd_expected: null,
		urd_headnode: true,
		urd_found: [],
		urd_timeout: null,
		urd_timeout_time: options.timeout,
		urd_ee: new mod_events.EventEmitter()
	};
	self.ur_discovery.urd_ee.cancel = function () {
		/*
		 * The user wants us to stop discovery now, rather than
		 * at the timeout, and without generating any more events.
		 */
		self._end_discover(gen, true);
	};

	if (options.node_list) {
		/*
		 * We have been asked to discover a fixed set of nodes.
		 * Discovery will terminate early once this set has been
		 * found.  If not all nodes are found by the discovery
		 * timeout, we will report an error to the callback()
		 * with the list of missing nodes.
		 */
		self.ur_discovery.urd_expected = [];
		for (var i = 0; i < options.node_list.length; i++) {
			/*
			 * Clean up whitespace and case:
			 */
			var s = (options.node_list[i] || '').
			    trim().toLowerCase();
			/*
			 * Don't allow empty strings:
			 */
			if (!s)
				continue;
			/*
			 * Don't allow duplicates:
			 */
			if (self.ur_discovery.urd_expected.indexOf(s) !== -1)
				continue;
			self.ur_discovery.urd_expected.push(s);
		}
		mod_assert.ok(self.ur_discovery.urd_expected.length > 0);
	}

	if (options.exclude_headnode)
		self.ur_discovery.urd_headnode = false;

	/*
	 * Set up the discovery timeout:
	 */
	self._reset_discovery_timeout(gen);

	/*
	 * Trigger sysinfo broadcast via AMQP:
	 */
	self.ur_urconn.send_sysinfo_broadcast();

	return (self.ur_discovery.urd_ee);
};

URClient.prototype._reset_discovery_timeout = function
_reset_discovery_timeout(gen)
{
	var self = this;
	var urd = self.ur_discovery;

	if (urd.urd_timeout)
		clearTimeout(urd.urd_timeout);

	urd.urd_timeout = setTimeout(function () {
		self._end_discover(gen);
	}, urd.urd_timeout_time);
};

URClient.prototype._on_server = function
_on_server(server)
{
	var self = this;
	var urd = self.ur_discovery;

	/*
	 * If we are not discovering, we don't care about sysinfo messages.
	 */
	if (!urd)
		return;

	/*
	 * Reset the discovery timeout.
	 */
	self._reset_discovery_timeout(urd.urd_gen);

	/*
	 * Skip headnodes if we have been asked to:
	 */
	if (server.headnode && !urd.urd_headnode)
		return;

	if (!urd.urd_expected) {
		/*
		 * We are just looking for every server we can find.  Don't
		 * re-add duplicates.
		 */
		for (var i = 0; i < urd.urd_found.length; i++) {
			var urdf = urd.urd_found[i];
			if (server.hostname === urdf.hostname ||
			    server.uuid === urdf.uuid) {
				return;
			}
		}
		urd.urd_ee.emit('server', server);
		urd.urd_found.push(server);
		return;
	}

	/*
	 * We are looking for a specific set of nodes.  The user may have
	 * specified both the UUID and the Hostname of a particular server,
	 * so attempt to remove _both_ from the expected list.
	 */
	var idx0 = urd.urd_expected.indexOf(server.uuid.toLowerCase().trim());
	if (idx0 !== -1)
		urd.urd_expected.splice(idx0, 1);

	var idx1 = urd.urd_expected.indexOf(server.hostname.toLowerCase().
	    trim());
	if (idx1 !== -1)
		urd.urd_expected.splice(idx1, 1);

	if (idx0 !== -1 && idx1 !== -1) {
		/*
		 * The user has (potentially inadvertently) listed both a
		 * hostname and a UUID that refers to the same server.  We
		 * should emit a warning event, so that client tooling can
		 * optionally detect this condition.
		 */
		urd.urd_ee.emit('duplicate', server.uuid, server.hostname);
	}

	if (idx0 !== -1 || idx1 !== -1) {
		urd.urd_ee.emit('server', server);
		urd.urd_found.push(server);
	}

	if (urd.urd_expected.length < 1)
		self._end_discover(urd.urd_gen);
};

URClient.prototype._end_discover = function
_end_discover(gen, silently)
{
	var self = this;

	mod_assert.object(self.ur_discovery);
	mod_assert.strictEqual(self.ur_discovery.urd_gen, gen,
	    'wrong generation');

	var urd = self.ur_discovery;
	self.ur_discovery = null;

	if (urd.urd_timeout)
		clearTimeout(urd.urd_timeout);

	/*
	 * We have been asked to emit no further discovery events, so
	 * we are done.
	 */
	if (silently)
		return;

	if (urd.urd_expected) {
		/*
		 * We expected a specific node list.
		 */
		if (urd.urd_expected.length !== 0) {
			/*
			 * But we did not find all of the nodes!
			 */
			var err = new VError('could not find all nodes');
			err.nodes_found = urd.urd_found;
			err.nodes_missing = urd.urd_expected;

			urd.urd_ee.emit('error', err, urd.urd_expected);
			return;
		}
	}

	urd.urd_ee.emit('end', urd.urd_found);
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
	    options.script, options.args || [], options.env, options);
	infl.start_timeout(options.timeout);
	infl.on('command_reply', function (reply) {
		infl.cancel_timeout();
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
