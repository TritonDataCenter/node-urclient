/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016 Joyent, Inc.
 */

var mod_util = require('util');
var mod_events = require('events');
var mod_assert = require('assert-plus');
var mod_verror = require('verror');

var VError = mod_verror.VError;

function
assert_positive_integer(val, name)
{
	mod_assert.number(val, name + ' must be a number');
	mod_assert.ok(!isNaN(val), name + ' must not be NaN');
	mod_assert.ok(val >= 1, name + ' must be at least 1');
}

function
RunQueue(options)
{
	var self = this;

	mod_events.EventEmitter.call(this);

	mod_assert.object(options, 'options');
	mod_assert.object(options.urclient, 'options.urclient');
	mod_assert.number(options.timeout, 'options.timeout');
	mod_assert.string(options.type, 'options.type');
	mod_assert.optionalNumber(options.concurrency,
	    'options.concurrency');

	/*
	 * Type-specific options:
	 */
	self.rq_action = null;
	if (options.type === 'send_file') {
		mod_assert.string(options.src_file, 'options.src_file');
		mod_assert.string(options.dst_dir, 'options.dst_dir');
		mod_assert.bool(options.clobber, 'options.clobber');

		self.rq_action = self._dispatch_send_file;
	} else if (options.type === 'recv_file') {
		mod_assert.string(options.src_file, 'options.src_file');
		mod_assert.string(options.dst_dir, 'options.dst_dir');

		self.rq_action = self._dispatch_recv_file;
	} else if (options.type === 'exec') {
		mod_assert.object(options.env, 'options.env');
		mod_assert.string(options.script, 'options.script');

		self.rq_action = self._dispatch_exec;
	} else {
		mod_assert.ok(false, 'unknown type: ' + options.type);
	}

	self.rq_concurrency = 0;
	if (options.concurrency !== undefined) {
		assert_positive_integer(options.concurrency,
		    'options.concurrency');
		self.rq_concurrency = options.concurrency;
	}

	self.rq_started = false;
	self.rq_closed = false;
	self.rq_finished = false;

	self.rq_dispatched_count = 0;
	self.rq_pending = [];
	self.rq_outstanding = [];

	self.rq_urclient = options.urclient;
	self.rq_script = options.script || null;
	self.rq_env = options.env || null;
	self.rq_src_file = options.src_file || null;
	self.rq_dst_dir = options.dst_dir || null;
	self.rq_clobber = options.clobber || false;
	self.rq_timeout = options.timeout;

	self.rq_epoch = process.hrtime();
	self.rq_reqid = 0;
}
mod_util.inherits(RunQueue, mod_events.EventEmitter);

RunQueue.prototype._time = function
_time()
{
	var self = this;

	var delta = process.hrtime(self.rq_epoch);

	return (delta[0] * 1000 + delta[1] / 1000000);
};

/*
 * Like close(), but clears the pending and outstanding queues first.
 * This essentially abandons all inflight requests.
 */
RunQueue.prototype.abort = function
abort()
{
	var self = this;

	var cancel_request = function (req) {
		var msg = 'aborted (before sent to server)';
		var was_sent = false;

		/*
		 * If the inflight for this request exists, it was sent
		 * to the remote server.
		 */
		if (req.rqr_inflight !== null) {
			if (!req.rqr_inflight.is_complete()) {
				req.rqr_inflight.complete();
			}
			msg = 'aborted';
			was_sent = true;
		}

		var err = new VError(msg);
		err.runqueue_code = 'ABORT';
		err.runqueue_abort_dispatched = was_sent;

		self.emit('failure', req.rqr_server, err);
	};

	while (self.rq_pending.length > 0) {
		cancel_request(self.rq_pending.shift());
	}
	while (self.rq_outstanding.length > 0) {
		cancel_request(self.rq_outstanding.shift());
	}
	self.rq_closed = true;

	setImmediate(self._dispatch.bind(self));
};

/*
 * Like close(), but clears the pending queue first.
 */
RunQueue.prototype.cancel = function
cancel()
{
	var self = this;

	self.rq_pending = [];
	self.rq_closed = true;

	setImmediate(self._dispatch.bind(self));
};

RunQueue.prototype.close = function
close()
{
	var self = this;

	if (self.rq_closed)
		return;
	self.rq_closed = true;

	setImmediate(self._dispatch.bind(self));
};

RunQueue.prototype.start = function
start()
{
	var self = this;

	if (self.rq_started)
		return;
	self.rq_started = true;

	setImmediate(self._dispatch.bind(self));
};

RunQueue.prototype.add_server = function
add_server(server)
{
	var self = this;

	mod_assert.ok(!self.rq_closed, 'add_server() after close()');

	/*
	 * We want a properly-formed server object:
	 */
	mod_assert.object(server, 'server');
	mod_assert.string(server.uuid, 'server.uuid');
	mod_assert.string(server.hostname, 'server.hostname');

	self.rq_pending.push({
		rqr_reqid: ++self.rq_reqid,
		rqr_server: {
			uuid: server.uuid,
			hostname: server.hostname
		},
		rqr_events: [
			{
				name: 'add_server',
				time: self._time()
			}
		],
		rqr_exit_status: null,
		rqr_stdout: null,
		rqr_stderr: null,
		rqr_inflight: null,
	});

	setImmediate(self._dispatch.bind(self));
};

RunQueue.prototype._prune = function
_prune(reqid)
{
	var self = this;

	for (var i = 0; i < self.rq_outstanding.length; i++) {
		if (self.rq_outstanding[i].rqr_reqid === reqid) {
			self.rq_outstanding.splice(i, 1);
			return;
		}
	}

	mod_assert.ok(false, 'could not prune request with id ' + reqid);
};

RunQueue.prototype._finish = function
_finish()
{
	var self = this;

	mod_assert.ok(!self.rq_finished);
	self.rq_finished = true;

	self.emit('end');
};

RunQueue.prototype.count_dispatched = function
count_dispatched()
{
	var self = this;

	return (self.rq_dispatched_count);
};

RunQueue.prototype.count_outstanding = function
count_outstanding()
{
	var self = this;

	return (self.rq_outstanding.length);
};

RunQueue.prototype._dispatch = function
_dispatch()
{
	var self = this;

	/*
	 * If we are finished already, there is no more work to do.
	 */
	if (self.rq_finished) {
		return;
	}

	/*
	 * If we have not been started, and have not yet been closed, then
	 * we have no work to do right now.
	 */
	if (!self.rq_started && !self.rq_closed) {
		return;
	}

	/*
	 * Ensure we do not exceed the requested concurrency limit.  A
	 * concurrency limit of 0 means "no limit".
	 */
	mod_assert.number(self.rq_concurrency);
	if (self.rq_concurrency > 0 &&
	    self.rq_outstanding.length >= self.rq_concurrency) {
		return;
	}

	/*
	 * Collect our first pending request:
	 */
	var req = self.rq_pending.shift();
	if (!req) {
		/*
		 * If there was no pending request, and close() has been
		 * called, and there are no outstanding requests remaining,
		 * then finish now.
		 */
		if (self.rq_closed && self.rq_outstanding.length === 0) {
			self._finish();
		}
		return;
	}

	/*
	 * Mark this request as being dispatched to the server:
	 */
	req.rqr_events.push({
		name: 'dispatch',
		time: self._time()
	});
	self.rq_outstanding.push(req);
	self.emit('dispatch', req.rqr_server);

	/*
	 * Dispatch it:
	 */
	self.rq_dispatched_count++;
	req.rqr_inflight = self.rq_action({
		script: self.rq_script,
		server_uuid: req.rqr_server.uuid,
		timeout: self.rq_timeout,
		env: self.rq_env,
		src_file: self.rq_src_file,
		dst_dir: self.rq_dst_dir,
		clobber: self.rq_clobber
	}, function (err, result) {
		req.rqr_events.push({
			name: 'exec_done',
			time: self._time()
		});
		self._prune(req.rqr_reqid);

		if (err) {
			self.emit('failure', req.rqr_server, err);
		} else {
			self.emit('success', req.rqr_server, result);
		}

		setImmediate(self._dispatch.bind(self));
	});

	setImmediate(self._dispatch.bind(self));
};

RunQueue.prototype._dispatch_exec = function
_dispatch_exec(options, callback)
{
	var self = this;

	return (self.rq_urclient.exec({
		/*
		 * Common:
		 */
		server_uuid: options.server_uuid,
		timeout: options.timeout,
		/*
		 * exec()-specific:
		 */
		script: options.script,
		env: options.env
	}, callback));
};

RunQueue.prototype._dispatch_send_file = function
_dispatch_send_file(options, callback)
{
	var self = this;

	return (self.rq_urclient.send_file({
		/*
		 * Common:
		 */
		server_uuid: options.server_uuid,
		timeout: options.timeout,
		/*
		 * send_file()-specific:
		 */
		src_file: options.src_file,
		dst_dir: options.dst_dir,
		clobber: options.clobber
	}, callback));
};

RunQueue.prototype._dispatch_recv_file = function
_dispatch_recv_file(options, callback)
{
	var self = this;

	return (self.rq_urclient.recv_file({
		/*
		 * Common:
		 */
		server_uuid: options.server_uuid,
		timeout: options.timeout,
		/*
		 * recv_file()-specific:
		 */
		src_file: options.src_file,
		dst_dir: options.dst_dir
	}, callback));
};

module.exports = {
	RunQueue: RunQueue
};

/* vim: ts=8 sts=8 sw=8 noet: */
