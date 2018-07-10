/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016 Joyent, Inc.
 */

var mod_events = require('events');
var mod_util = require('util');

var mod_assert = require('assert-plus');
var mod_amqp = require('amqp');
var mod_backoff = require('backoff');
var mod_jsprim = require('jsprim');
var mod_uuid = require('libuuid');
var mod_verror = require('verror');
var mod_os = require('os');

var lib_common = require('./common');

var SECONDS = 1000;

var HEARTBEAT_INTERVAL = 15; /* in seconds */

function
sysinfo_summary(reply)
{
	/*
	 * Determine if this is a headnode or not:
	 */
	var headnode = false;
	if (reply['Boot Parameters'] &&
	    reply['Boot Parameters'].headnode) {
		headnode = true;
	}

	/*
	 * Construct a summary record with useful server identity
	 * information:
	 */
	return ({
		uuid: reply['UUID'],
		hostname: reply['Hostname'],
		datacenter: reply['Datacenter Name'],
		setup: (reply['Setup'] === true || reply['Setup'] === 'true'),
		headnode: headnode
	});
}

function
URConnection(opts)
{
	var self = this;
	mod_events.EventEmitter.call(self);

	mod_assert.object(opts, 'opts');

	mod_assert.object(opts.log, 'opts.log');
	self.urc_log = opts.log;

	mod_assert.object(opts.inflights, 'opts.inflights');
	self.urc_inflights = opts.inflights;

	mod_assert.object(opts.amqp_config);
	mod_assert.string(opts.amqp_config.login, 'opts.amqp_config.login');
	mod_assert.string(opts.amqp_config.password,
	    'opts.amqp_config.password');
	mod_assert.string(opts.amqp_config.host, 'opts.amqp_config.host');
	mod_assert.number(opts.amqp_config.port, 'opts.amqp_config.port');
	self.urc_amqp_config = opts.amqp_config;

	mod_assert.number(opts.connect_timeout, 'opts.connect_timeout');
	self.urc_connect_timeout = opts.connect_timeout;

	mod_assert.bool(opts.reconnect, 'opts.reconnect');
	self.urc_reconnect = opts.reconnect;

	mod_assert.bool(opts.promiscuous, 'opts.promiscuous');
	self.urc_promiscuous = opts.promiscuous;

	mod_assert.string(opts.consumer_name, 'opts.consumer_name');
	self.urc_consumer_name = opts.consumer_name;

	self.urc_closed = false;
	self.urc_ready = false;
	self.urc_ready_count = 0;

	self.urc_history = [];
	self._history('create');

	self.urc_amqp = null;
	self.urc_exchange = null;
	self.urc_queue = null;
	self.urc_queue_name = null;

	/*
	 * Generate a random ID to use in the routing key for ping replies:
	 */
	self.urc_ping_id = lib_common.request_id();

	/*
	 * Reconnection, if configured, will be triggered by a backoff object:
	 */
	self.urc_backoff = mod_backoff.fibonacci({
		randomisationFactor: 0.25,
		initialDelay: 100,
		maxDelay: 10 * SECONDS
	});
	self.urc_backoff.on('ready', function (number, delay) {
		self.urc_log.debug({
			backoff: {
				number: number,
				delay: delay
			}
		}, 'backoff ready');
		self._connect();
	});
	self.urc_backoff.once('fail', function () {
		throw (new Error('backoff fail unexpected'));
	});

	/*
	 * Start connection process:
	 */
	self.urc_backoff.backoff();
}
mod_util.inherits(URConnection, mod_events.EventEmitter);

URConnection.prototype._history = function
_history(event)
{
	var self = this;

	/*
	 * Track the timestamps of the last 32 state transitions for
	 * debugging purposes.
	 */
	self.urc_history.push({
		event: event,
		timestamp: Date.now()
	});
	while (self.urc_history.length > 32) {
		self.urc_history.shift();
	}
};

URConnection.prototype._make_ready = function
_make_ready(ready)
{
	var self = this;

	if (self.urc_ready !== ready) {
		self.urc_ready = ready;
		if (ready) {
			self.urc_ready_count++;
		}
		var evt = ready ? 'ready' : 'unready';
		self._history(evt);
		self.emit(evt);
	}
};

URConnection.prototype._start_timeout = function
_start_timeout(amqp, message, time_ms)
{
	var self = this;

	mod_assert.number(time_ms);

	/*
	 * (Re-)establish connection or inactivity timeout:
	 */
	clearTimeout(self.urc_timeout);
	self.urc_timeout = setTimeout(function () {
		var err = new Error(message);
		amqp.emit('error', err);
	}, time_ms);
};

URConnection.prototype._cancel_timeout = function
_cancel_timeout()
{
	var self = this;

	/*
	 * Cancel connection timeout:
	 */
	clearTimeout(self.urc_timeout);
	self.urc_timeout = null;
};

URConnection.prototype._disconnect = function
_disconnect(err, close)
{
	var self = this;
	var emit_error = null;

	self._cancel_timeout();
	self._make_ready(false);

	if (!close) {
		/*
		 * If we are not closing this object, there must have been an
		 * error.
		 */
		mod_assert.ok(err);
		if (self.urc_reconnect) {
			self.urc_log.warn(err, 'amqp error (reconnecting)');
		} else {
			self.urc_log.error(err, 'amqp error');

			/*
			 * Attach the AMQP client object to the VError object
			 * before we let go, so that it is relatively easy to
			 * find in core files.
			 */
			emit_error = new mod_verror.VError(err, 'amqp error');
			emit_error.urce_amqp = self.urc_amqp;
			emit_error.urce_urconn = self;
		}

		/*
		 * Pass the AMQP error on to all inflight requests, so that
		 * they may optionally do something with it.  We defer this to
		 * the next tick, in case an EPIPE (or similar) is emitted
		 * synchronously before a listener can be attached.
		 */
		setImmediate(function () {
			self.urc_inflights.broadcast('amqp_error', err);
		});
	} else {
		setImmediate(function () {
			self.urc_inflights.broadcast('amqp_close');
		});
	}

	/*
	 * Clear all AMQP state to prepare for reconnection:
	 */
	try {
		self.urc_amqp.end();
		self.urc_amqp.destroy();
	} catch (ex) {
		self.urc_log.trace(ex, 'amqp destroy error');
	}
	self.urc_amqp = null;
	self.urc_queue = null;
	self.urc_queue_name = null;
	self.urc_exchange = null;

	if (emit_error) {
		self.emit('error', emit_error);
		return;
	}

	if (!close) {
		if (self.urc_reconnect) {
			self.urc_backoff.backoff();
		}
	}
};

URConnection.prototype._connect = function
_connect()
{
	var self = this;

	mod_assert.strictEqual(self.urc_amqp, null, 'concurrent _connect');
	mod_assert.strictEqual(self.urc_ready, false, 'cannot be ready yet');

	/*
	 * Construct configuration for the AMQP client.  We will reconnect to
	 * the broker ourselves, rather than require node-amqp to do so.  We
	 * will also manage our own connection timeout.  Force a protocol-level
	 * heartbeat interval of 15 seconds.
	 */
	var amqp_options = mod_jsprim.mergeObjects(self.urc_amqp_config, {
		/*
		 * Overrides:
		 */
		heartbeat: HEARTBEAT_INTERVAL,
		connectionTimeout: undefined
	});
	var amqp_impl_options = {
		defaultExchangeName: '',
		reconnect: false
	};

	/*
	 * Create the connection and start the connect timeout:
	 */
	self.urc_amqp = mod_amqp.createConnection(amqp_options,
	    amqp_impl_options);
	self._start_timeout(self.urc_amqp, 'connect timeout',
	    self.urc_connect_timeout);

	var abend = false;
	var give_up = function () {
		return (abend || self.urc_closed);
	};

	var fail = function (err) {
		if (give_up())
			return;

		abend = true;

		self._disconnect(err, false);
	};

	var reset_inact_timer = function () {
		if (give_up())
			return;

		self._start_timeout(self.urc_amqp,
		    'no heartbeat or data in 2.5 heartbeat intervals',
		    2.5 * HEARTBEAT_INTERVAL * SECONDS);

		self.urc_amqp._____inboundHeartbeatTimerReset();
	};

	self.urc_amqp.on('heartbeat', function () {
		if (give_up())
			return;

		self.emit('heartbeat');
	});
	self.urc_amqp.on('error', fail);
	self.urc_amqp.on('close', function () {
		/*
		 * If the library emits "close", it is almost certainly because
		 * RabbitMQ has gone away "gracefully"; e.g., as part of the
		 * program ending and closing all connections.
		 */
		fail(new Error('connection closed by remote peer'));
	});
	self.urc_amqp.once('ready', function () {
		if (give_up())
			return;

		self._cancel_timeout();

		self.urc_log.debug('amqp ready');

		/*
		 * Attempt to enable TCP keepalives on the connection to the
		 * broker, in order to avoid connections that hang open forever
		 * after the broker has gone away.
		 */
		var socket = self.urc_amqp.socket;
		if (socket && socket.setKeepAlive) {
			self.urc_log.debug('amqp enable TCP keepalives');
			socket.setKeepAlive(true, HEARTBEAT_INTERVAL *
			    SECONDS);
		} else {
			self.urc_log.debug('amqp could not enable TCP ' +
			    'keepalives');
		}

		/*
		 * node-amqp is capable of arranging for heartbeats from the
		 * server, but at present it is not capable of correctly
		 * detecting their absence.  Until node-amqp#416 is fixed,
		 * arrange to detect an inactive connection on our own:
		 */
		if (self.urc_amqp._inboundHeartbeatTimerReset) {
			self.urc_log.debug('amqp co-opting ' +
			    '_inboundHeartbeatTimerReset');
			self.urc_amqp._____inboundHeartbeatTimerReset =
			    self.urc_amqp._inboundHeartbeatTimerReset;
			self.urc_amqp._inboundHeartbeatTimerReset =
			    reset_inact_timer;
		}

		/*
		 * Connect to the topic exchange, through which we publish
		 * messages destined for the ur agent:
		 */
		mod_assert.strictEqual(self.urc_exchange, null);
		self.urc_exchange = self.urc_amqp.exchange('amq.topic', {
			type: 'topic'
		});

		/*
		 * Create a single queue, through which we will receive all
		 * relevant ur messages.  This queue is exclusive, so it will
		 * be deleted when we disconnect.  If reconnection is enabled,
		 * a new queue will be created (with the appropriate bindings)
		 * through the new connection.
		 */
		mod_assert.strictEqual(self.urc_queue_name, null);
		self.urc_queue_name = [
			'ur',
			self.urc_consumer_name,
			mod_uuid.create()
		].join('.');

		self.urc_log.debug('amqp create queue %s', self.urc_queue_name);

		mod_assert.strictEqual(self.urc_queue, null);
		self.urc_queue = self.urc_amqp.queue(self.urc_queue_name, {
			exclusive: true
		});
		self.urc_queue.on('error', function (err) {
			fail(new mod_verror.VError(err, 'amqp queue error'));
		});
		self.urc_queue.on('open', function () {
			if (give_up())
				return;

			self.urc_log.debug('amqp queue open');

			var queue_bindings;
			if (self.urc_promiscuous) {
				/*
				 * If a promiscuous binding has been requested,
				 * the consumer wants to see all ur messages.
				 */
				queue_bindings = [
					'ur.#'
				];
			} else {
				/*
				 * Bind to the list of static bindings we
				 * need, as well as the key to which our ping
				 * replies will be sent.
				 */
				queue_bindings = [
					'ur.execute-reply.*.*',
					'ur.startup.#',
					'ur.sysinfo.#',
					'ur.ack' + self.urc_ping_id + '.*'
				];
			}
			queue_bindings.forEach(function (qb) {
				self.urc_log.debug('amqp bind queue to ' + qb);
				self.urc_queue.bind('amq.topic', qb);
			});

			self.urc_queue.subscribe(function (msg, hdrs, dinfo) {
				/*
				 * node-amqp appears to catch exceptions thrown
				 * from the callback functions registered by
				 * consumers.  This is deeply unfortunate, as
				 * it prevents consumers from correctly using
				 * thrown exceptions to represent programmer
				 * errors which should abort the process.
				 *
				 * Schedule the firing of the message delivery
				 * callback on a new stack without a try-catch:
				 */
				setImmediate(function () {
					if (give_up())
						return;

					self._on_reply(msg, hdrs, dinfo);
				});
			});

			/*
			 * We have successfully set up our AMQP connection;
			 * reset the reconnection delay to the minimum value.
			 */
			self.urc_backoff.reset();

			/*
			 * Signal readiness to our consumer:
			 */
			self.urc_log.debug('amqp ready complete');
			mod_assert.strictEqual(self.urc_ready, false);
			self._make_ready(true);
		});
	});
};

URConnection.prototype.close = function
close()
{
	var self = this;

	if (self.urc_closed)
		return;
	self.urc_closed = true;

	self._history('close');
	self._disconnect(null, true);
};

URConnection.prototype.ready = function
ready()
{
	var self = this;

	return (self.urc_ready && !self.urc_closed);
};

URConnection.prototype._on_reply = function
_on_reply(msg, hdrs, dinfo)
{
	var self = this;
	var key = dinfo.routingKey.split('.');

	self.urc_log.trace({
		deliveryInfo: dinfo,
		headers: hdrs,
		message: msg
	}, '_on_reply()');

	var unknown = function () {
		self.urc_log.trace({
			deliveryInfo: dinfo,
			headers: hdrs,
			message: msg
		}, 'unknown delivery info in reply');
	};

	if (key[0] !== 'ur') {
		unknown();
		return;
	}

	self.emit('snoop', msg, hdrs, dinfo);

	switch (key[1]) {
	case 'execute-reply':
		var infl = self.urc_inflights.lookup(key[3]);
		if (infl) {
			infl.emit('command_reply', msg);
			return;
		}
		break;

	case 'startup':
	case 'sysinfo':
		/*
		 * Unsolicited broadcast message from a server that has just
		 * started up, or that has detected a change in its sysinfo
		 * data.
		 */
		self.emit('broadcast', key[1], sysinfo_summary(msg), msg);
		break;

	case ('ack' + self.urc_ping_id):
		var ping = self.urc_inflights.lookup(msg.req_id);
		if (ping && ping.data().server_uuid === key[2]) {
			ping.emit('ping_reply', msg);
			return;
		}
		break;
	}

	unknown();
};

URConnection.prototype.send_sysinfo_broadcast = function
send_sysinfo_broadcast()
{
	var self = this;

	if (!self.ready()) {
		return (null);
	}

	var infl = self.urc_inflights.register({
		type: 'sysinfo',
		toString: function () {
			return ('ur sysinfo broadcast');
		}
	});

	infl.on('command_reply', function (reply) {
		var server_info = sysinfo_summary(reply);

		self.urc_log.debug({
			request_id: infl.id(),
			server_info: server_info
		}, 'sysinfo response');
		infl.emit('server', server_info, reply);
	});

	self.urc_log.debug({
		request_id: infl.id()
	}, 'send sysinfo broadcast');

	self.urc_exchange.publish('ur.broadcast.sysinfo.' + infl.id(), {});

	return (infl);
};

URConnection.prototype.send_ping = function
send_ping(server_uuid)
{
	var self = this;

	if (!self.ready()) {
		return (null);
	}

	var infl = self.urc_inflights.register({
		type: 'ping',
		server_uuid: server_uuid,
		toString: function () {
			return ('ur ping <Server ' + server_uuid + '>');
		}
	});

	self.urc_exchange.publish('ur.ping.' + server_uuid, {
		client_id: self.urc_ping_id,
		id: infl.id()
	});

	return (infl);
};

var networkIps;

URConnection.prototype.send_command = function
send_command(server_id, script, args, env, data)
{
	var self = this;

	if (!self.ready()) {
		return (null);
	}

	if (networkIps === undefined) {
		var netIfs = mod_os.networkInterfaces();
		networkIps = [];
		mod_jsprim.forEachKey(netIfs, function (ifName, ifAddrs) {
			ifAddrs.forEach(function (ifAddr) {
				if (!ifAddr.internal) {
					networkIps.push(ifAddr.address);
				}
			});
		});
	}
	var origin = {
		user: process.env.LOGNAME,
		smf_fmri: process.env.SMF_FMRI,
		hostname: mod_os.hostname(),
		ips: networkIps
	};

	var infl = self.urc_inflights.register(data);

	self.urc_exchange.publish('ur.execute.' + server_id + '.' +
	    infl.id(), {
		type: 'script',
		script: script,
		args: args.map(function (x) {
			return (x.replace(/%%ID%%/g, infl.id()));
		}),
		env: env,
		origin: origin
	});

	return (infl);
};

module.exports = {
	URConnection: URConnection
};

/* vim: set ts=8 sts=8 sw=8 noet: */
