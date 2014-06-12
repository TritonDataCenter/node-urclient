/* vim: set syntax=javascript ts=8 sts=8 sw=8 noet: */

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

	/*
	 * If requested, enable the HTTP Server:
	 */
	self.ur_http = null;
	if (options.enable_http) {
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
discover(options, callback)
{
	var self = this;

	self._assert_ready();

	mod_assert.optionalArrayOfString(options.node_list,
	    'options.node_list');
	mod_assert.optionalBool(options.exclude_headnode,
	    'options.exclude_headnode');
	mod_assert.number(options.timeout, 'options.timeout');
	mod_assert.func(callback, 'callback');

	var gen = ++self.ur_discovery_gen;

	mod_assert.ok(!self.ur_discovery, 'concurrent discover()');
	self.ur_discovery = {
		urd_gen: gen,
		urd_expected: null,
		urd_headnode: true,
		urd_found: [],
		urd_timeout: null,
		urd_callback: callback,
		urd_timeout_time: options.timeout
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

	self._reset_discovery_timeout(gen);
	/*
	 * Set up the discovery timeout:
	 */

	/*
	 * Trigger sysinfo broadcast via AMQP:
	 */
	self.ur_conn.send_sysinfo_broadcast();
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
		 * We are just looking for every server we can find.
		 */
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
		urd.urd_expected.splice(idx0, 1);

	if (idx0 !== -1 || idx1 !== -1)
		urd.urd_found.push(server);
};

URClient.prototype._end_discover = function
_end_discover(gen)
{
	var self = this;

	mod_assert.object(self.ur_discovery);
	mod_assert.strictEqual(self.ur_discovery.urd_gen, gen,
	    'wrong generation');

	var urd = self.ur_discovery;
	self.ur_discovery = null;

	if (urd.urd_timeout)
		clearTimeout(urd.urd_timeout);

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

			urd.urd_callback(err);
			return;
		}
	}

	urd.urd_callback(null, urd.urd_found);
};

URClient.prototype.send_file = function
send_file(options, callback)
{
	this._assert_ready();
	this._assert_http_enabled();
};

URClient.prototype.recv_file = function
recv_file(options, callback)
{
	this._assert_ready();
	this._assert_http_enabled();
};

URClient.prototype.exec = function
exec(options, callback)
{
	this._assert_ready();
};


module.exports = {
	URClient: URClient
};
