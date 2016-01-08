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
var mod_verror = require('verror');

var VError = mod_verror.VError;

function
clean(id)
{
	return (id ? id.toLowerCase().trim() : '');
}

function
URDiscovery(inflight, options)
{
	var self = this;
	mod_events.EventEmitter.call(self);

	self.urd_expected = null;
	self.urd_headnode = options.exclude_headnode ? false : true;
	self.urd_timeout_time = options.timeout;

	self.urd_inflight = inflight;
	self.urd_found = [];
	self.urd_done = false;

	if (options.node_list) {
		/*
		 * We have been asked to discover a fixed set of nodes.
		 * Discovery will terminate early once this set has been
		 * found.  If not all nodes are found by the discovery
		 * timeout, we will report an error to the callback()
		 * with the list of missing nodes.
		 */
		self.urd_expected = [];
		for (var i = 0; i < options.node_list.length; i++) {
			/*
			 * Clean up whitespace and case:
			 */
			var s = clean(options.node_list[i]);

			/*
			 * Don't allow empty strings:
			 */
			if (!s)
				continue;

			/*
			 * Don't allow duplicates:
			 */
			if (self.urd_expected.indexOf(s) !== -1)
				continue;

			self.urd_expected.push(s);
		}
		mod_assert.ok(self.urd_expected.length > 0);
	}

	self.urd_inflight.on('server', function (server, sysinfo) {
		self._on_server(server, sysinfo);
	});
	self.urd_inflight.once('timeout', function () {
		self._end_discover(false);
	});
	self.urd_inflight.once('amqp_error', function (err) {
		/*
		 * If there was a transport error, we can be sure of
		 * essentially nothing.  Abort discovery early.
		 */
		self._end_discover(false, err);
	});

	/*
	 * Set up the discovery timeout:
	 */
	self.urd_inflight.start_timeout(self.urd_timeout_time);
}
mod_util.inherits(URDiscovery, mod_events.EventEmitter);

URDiscovery.prototype.cancel = function
cancel()
{
	var self = this;

	/*
	 * The user wants us to stop discovery now, rather than
	 * at the timeout, and without generating any more events.
	 */
	self._end_discover(true);
};

URDiscovery.prototype._end_discover = function
_end_discover(silently, with_error)
{
	var self = this;

	if (self.urd_done)
		return;
	self.urd_done = true;

	/*
	 * Destroy our inflight request.
	 */
	self.urd_inflight.cancel_timeout();
	self.urd_inflight.complete();

	/*
	 * We have been asked to emit no further discovery events, so we are
	 * done.
	 */
	if (silently)
		return;

	if (with_error) {
		self.emit('error', new VError(with_error,
		    'discovery interrupted'));
		return;
	}

	if (self.urd_expected) {
		/*
		 * We expected a specific node list.
		 */
		if (self.urd_expected.length !== 0) {
			/*
			 * But we did not find all of the nodes!
			 */
			var err = new VError('could not find all nodes');
			err.nodes_found = self.urd_found;
			err.nodes_missing = self.urd_expected;

			self.emit('error', err, self.urd_expected);
			return;
		}
	}

	self.emit('end', self.urd_found);
};

URDiscovery.prototype._on_server = function
_on_server(server, sysinfo)
{
	var self = this;

	if (self.urd_done)
		return;

	/*
	 * Reset the discovery timeout.
	 */
	self.urd_inflight.start_timeout(self.urd_timeout_time);

	/*
	 * Skip headnodes if we have been asked to:
	 */
	if (server.headnode && !self.urd_headnode)
		return;

	if (!self.urd_expected) {
		/*
		 * We are just looking for every server we can find.  Don't
		 * re-add duplicates.
		 */
		for (var i = 0; i < self.urd_found.length; i++) {
			var urdf = self.urd_found[i];
			if (server.hostname === urdf.hostname ||
			    server.uuid === urdf.uuid) {
				return;
			}
		}
		self.emit('server', server, sysinfo);
		self.urd_found.push(server);
		return;
	}

	/*
	 * We are looking for a specific set of nodes.  The user may have
	 * specified both the UUID and the Hostname of a particular server,
	 * so attempt to remove _both_ from the expected list.
	 */
	var idx0 = self.urd_expected.indexOf(clean(server.uuid));
	if (idx0 !== -1)
		self.urd_expected.splice(idx0, 1);

	var idx1 = self.urd_expected.indexOf(clean(server.hostname));
	if (idx1 !== -1)
		self.urd_expected.splice(idx1, 1);

	if (idx0 !== -1 && idx1 !== -1) {
		/*
		 * The user has (potentially inadvertently) listed both a
		 * hostname and a UUID that refers to the same server.  We
		 * should emit a warning event, so that client tooling can
		 * optionally detect this condition.
		 */
		self.emit('duplicate', server.uuid, server.hostname);
	}

	if (idx0 !== -1 || idx1 !== -1) {
		self.emit('server', server, sysinfo);
		self.urd_found.push(server);
	}

	if (self.urd_expected.length < 1) {
		/*
		 * We have found every server we were expecting to find.
		 * Discovery is over.
		 */
		self._end_discover(false);
	}
};

module.exports = {
	URDiscovery: URDiscovery
};

/* vim: set ts=8 sts=8 sw=8 noet: */
