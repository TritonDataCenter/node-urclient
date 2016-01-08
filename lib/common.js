/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016 Joyent, Inc.
 */

var mod_extsprintf = require('extsprintf');

function
request_id()
{
	var id = (Math.random() * 0xffffffff) >>> 0;

	return (mod_extsprintf.sprintf('%08x', id));
}


module.exports = {
	request_id: request_id
};

/* vim: set ts=8 sts=8 sw=8 noet: */
