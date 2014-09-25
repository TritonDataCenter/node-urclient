#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

.PHONY: all
all: 0-npm-stamp

0-npm-stamp: package.json
	rm -rf $(PWD)/node_modules
	npm install
	touch $@

.PHONY: check
check: 0-npm-stamp
	@node_modules/.bin/jshint index.js lib/*.js

