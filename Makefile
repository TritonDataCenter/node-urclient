#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2015 Joyent, Inc.
#

ROOT :=			$(PWD)
NODE_MOD_BIN =		$(ROOT)/node_modules/.bin

JSHINT =		$(NODE_MOD_BIN)/jshint
JSCS =			$(NODE_MOD_BIN)/jscs

CHECK_JS_FILES =	$(shell find *.js lib -name \*.js)

.PHONY: all
all: 0-npm-stamp

0-npm-stamp: package.json
	rm -rf $(PWD)/node_modules
	npm install
	touch $@

.PHONY: check
check: 0-npm-stamp
	$(JSHINT) $(CHECK_JS_FILES)
	$(JSCS) $(CHECK_JS_FILES)
	@echo "check ok"

clean:
	rm -f 0-npm-stamp
	rm -rf node_modules

