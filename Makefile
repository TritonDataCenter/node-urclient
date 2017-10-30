#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

ROOT :=			$(PWD)
NODE_MOD_BIN =		$(ROOT)/node_modules/.bin
TAP = 			$(NODE_MOD_BIN)/tape

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

.PHONY: test
test:
	$(TAP) test/*.js

.PHONY: prepush
prepush: check test

clean:
	rm -f 0-npm-stamp
	rm -rf node_modules

.PHONY: cutarelease
cutarelease:
	[[ -z `git status --short` ]]  # If this fails, the working dir is dirty.
	@which json 2>/dev/null 1>/dev/null && \
	    ver=$(shell json -f package.json version) && \
	    name=$(shell json -f package.json name) && \
	    publishedVer=$(shell npm view -j $(shell json -f package.json name)@$(shell json -f package.json version) version 2>/dev/null) && \
	    if [[ -n "$$publishedVer" ]]; then \
		echo "error: $$name@$$ver is already published to npm"; \
		exit 1; \
	    fi && \
	    echo "** Are you sure you want to tag and publish $$name@$$ver to npm?" && \
	    echo "** Enter to continue, Ctrl+C to abort." && \
	    read
	ver=$(shell cat package.json | json version) && \
	    date=$(shell date -u "+%Y-%m-%d") && \
	    git tag -a "v$$ver" -m "version $$ver ($$date)" && \
	    git push --tags origin && \
	    npm publish
