/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017 Joyent, Inc.
 */


var test = require('tape').test;
var dns = require('dns');
var fs = require('fs');
var os = require('os');
var UrClient = require('../index');
var bunyan = require('../node_modules/restify/node_modules/bunyan');


var CLIENT;
var SERVERS = [];
var TEST_FILE = 'node-urclient-test-' + os.hostname() + '-' +
                new Date().toISOString();


test('setup', function (t) {
    var log = bunyan.createLogger({ name: 'test' });

    dns.lookup(os.hostname(), function (err, localIp) {
        t.ifError(err, 'err getting local IP');

        CLIENT = UrClient.create_ur_client({
            connect_timeout: 5000,
            enable_http: true,
            bind_ip: localIp,
            log: log,
            amqp_config: {
                login: 'guest',
                password: 'guest',
                host: 'rabbitmq.coal.joyent.us',
                port: 5672
            }
        });

        CLIENT.once('ready', function onClientReady() {
            fs.linkSync(__filename, TEST_FILE);
            t.end();
        });

        CLIENT.once('error', function onClientErr(err) {
            console.error('UrClient connection error:');
            console.error(err);
            process.abort();
        });
    });
});


test('discover', function (t) {
    var disco = CLIENT.discover({
        timeout: 1 * 1000,
        exclude_headnode: false
    });

    disco.on('error', function onDiscoErr(err) {
        t.ifError(err);
    });

    disco.on('server', function onDiscoServer(server) {
        SERVERS.push(server);
    });

    disco.once('end', function onDiscoEnd() {
        t.ok(SERVERS.length >= 1, 'at least one server');

        var headnodes = SERVERS.filter(function (s) {
            return s.headnode;
        });

        t.ok(headnodes.length >= 1, 'at least one headnode');

        t.end();
    });
});


test('ping', function (t) {
    CLIENT.ping({
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onPing(err, msg) {
        t.ifError(err, 'no ping error');
        t.ok(msg.req_id, 'should have req_id');
        t.ok(msg.timestamp, 'should have timestamp');
        t.end();
    });
});


test('ping non-existent server', function (t) {
    CLIENT.ping({
        server_uuid: '2f501342-f818-11e6-a7ab-28cfe91f7d53',
        timeout: 1 * 1000
    }, function onPingErr(err, msg) {
        t.ok(err, 'should err');
        t.equal(err.message, 'ping timeout', 'err is ping timeout');
        t.end();
    });
});


test('exec', function (t) {
    CLIENT.exec({
        script: 'env; echo "foo" 1>&2',
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000,
        env: { TEST_PATH: '/usr/bin' }
    }, function onExec(err, result) {
       t.ifError(err, 'no err on exec');

       t.equal(result.exit_status, 0, 'exit status');
       t.ok(result.stdout.match('TEST_PATH=/usr/bin'), 'path set');
       t.deepEqual(result.stderr, 'foo\n', 'stderr set');

       t.end();
    });
});


test('exec with bad command', function (t) {
    CLIENT.exec({
        script: 'asdasdasdasd',
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onExec(err, result) {
       t.ifError(err, 'no err on exec');

       t.equal(result.exit_status, 127, 'exit status');
       t.deepEqual(result.stdout, '', 'blank stdout');
       t.ok(result.stderr.match('asdasdasdasd: not found'), 'error msg');

       t.end();
    });
});


test('exec with non-existent server', function (t) {
    CLIENT.exec({
        script: 'asdasdasdasd',
        server_uuid: '2f501342-f818-11e6-a7ab-28cfe91f7d54',
        timeout: 1 * 1000
    }, function onExecErr(err, result) {
       t.ok(err);

       t.deepEqual(err.message, 'timeout for host 2f501342-f818-11e6-a7ab-' +
                   '28cfe91f7d54', 'err message set');
       t.end();
    });
});


test('send_file', function (t) {
    CLIENT.send_file({
        src_file: TEST_FILE,
        dst_dir: '/tmp',
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onSendfile(err) {
        t.ifError(err, 'no err on send');
        t.end();
    });
});


test('send_file with existing file', function (t) {
    CLIENT.send_file({
        src_file: __filename,
        dst_dir: '/tmp',
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onSendfileErr(err) {
        t.ok(err, 'should err with existing file');

        t.deepEqual(err.code, 'EEXIST', 'file exists');
        t.deepEqual(err.message, 'file exists already',
                    'message describing err');
        t.end();
    });
});


test('send_file clobber existing file', function (t) {
    CLIENT.send_file({
        src_file: TEST_FILE,
        dst_dir: '/tmp',
        clobber: true,
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onSendfile(err) {
        t.ifError(err, 'no err on send');
        t.end();
    });
});


test('send_file to non-existent server', function (t) {
    CLIENT.send_file({
        src_file: TEST_FILE,
        dst_dir: '/tmp',
        server_uuid: '2f501342-f818-11e6-a7ab-28cfe91f7d54',
        timeout: 1 * 1000
    }, function onSendfileErr(err) {
        t.ok(err, 'should err with absent server');
        t.deepEqual(err.message, 'timed out', 'message describing err');
        t.end();
    });
});


test('recv_file', function (t) {
    var dstFileName = TEST_FILE + '.2';

    CLIENT.recv_file({
        src_file: '/tmp/' + TEST_FILE,
        dst_dir: __dirname,
        dst_file: dstFileName,
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onRecvFile(err) {
        t.ifError(err, 'no err on recv');
        t.ok(fs.existsSync(dstFileName), 'file received');
        fs.unlinkSync(dstFileName);
        t.end();
    });
});


test('recv_file of non-existent file', function (t) {
    CLIENT.recv_file({
        src_file: '/tmp/2f501342-f818-11e6-a7ab-28cfe91f7d55',
        dst_dir: __dirname,
        dst_file: TEST_FILE + '.3',
        server_uuid: SERVERS[0].uuid,
        timeout: 1 * 1000
    }, function onRecvFileErr(err) {
        t.ok(err, 'should err with absent file');

        t.deepEqual(err.code, 'ENOENT', 'err code');
        t.deepEqual(err.message, 'file does not exist',
                    'message describing err');
        t.end();
    });
});


test('recv_file from non-existent server', function (t) {
    CLIENT.recv_file({
        src_file: '/tmp/' + TEST_FILE,
        dst_dir: __dirname,
        dst_file: TEST_FILE + '.4',
        server_uuid: '2f501342-f818-11e6-a7ab-28cfe91f7d54',
        timeout: 1 * 1000
    }, function onRecvFile(err) {
        t.ok(err, 'should err with absent server');
        t.deepEqual(err.message, 'timed out', 'message describing err');
        t.end();
    });
});


test('teardown', function (t) {
    fs.unlinkSync(TEST_FILE);
    CLIENT.close();
    t.end();
});
