# node-urclient

A high-level client for the use of the `ur-agent` in SmartDataCenter.

## Overview

This library provides two classes:

- `URClient`, a client for performing node discovery and interacting
              with the `ur-agent` of a single node, via AMQP, for the
              purpose of running a command or transferring a file.
- `RunQueue`, a higher-level orchestration class, capable of performing
              the same action on an incrementally specified set of
              remote `ur-agent` instances, with per-node execution
              timeouts and starting line synchronisation

The software is basically done, but the documentation is still a work in
progress.

## `URClient`; Interacting With A Single Node

### `discovery()`; Node Discovery

Create an instance of `URClient`, thus:

```javascript
var mod_urclient = require('urclient');

URCLIENT = mod_urclient.create_ur_client({
    log: LOG,
    connect_timeout: 5000,
    enable_http: false,
    bind_ip: '10.0.0.1',
    amqp_config: {
        login: 'guest',
        pass: 'guest',
        host: '10.0.0.100',
        port: '5762'
    }
});

URCLIENT.on('ready', function () {
    var disco = URCLIENT.discovery({
        timeout: 3 * 1000,
        exclude_headnode: false
    });

    disco.on('server', function (server) {
        console.log('found server: %s (%s)', server.uuid, server.hostname);
    });
    disco.on('end', function () {
        console.log('discovery complete');
        process.exit(0);
    });
});
```

The `discovery()` function returns an event emitter that will emit `'server'`
events for each server that responds to the discovery request.  If the same
server responds more than once, only the first response will induce a
`'server'` event.  Once the discovery timeout has expired, an `'end'` event
will be emitted.

If you need to check for the existence of a specific set of hostnames or UUIDs,
provide that list to `discovery()` in the `node_list` parameter.  In this mode,
the `'end'` event will trigger when all nodes are discovered.  An `'error'`
will be generated in the event that we were not able to find a match for each
node in `node_list`.

### `exec()`; Execute Script On Node

*TODO*

### `send_file()`; Send File To Node

*TODO*

### `recv_file()`; Receive File From Node

*TODO*

## `RunQueue`; Interacting With A Set Of Nodes

### `add_server()`; Add Node To Set

*TODO*

### `start()`; Begin Execution On Nodes In Set

You may call `start()` at any time -- before or after selecting nodes, and
before calling `close()` to finish node selection.  This enables you to either
configure the entire set in advance and only execute once you have finished
discovery, or begin executing immediately as discovery messages are still
arriving.

### `close()`; Finish Specifying Set

*TODO*

### `'dispatch'` event

*TODO*

### `'success'` event

*TODO*

### `'failure'` event

*TODO*

### `'end'` event

*TODO*
