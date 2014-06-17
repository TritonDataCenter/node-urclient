/* vim: set ts=8 sts=8 sw=8 noet: */

var lib_ur_client = require('./lib/ur_client');
var lib_runqueue = require('./lib/runqueue');

function
create_ur_client(options)
{
	var ur = new lib_ur_client.URClient(options);

	return (ur);
}

function
create_run_queue(options)
{
	var rq = new lib_runqueue.RunQueue(options);

	return (rq);
}

module.exports = {
	create_ur_client: create_ur_client,
	create_run_queue: create_run_queue
};
