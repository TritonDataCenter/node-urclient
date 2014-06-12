/* vim: set ts=8 sts=8 sw=8 noet: */

var lib_ur_client = require('./lib/ur_client');

function
create_ur_client(options)
{
	var ur = new lib_ur_client.URClient(options);

	return (ur);
}

module.exports = {
	create_ur_client: create_ur_client
};
