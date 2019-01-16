const elasticsearch = require('elasticsearch');

module.exports = function (config) {
	const client = elasticsearch.Client(config.elastic);

	return client;
};
