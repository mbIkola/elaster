const mongo = require('mongojs');

module.exports = function (config) {
	const collections = config.collections.map(function (c) {
		return c.name;
	});

	return new mongo(config.mongo.connection, collections);
};
