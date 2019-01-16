const _ = require('underscore');
const moment = require('moment');
const async = require('async');
const config = require('../config');

const db = require('./db')(config);
const elastic = require('./elastic')(config);

const through = require('through');
const single = require('single-line-log');
const debug = require('debug')('elaster');

require('colors');
require('debug').enable('elaster');

function format(duration) {
	return duration.hours() + ':' + duration.minutes() + ':' + duration.seconds() + ':' + duration.milliseconds();
}

function exportCollection(desc, callback) {
	let collection = db[desc.name];
	let query = desc.query || {};

	if (!collection) {
		return callback('collection ' + desc.name + ' does not exist.');
	}

	debug(('====> exporting collection [' + desc.name + ']').bold.white);

	let started = moment();

	async.waterfall([
		function (next) {
			debug('----> checking connection to elastic');
			elastic.ping({requestTimeout: 1000}, function (err) {
				next(err);
			});
		},
		function (next) {
			debug('----> dropping existing index [' + desc.index + ']');
			elastic.indices.delete({index: desc.index}, function (err) {
				let indexMissing = err && err.message.indexOf('IndexMissingException') === 0;
				next(indexMissing ? null : err);
			});
		},
		function (next) {
			debug('----> creating new index [' + desc.index + ']');
			elastic.indices.create({index: desc.index}, function (err) {
				next(err);
			});
		},
		function (next) {
			debug('----> initialize index mapping');

			if (!desc.mappings) {
				return next();
			}

			elastic.indices.putMapping({index: desc.index, type: desc.type, body: desc.mappings }, function (err) {
				next(err);
			});
		},
		function (next) {
			debug('----> analizing collection [' + desc.name + ']');
			collection.count(query, function (err, total) {
				if (err) {
					return next(err);
				}

				debug('----> find ' + total + ' documentents to export');
				next(null, total);
			});
		},
		function (total, next) {
			debug('----> streaming collection to elastic');

			let takeFields = through(function (item) {
				if (desc.fields) {
					item = _.pick(item, desc.fields);
				}

				this.queue(item);
			});

			let postToElastic = through(function (item) {
				let me = this;

				me.pause();

				elastic.create({
					index: desc.index,
					type: desc.type,
					id: item._id.toString(),
					body: item
				}, function (err) {
					if (err) {
						single(('failed to create document in elastic.').bold.red);
						return next(err);
					}

					me.queue(item);
					me.resume();
				});
			});

			let progress = function () {
				let count = 0;
				return through(function () {
					let percentage = Math.floor(100 * ++count / total);
					single(('------> processed ' + count + ' documents [' + percentage + '%]').magenta);
				});
			};

			let stream = collection
				.find(query)
				.sort({_id: 1})
				.pipe(takeFields)
				.pipe(postToElastic)
				.pipe(progress());

			stream.on('end', function (err) {
				next(err, total);
			});
		},
	], function (err) {
		if (err) {
			single(('====> collection [' + desc.name + '] - failed to export.\n').bold.red);
			debug(err);
			return callback(err);
		}

		let duration = moment.duration(moment().diff(started));

		debug(('====> collection [' + desc.name + '] - exported successfully.').green);
		debug(('====> time elapsed ' + format(duration) + '\n').green);

		callback(null);
	});
}

function close() {
	async.each([db, elastic], _close);

	function _close(conn, callback) {
		conn.close(callback);
	}
}

function exporter(collections) {
	debug("Collection to export: ", collections);
	const exports = collections.map(function (c) {
		return function (callback) {
			exportCollection(c, callback);
		};
	});

	async.series(exports, close);
}

module.exports = {
	run: exporter
};
