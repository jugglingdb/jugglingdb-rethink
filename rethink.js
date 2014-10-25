/* jshint sub: true */
var r = require("rethinkdb");
var moment = require("moment");
var gpool = require("generic-pool");
var async = require("async");
var _ = require("lodash-node");

exports.initialize = function initializeSchema(schema, callback) {
    if (!r) return;

    var s = schema.settings;

    if (schema.settings.rs) {

        s.rs = schema.settings.rs;
        if (schema.settings.url) {
            var uris = schema.settings.url.split(',');
            s.hosts = [];
            s.ports = [];
            uris.forEach(function(uri) {
                var url = require('url').parse(uri);

                s.hosts.push(url.hostname || 'localhost');
                s.ports.push(parseInt(url.port || '28015', 10));

                if (!s.database) s.database = url.pathname.replace(/^\//, '');
                if (!s.username) s.username = url.auth && url.auth.split(':')[0];
                if (!s.password) s.password = url.auth && url.auth.split(':')[1];
            });
        }

        s.database = s.database || 'test';

    } else {

        if (schema.settings.url) {
            var url = require('url').parse(schema.settings.url);
            s.host = url.hostname;
            s.port = url.port;
            s.database = url.pathname.replace(/^\//, '');
            s.username = url.auth && url.auth.split(':')[0];
            s.password = url.auth && url.auth.split(':')[1];
        }

        s.host = s.host || 'localhost';
        s.port = parseInt(s.port || '28015', 10);
        s.database = s.database || 'test';

    }

    s.safe = s.safe || false;

    schema.adapter = new RethinkDB(s, schema);
    schema.adapter.pool = gpool.Pool({
        name: "jugglingdb-rethink-pool",
        create: function(cb) {
            r.connect({host: s.host, port: s.port}, function (error, client) {
                if (error) return cb(error, null);
                cb(null, client);
            });
        },
        destroy: function(client) {
            client.close();
        },
        max: s.poolMax || 10,
        min: s.poolMin || 1,
        idleTimeoutMillis: 30000,
        log: function(what, level) {
            if (level === "error")
                require('fs').appendFile("jugglingdb-rethink-pool.log", what + "\r\n");
        }
    });

    process.nextTick(callback);
};

function RethinkDB(s, schema) {
    var i, n;
    this.name = 'rethink';
    this._models = {};
    this._foreignKeys = {};
    this.collections = {};
    this.schema = schema;
    this.s = s;
    this.database = s.database;
}

RethinkDB.prototype.connect = function(cb) {
    cb(); // connection pooling handles it
};

RethinkDB.prototype.define = function (descr) {
    if (!descr.settings) descr.settings = {};
    this._models[descr.model.modelName] = descr;
    this._foreignKeys[descr.model.modelName] = [];
};

// creates tables if not exists
RethinkDB.prototype.autoupdate = function(cb) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        r.db(_this.database).tableList().run(client, function(error, cursor) {
            if (!error) {
                cursor.toArray(function(error, list) {
                    async.each(Object.keys(_this._models), function(model, cb2) {
                        if (list.length === 0 || list.indexOf(model) < 0) {
                            r.db(_this.database).tableCreate(model).run(client, function(error) {
                                if (error) return cb2(error);

                                createIndices();
                            });
                        }
                        else {
                            createIndices();
                        }

                        function createIndices() {
                            var properties = _this._models[model].properties;
                            var settings = _this._models[model].settings;
                            var indexCollection = _.extend({}, properties, settings);

                            function checkAndCreate(list, indexName, indexOption, indexFunction, cb) {

                                // Don't attempt to create an index on primary key 'id'
                                if (indexName !== 'id' && _hasIndex(_this, model, indexName) && list.indexOf(indexName) < 0) {
                                    var query = r.db(_this.database).table(model);
                                    if (indexFunction) {
                                        query = query.indexCreate(indexName, indexFunction, indexOption);
                                    }
                                    else {
                                        query = query.indexCreate(indexName, indexOption);
                                    }
                                    query.run(client, cb);
                                }
                                else {
                                    cb();
                                }
                            }

                            if (!_.isEmpty(indexCollection)) {
                                r.db(_this.database).table(model).indexList().run(client, function(error, cursor) {
                                    if (error) return cb2(error);

                                    cursor.toArray(function(error, list) {
                                        if (error) return cb2(error);

                                        async.each(Object.keys(indexCollection), function (indexName, cb3) {
                                            var indexConf = indexCollection[indexName];
                                            checkAndCreate(list, indexName, indexConf.indexOption || {}, indexConf.indexFunction, cb3);
                                        }, function(err) {
                                            cb2(err);
                                        });
                                    });
                                });
                            } else {
                                cb2();
                            }
                        }
                    }, function(err) {
                        _this.pool.release(client);
                        cb(err);
                    });
                });
            } else {
                _this.pool.release(client);
                cb(error);
            }
        });
    });
};

// drops tables and re-creates them
RethinkDB.prototype.automigrate = function(cb) {
    this.autoupdate(cb);
};

// checks if database needs to be actualized
RethinkDB.prototype.isActual = function(cb) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        r.db(_this.database).tableList().run(client, function(error, cursor) {
            if (!error) {
                if (cursor.next()) {
                    cursor.toArray(function(error, list) {
                        if (error) {
                            _this.pool.release(client);
                            return cb(error);
                        }
                        var actual = true;
                        async.each(Object.keys(_this._models), function(model, cb2) {
                            if(!actual) return cb2();

                            var properties = _this._models[model].properties;
                            var settings = _this._models[model].settings;
                            var indexCollection = _.extend({}, properties, settings);
                            if (list.indexOf(model) < 0) {
                                actual = false;
                                cb2();
                            } else {
                                r.db(_this.database).table(model).indexList().run(client, function(error, cursor) {
                                    if (error) return cb2(error);

                                    cursor.toArray(function(error, list) {
                                        if (error || !actual) return cb2(error);


                                        Object.keys(indexCollection).forEach(function (property) {
                                            if (_hasIndex(_this, model, property) && list.indexOf(property) < 0)
                                                actual = false;
                                        });
                                        cb2();
                                    });
                                });
                            }
                        }, function(err) {
                            _this.pool.release(client);
                            cb(err, actual);
                        });
                    });
                }
                else {
                    _this.pool.release(client);
                    cb(null, _.isEmpty(_this._models));
                }
            } else {
                _this.pool.release(client);
                cb(error);
            }
        });
    });
};

RethinkDB.prototype.defineForeignKey = function(name, key, anotherName, cb) {
    this._foreignKeys[name].push(key);
    cb(null, String);
};

RethinkDB.prototype.create = function (model, data, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        if (data.id === null || data.id === undefined) {
            delete data.id;
        }
        Object.keys(data).forEach(function (key) {
            if (data[key] === undefined)
                data[key] = null;
        });
        r.db(_this.database).table(model).insert(data).run(client, function (err, m) {
            _this.pool.release(client);
            err = err || m.first_error && new Error(m.first_error);
            if (m.generated_keys) {
                data.id = m.generated_keys[0];
            }
            callback(err, err ? null : data.id);
        });
    });
};

RethinkDB.prototype.save = function (model, data, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        Object.keys(data).forEach(function (key) {
            if (data[key] === undefined)
                data[key] = null;
        });
        r.db(_this.database).table(model).insert(data, { upsert: true }).run(client, function (err, notice) {
            _this.pool.release(client);
            err = err || notice.first_error && new Error(notice.first_error);
            callback(err, notice);
        });
    });
};

RethinkDB.prototype.exists = function (model, id, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        r.db(_this.database).table(model).get(id).run(client, function (err, data) {
            _this.pool.release(client);
            callback(err, !!(!err && data));
        });
    });
};

RethinkDB.prototype.find = function find(model, id, callback) {
    var _this = this,
        _keys;

    var done = function (client) {

        return function finished(err, data) {

            // Acquire the keys for this model
            _keys = _this._models[model].properties;

            if (data) {

                // Pass to expansion helper
                _expandResult(data, _keys);
            }

            // Release connection
            _this.pool.release(client);

            // Done
            callback(err, data);
        };
    };

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        r.db(_this.database)
            .table(model)
            .get(id)
            .run(client, done(client));
    });
};

RethinkDB.prototype.updateOrCreate = function updateOrCreate(model, data, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        if (data.id === null || data.id === undefined) {
            delete data.id;
        }
        Object.keys(data).forEach(function (key) {
            if (data[key] === undefined)
                data[key] = null;
        });
        r.db(_this.database).table(model).insert(data, {upsert: true, returnVals: true}).run(client, function (err, m) {
            _this.pool.release(client);
            err = err || m.first_error && new Error(m.first_error);
            callback(err, err ? null : m['new_val']);
        });
    });
};

RethinkDB.prototype.destroy = function destroy(model, id, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        r.db(_this.database).table(model).get(id).delete().run(client, function(error, result) {
            _this.pool.release(client);
            callback(error);
        });
    });
};

RethinkDB.prototype.all = function all(model, filter, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        if (!filter) {
            filter = {};
        }

        var promise = r.db(_this.database).table(model);

        if (filter.where) {
            promise = _processWhere(_this, model, filter.where, promise);
        }

        if (filter.order) {
            var keys = filter.order;
            if (typeof keys === 'string') {
                keys = keys.split(',');
            }
            keys.forEach(function(key) {
                var m = key.match(/\s+(A|DE)SC$/);
                key = key.replace(/\s+(A|DE)SC$/, '').trim();
                if (m && m[1] === 'DE') {
                    promise = promise.orderBy(r.desc(key));
                } else {
                    promise = promise.orderBy(r.asc(key));
                }
            });
        } else {
            // default sort by id
            promise = promise.orderBy(r.asc("id"));
        }

        if (filter.skip) {
            promise = promise.skip(filter.skip);
        } else if (filter.offset) {
            promise = promise.skip(filter.offset);
        }
        if (filter.limit) {
            promise = promise.limit(filter.limit);
        }

        promise.run(client, function(error, cursor) {

            if (error || !cursor) {
                _this.pool.release(client);
                return callback(error, null);
            }

            _keys = _this._models[model].properties;
            _model = _this._models[model].model;

            cursor.toArray(function (err, data) {
                if (err) {
                    _this.pool.release(client);
                    return callback(err);
                }

                data.forEach(function(element, index) {
                    _expandResult(element, _keys);
                });

                _this.pool.release(client);

                if (filter && filter.include && filter.include.length > 0) {
                    _model.include(data, filter.include, callback);
                } else {
                    callback(null, data);
                }
            });
        });
    }, 0); // high-priority pooling
};

RethinkDB.prototype.destroyAll = function destroyAll(model, callback) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;
        r.db(_this.database).table(model).delete().run(client, function(error, result) {
            _this.pool.release(client);
            callback(error, result);
        });
    });
};

RethinkDB.prototype.count = function count(model, callback, where) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        var promise = r.db(_this.database).table(model);

        if (where && typeof where === "object")
            promise = _processWhere(_this, model, where, promise);

        promise.count().run(client, function (err, count) {
            _this.pool.release(client);
            callback(err, count);
        });
    });
};

RethinkDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {
    var _this = this;

    _this.pool.acquire(function(error, client) {
        if (error) throw error;

        data.id = id;
        Object.keys(data).forEach(function (key) {
            if (data[key] === undefined)
                data[key] = null;
        });
        r.db(_this.database).table(model).update(data).run(client, function(err, object) {
            _this.pool.release(client);
            cb(err, data);
        });
    });
};

RethinkDB.prototype.disconnect = function () {
    var _this = this;
    _this.pool.drain(function() {
        _this.pool.destroyAllNow();
    });
};

/*
    Some values may require post-processing. Do that here.
*/
function _expandResult(result, keys) {

    Object.keys(result).forEach(function (key) {

        if (!keys.hasOwnProperty(key)) return;

        if (keys[key]['type'] &&
            keys[key]['type']['name'] === 'Date' &&
            !(result[key] instanceof Date)) {

            // Expand date result data, backward compatible
            result[key] = moment.unix(result[key]).toDate();
        }
    });
}

function _hasIndex(_this, model, key) {

    // Primary key always hasIndex
    if (key === 'id') return true;

    var modelDef = _this._models[model];
    return (_.isObject(modelDef.properties[key]) && modelDef.properties[key].index) ||
        _this._foreignKeys[model].indexOf(key) >= 0 ||
        (_.isObject(modelDef.settings[key]) && modelDef.settings[key].index);
}

function _toMatchExpr(regexp) {
    var expr = regexp.toString(),
        exprStop = expr.lastIndexOf('/'),
        exprCi = expr.slice(exprStop).search('i');

    expr = expr.slice(1, exprStop);

    if (exprCi > -1) {
        expr = '(?i)' + expr;
    }

    return expr;
}

function _matchFn(k, cond) {

    var matchExpr = _toMatchExpr(cond);
    return function (row) {
        return row(k).match(matchExpr);
    };
}

function _inqFn(k, cond) {

    return function (row) {
        return r.expr(cond).contains(row(k));
    };
}

function _processWhere(_this, model, where, promise) {
    //Transform promise (a rethinkdb query) based on the given where clause.
    //Returns the modified promise
    var i, m, keys;
    var inqFn, matchFn;
    var indexed = false;
    var queryParts = [];
    Object.keys(where).forEach(function (k) {
        var spec, cond = where[k];
        var allConds = [];
        if (cond && cond.constructor.name === 'Object') {
            keys = Object.keys(cond);
            for (i = 0, m = keys.length; i < m; i++) {
                allConds.push([ keys[i], cond[keys[i]] ]);
            }
        }
        else {
            allConds.push([ false, cond ]);
        }
        var hasIndex = _hasIndex(_this, model, k);
        for (i = 0, m = allConds.length; i < m; i++) {
            spec = allConds[i][0];
            cond = allConds[i][1];
            if (cond instanceof Date) {

                // Time comparison should still work regardless of
                // whether we stored the timestamp (old approach)
                // or the native Date obj.

                // TODO: Consider removing this on a major version bump?
                cond = moment(cond).unix();
            }
            if (!spec) {
                if (cond instanceof RegExp) {
                    matchFn = _matchFn(k, cond);
                    promise = promise.filter(matchFn);
                } else if(!indexed && hasIndex) {
                    promise = promise.getAll(cond, {index: k});
                    indexed = true;
                } else {
                    queryParts.push(r.row(k).eq(cond));
                }
            }
            else {
                switch (spec) {
                    case 'between':
                        if(!indexed && hasIndex) {
                            promise = promise.between(cond[0], cond[1], {index: k});
                            indexed = true;
                        } else {
                            queryParts.push(r.row(k).ge(cond[0]).and(r.row(k).le(cond[1])));
                        }
                        break;
                    case 'inq':
                        if(!indexed && hasIndex) {
                            cond.push({ index: k });
                            promise = promise.getAll.apply(promise, cond);
                            indexed = true;
                        } else {
                            inqFn = _inqFn(k, cond);
                            queryParts.push(inqFn);
                        }
                        break;
                    case 'nin':
                        queryParts.push(r.expr(cond).contains(r.row(k)).not());
                        break;
                    case 'gt':
                        if(!indexed && hasIndex) {
                            promise = promise.between(cond, null, {index: k, left_bound: 'open'});
                            indexed = true;
                        }
                        else {
                            queryParts.push(r.row(k).gt(cond));
                        }
                        break;
                    case 'gte':
                        if(!indexed && hasIndex) {
                            promise = promise.between(cond, null, {index: k, left_bound: 'closed'});
                            indexed = true;
                        }
                        else {
                            queryParts.push(r.row(k).ge(cond));
                        }
                        break;
                    case 'lt':
                        if(!indexed && hasIndex) {
                            promise = promise.between(null, cond, {index: k, right_bound: 'open'});
                            indexed = true;
                        }
                        else {
                            queryParts.push(r.row(k).lt(cond));
                        }
                        break;
                    case 'lte':
                        if(!indexed && hasIndex) {
                            promise = promise.between(null, cond, {index: k, right_bound: 'closed'});
                            indexed = true;
                        }
                        else {
                            queryParts.push(r.row(k).le(cond));
                        }
                        break;
                    case 'neq':
                        queryParts.push(r.row(k).ne(cond));
                        break;
                }
            }
        }
    });

    var query;
    queryParts.forEach(function (comp) {
        if (!query) {
            query = comp;
        } else {
            query = query.and(comp);
        }
    });
    if (query) {
        promise = promise.filter(query);
    }

    return promise;
}
