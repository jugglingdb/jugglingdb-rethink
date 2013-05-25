var r = require('rethinkdb');
var moment = require('moment');

exports.initialize = function initializeSchema(schema, callback) {
    if (!r) return;

    var s = schema.settings;

    if (schema.settings.rs) {

        s.rs = schema.settings.rs;
        if (schema.settings.url) {
            var uris = schema.settings.url.split(',');
            s.hosts = []
            s.ports = []
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

    schema.adapter = new RethinkDB(s, schema, callback);
};

function RethinkDB(s, schema, callback) {
    var i, n;
    this.name = 'rethink';
    this._models = {};
    this.collections = {};
    this.schema = schema;
    this.s = s;
};

RethinkDB.prototype.connect = function(cb) {
    r.connect({host: this.s.host, port: this.s.port}, function (error, client) {
        if (error) throw error;
        this.client = client;
        this.database = this.s.db;
        cb();
    }.bind(this));
};

RethinkDB.prototype.define = function (descr) {
    if (!descr.settings) descr.settings = {};
    this._models[descr.model.modelName] = descr;

    if (!this.schema.connecting && !this.schema.connected)
        this.schema.connect();
};

// creates tables if not exists
RethinkDB.prototype.autoupdate = function(cb) {
    var _this = this;

    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.autoupdate(cb);
        }.bind(this));
    }

    r.db(this.database).tableList().run(this.client, function(error, cursor) {
        if (!error) {
            cursor.toArray(function(error, list) {
                Object.keys(_this._models).forEach(function (model) {
                    if (list.length == 0 || list.indexOf(model) < 0)
                        r.db(_this.database).tableCreate(model).run(_this.client, function() {});
                });
                cb();
            });
        } else {
            cb(error);
        }
    });
};

// drops tables and re-creates them
RethinkDB.prototype.automigrate = function(cb) {
    this.autoupdate(cb);
};

// checks if database needs to be actualized
RethinkDB.prototype.isActual = function(cb) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.isActual(cb);
        }.bind(this));
    }

    r.db(this.database).tableList().run(this.client, function(error, cursor) {
        if (!error) {
            if (cursor.hasNext()) {
                cursor.toArray(function(error, list) {
                    if (error)
                        return cb(error);
                    Object.keys(this._models).forEach(function (model) {
                        if (list.indexOf(model) < 0)
                            cb(null, false);
                    });
                    cb(null, true);
                });
            } else if (this._models.length > 0)
                cb(null, false);
        } else {
            cb(error);
        }
    });
};

RethinkDB.prototype.defineForeignKey = function(name, key, anotherName, cb) {
    cb(null, String);
}

//RethinkDB.prototype.defineProperty = function (model, prop, params) {
//    this._models[model].properties[prop] = params;
//};

RethinkDB.prototype.create = function (model, data, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.create(model, data, callback);
        }.bind(this));
    }

    if (data.id === null || data.id === undefined) {
        delete data.id;
    }
    Object.keys(data).forEach(function (key) {
        if (data[key] instanceof Date)
            data[key] = moment(data[key]).unix();
    });
    r.db(this.database).table(model).insert(data).run(this.client, function (err, m) {
        callback(err, err ? null : m['generated_keys'][0]);
    });
};

RethinkDB.prototype.save = function (model, data, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.save(model, data, callback);
        }.bind(this));
    }

    r.db(this.database).table(model).update(data).run(this.client, function (err, notice) {
        callback(err, notice);
    });
};

RethinkDB.prototype.exists = function (model, id, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.exists(model, id, callback);
        }.bind(this));
    }

    r.db(this.database).table(model).get(id).run(this.client, function (err, data) {
        callback(err, !!(!err && data));
    });
};

RethinkDB.prototype.find = function find(model, id, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.find(model, id, callback);
        }.bind(this));
    }

    r.db(this.database).table(model).get(id).run(this.client, function (err, data) {
        if (data)
            Object.keys(data).forEach(function (key) {
                if (this._models[model].properties[key]['type']['name'] == "Date")
                    data[key] = moment.unix(data[key]).toDate();
            }.bind(this));
        callback(err, data);
    }.bind(this));
};

RethinkDB.prototype.updateOrCreate = function updateOrCreate(model, data, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.updateOrCreate(model, data, callback);
        }.bind(this));
    }

    if (data.id === null || data.id === undefined) {
        delete data.id;
    }
    data.forEach(function (value, key) {
        if (value instanceof Date)
            data[key] = moment(value).unix();
    });
    r.db(this.database).table(model).insert(data, {upsert: true}).run(this.client, function (err, m) {
        callback(err, err ? null : m['generated_keys'][0]);
    });
};

RethinkDB.prototype.destroy = function destroy(model, id, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.destroy(model, id, callback);
        }.bind(this));
    }

    r.db(this.database).table(model).get(id).delete().run(this.client, function(error, result) {
        callback(error);
    });
};

RethinkDB.prototype.all = function all(model, filter, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.all(model, filter, callback);
        }.bind(this));
    }

    if (!filter) {
        filter = {};
    }
    var query = {};

    var promise = r.db(this.database).table(model);

    if (filter.where) {
        Object.keys(filter.where).forEach(function (k) {
            var cond = filter.where[k];
            var spec = false;
            if (cond && cond.constructor.name === 'Object') {
                spec = Object.keys(cond)[0];
                cond = cond[spec];
            }
            if (spec) {
                if (spec === 'between') {
                    promise = promise.filter(r.row(k).ge(cond[0])).filter(r.row(k).le(cond[1]));
                } else if (spec === 'inq') {
                    var expr = '(function(row) { return ' + JSON.stringify(cond) + '.indexOf(row.' + k + ') >= 0 })';
                    promise = promise.filter(r.js(expr));
                } else {
                    query[k] = {};
                    query[k]['$' + spec] = cond;
                }
            } else {
                if (cond === null) {
                    query[k] = {$type: 10};
                } else {
                    query[k] = cond;
                }

                promise = promise.filter(query);
            }
        });
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
        promise = promise.orderBy(r.asc("id"))
    }

    if (filter.skip) {
        promise = promise.skip(filter.skip);
    } else if (filter.offset) {
        promise = promise.skip(filter.offset);
    }
    if (filter.limit) {
        promise = promise.limit(filter.limit);
    }

    _keys = this._models[model].properties;
    _model = this._models[model].model;

    promise.run(this.client, function(error, cursor) {
        if (error)
            callback(error, null);
        cursor.toArray(function (err, data) {
            if (err) return callback(err);

            data.forEach(function(element, index) {
                Object.keys(element).forEach(function (key) {
                    if (!_keys.hasOwnProperty(key)) return;
                    if (_keys[key]['type']['name'] == "Date")
                        element[key] = moment.unix(element[key]).toDate();
                    if (_keys[key]['type']['name'] == "Mumber")
                        element[key] = Number(element[key]);
                });
                data[index] = element;
            });

            if (filter && filter.include && filter.include.length > 0) {
                _model.include(data, filter.include, callback);
            } else {
                callback(null, data);
            }
        });
    });
};

RethinkDB.prototype.destroyAll = function destroyAll(model, callback) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.destroyAll(model, callback);
        }.bind(this));
    }
    r.db(this.database).table(model).delete().run(this.client, callback);
};

RethinkDB.prototype.count = function count(model, callback, where) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.count(model, callback, where);
        }.bind(this));
    }
    var promise = r.db(this.database).table(model);
    if (where && typeof where == "object")
        promise = promise.filter(where)
    promise.count().run(this.client, function (err, count) {
        callback(err, count);
    });
};

RethinkDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {
    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.updateAttributes(model, id, data, callback);
        }.bind(this));
    }
    data.id = id
    r.db(this.database).table(model).update(data).run(this.client, function(err, object) {
        cb(err, data);
    });
};

RethinkDB.prototype.disconnect = function () {
    if (this.schema.connected)
        this.client.close();
};

