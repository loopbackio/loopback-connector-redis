'use strict';

/**
 * Module dependencies
 */
var g = require('strong-globalize')();
var redis = require('redis');
var async = require('async');

exports.initialize = function initializeSchema(dataSource, callback) {
  if (!redis) return;

  if (dataSource.settings.url) {
    var url = require('url');
    var redisUrl = url.parse(dataSource.settings.url);
    var redisAuth = (redisUrl.auth || '').split(':');
    dataSource.settings.host = redisUrl.hostname;
    dataSource.settings.port = redisUrl.port;

    if (redisAuth.length > 1) {
      dataSource.settings.db = redisAuth[0];
      dataSource.settings.password = redisAuth.slice(1).join(':');
    }
  }

  if (!dataSource.settings.options) {
    dataSource.settings.options = {};
  }
  if (dataSource.settings.port) {
    dataSource.settings.options.port = dataSource.settings.port;
  }
  if (dataSource.settings.host) {
    dataSource.settings.options.host = dataSource.settings.host;
  }

  dataSource.client = redis.createClient(dataSource.settings.options);
  if (dataSource.settings.password) {
    dataSource.client.auth(dataSource.settings.password);
  }
  var callbackCalled = false;
  var database = dataSource.settings.hasOwnProperty('database') &&
    dataSource.settings.database;
  dataSource.client.on('connect', function() {
    if (!callbackCalled && database === false) {
      callbackCalled = true;
      callback();
    } else if (database !== false) {
      if (callbackCalled) {
        return dataSource.client.select(dataSource.settings.database);
      } else {
        callbackCalled = true;
        return dataSource.client.select(dataSource.settings.database, callback);
      }
    }
  });
  dataSource.client.on('error', function(error) {
    g.log(String(error));
  });

  var clientWrapper = new Client(dataSource.client);

  dataSource.connector = new BridgeToRedis(clientWrapper);
  clientWrapper._adapter = dataSource.connector;
};

function Client(client) {
  this._client = client;
}

var commands = Object.keys(redis.Multi.prototype).filter(function(n) {
  return n.match(/^[a-z]/);
});

commands.forEach(function(cmd) {
  Client.prototype[cmd] = function(args, callback) {
    var c = this._client;
    var log;

    if (typeof args === 'string') {
      args = [args];
    }

    if (typeof args === 'function') {
      callback = args;
      args = [];
    }

    if (!args) args = [];

    log = this._adapter.logger(cmd.toUpperCase() + ' ' + args.map(function(a) {
      if (typeof a === 'object') return JSON.stringify(a);
      return a;
    }).join(' '));
    args.push(function(err, res) {
      if (err) console.log(err);
      log();
      if (callback) {
        callback(err, res);
      }
    });

    c[cmd].apply(c, args);
  };
});

Client.prototype.multi = function(commands, callback) {
  if (commands.length === 0) return callback && callback();
  if (commands.length === 1) {
    return this[commands[0].shift().toLowerCase()].call(
        this,
        commands[0],
        callback && function(e, r) { callback(e, [r]); });
  }
  var log = this._adapter.logger('MULTI\n  ' + commands.map(function(x) {
    return x.join(' ');
  }).join('\n  ') + '\nEXEC');
  this._client.multi(commands).exec(function(err, replies) {
    if (err) console.log(err);
    log();
    if (callback)
      callback(err, replies);
  });
};

Client.prototype.transaction = function() {
  return new Transaction(this);
};

function Transaction(client) {
  this._client = client;
  this._handlers = [];
  this._schedule = [];
}

Transaction.prototype.run = function(cb) {
  var t = this;
  var atLeastOneHandler = false;
  switch (this._schedule.length) {
    case 0: return cb();
    case 1: return this._client[this._schedule[0].shift()].call(
      this._client,
      this._schedule[0],
      this._handlers[0] || cb);
    default:
      this._client.multi(this._schedule, function(err, replies) {
        if (err) return cb(err);
        replies.forEach(function(r, i) {
          if (t._handlers[i]) {
            atLeastOneHandler = true;
            t._handlers[i](err, r);
          }
        });
        if (!atLeastOneHandler) cb(err);
      });
  }
};

commands.forEach(function(k) {
  Transaction.prototype[k] = function(args, cb) {
    if (typeof args === 'string') {
      args = [args];
    }
    args.unshift(k);
    this._schedule.push(args);
    this._handlers.push(cb || false);
  };
});

function BridgeToRedis(client) {
  this._models = {};
  this.client = client;
  this.indexes = {};
  this.name = 'redis';
}

BridgeToRedis.prototype.define = function(descr) {
  var m = descr.model.modelName;
  this._models[m] = descr;
  this.indexes[m] = {};
  Object.keys(descr.properties).forEach(function(prop) {
    if (descr.properties[prop].index) {
      this.indexes[m][prop] = descr.properties[prop].type;
    }
  }.bind(this));
};

BridgeToRedis.prototype.defineForeignKey = function(model, key, cb) {
  this.indexes[model][key] = Number;
  cb(null, Number);
};

BridgeToRedis.prototype.forDb = function(model, data) {
  var p = this._models[model].properties;
  for (var i in data) {
    if (typeof data[i] === 'undefined') {
      delete data[i];
      continue;
    }
    if (!p[i]) {
      data[i] = JSON.stringify(data[i]);
      continue;
    }
    if (p[i].type.name != 'Boolean' && !(i in data && data[i] !== null)) {
      data[i] = '';
      continue;
    }
    switch (p[i].type.name) {
      case 'Date':
        if (data[i].getTime) {
          // just Date object
          data[i] = data[i].getTime().toString();
        } else if (typeof data[i] === 'number') {
          // number of milliseconds
          data[i] = parseInt(data[i], 10).toString();
        } else if (typeof data[i] === 'string' && !isNaN(parseInt(data[i]))) {
          // numeric string
          data[i] = data[i];
        } else {
          // something odd
          data[i] = '0';
        }
        break;
      case 'Number':
        data[i] = data[i] && data[i].toString();
        break;
      case 'Boolean':
        data[i] = data[i] ? 'true' : 'false';
        break;
      case 'String':
      case 'Text':
        break;
      default:
        data[i] = JSON.stringify(data[i]);
    }
  }
  return data;
};

/*!
 * Decide if a field should be included
 * @param {Object} fields
 * @returns {Boolean}
 * @private
 */
function isIncluded(fields, f) {
  if (!fields) {
    return true;
  }
  if (Array.isArray(fields)) {
    return fields.indexOf(f) >= 0;
  }
  if (fields[f]) {
    // Included
    return true;
  }
  if ((f in fields) && !fields[f]) {
    // Excluded
    return false;
  }
  for (var f1 in fields) {
    return !fields[f1]; // If the fields has exclusion
  }
  return true;
}

BridgeToRedis.prototype.fromDb = function(model, data, fields) {
  fields = fields || {};
  var p = this._models[model].properties;
  var d;
  for (var i in data) {
    if (!isIncluded(fields, i)) {
      // Exclude
      delete data[i];
      continue;
    }
    if (!p[i]) {
      data[i] = JSON.parse(data[i]);
      continue;
    }
    if (!data[i]) {
      data[i] = '';
      continue;
    }
    switch (p[i].type.name) {
      case 'Date':
        d = new Date(data[i]);
        d.setTime(data[i]);
        data[i] = d;
        break;
      case 'Number':
        data[i] = Number(data[i]);
        break;
      case 'Boolean':
        data[i] = data[i] === 'true' || data[i] === '1';
        break;
      case 'String':
      case 'Text':
        break;
      default:
        d = data[i];
        try {
          data[i] = JSON.parse(data[i]);
        } catch (e) {
          data[i] = d;
        }
    }
  }
  return data;
};

BridgeToRedis.prototype.save = function(model, data, callback) {
  data = this.forDb(model, data);
  deleteNulls(data);
  this.client.hgetall(model + ':' + data.id, function(err, prevData) {
    if (err) return callback(err);
    this.client.hmset([model + ':' + data.id, data], function(err) {
      if (err) return callback(err);
      if (prevData) {
        Object.keys(prevData).forEach(function(k) {
          if (data.hasOwnProperty(k)) return;
          data[k] = prevData[k];
        });
      }
      this.updateIndexes(model, data.id, data, callback,
        this.forDb(model, prevData));
    }.bind(this));
  }.bind(this));
};

BridgeToRedis.prototype.updateIndexes =
function(model, id, data, callback, prevData) {
  var p = this._models[model].properties;
  var i = this.indexes[model];
  var schedule = [];
  if (!callback.removed) {
    schedule.push(['SADD', 's:' + model, id]);
  }
  Object.keys(i).forEach(function(key) {
    if (data.hasOwnProperty(key)) {
      var val = data[key];
      schedule.push([
        'SADD',
        'i:' + model + ':' + key + ':' + val,
        id,
      ]);
    }
    if (prevData && prevData[key] !== data[key]) {
      schedule.push([
        'SREM',
        'i:' + model + ':' + key + ':' + prevData[key],
        id,
      ]);
    }
  });

  if (schedule.length) {
    this.client.multi(schedule, function(err) {
      callback(err, data);
    });
  } else {
    callback(null);
  }
};

BridgeToRedis.prototype.create = function(model, data, callback) {
  if (data.id) return create.call(this, data.id, true);

  this.client.incr('id:' + model, function(err, id) {
    create.call(this, id);
  }.bind(this));

  function create(id, upsert) {
    data.id = id.toString();
    this.save(model, data, function(err) {
      if (callback) {
        callback(err, id);
      }
    });

    // push the id to the list of user ids for sorting
    this.client.sadd(['s:' + model, data.id]);
  }
};

BridgeToRedis.prototype.updateOrCreate = function(model, data, callback) {
  if (!data.id) {
    return this.create(model, data, callback);
  }

  var self = this;

  var client = this.client;
  this.save(model, data, function(error, obj) {
    var key = 'id:' + model;
    client.get(key, function(err, id) {
      if (!id && data.id) {
        client.set([key, data.id], ok);
      } else {
        ok();
      }
    });
    function ok() {
      callback(error, self.fromDb(model, obj));
    }
  });
};

BridgeToRedis.prototype.exists = function(model, id, callback) {
  this.client.exists(model + ':' + id, function(err, exists) {
    if (callback) {
      callback(err, exists);
    }
  });
};

BridgeToRedis.prototype.find = function find(model, id, callback) {
  this.client.hgetall(model + ':' + id, function(err, data) {
    if (data && Object.keys(data).length > 0) {
      data.id = id;
    } else {
      data = null;
    }
    callback(err, this.fromDb(model, data));
  }.bind(this));
};

BridgeToRedis.prototype.destroy = function destroy(model, id, callback) {
  var br = this;
  var tr = br.client.transaction();

  br.client.hgetall(model + ':' + id, function(err, data) {
    if (err) return callback(err);

    tr.srem(['s:' + model, id]);
    tr.del(model + ':' + id);
    tr.run(function(err) {
      if (err) return callback(err);
      callback.removed = true;

      br.updateIndexes(model, id, {}, callback, data);
    });
  });
};

BridgeToRedis.prototype.possibleIndexes = function(model, filter) {
  if (!filter || Object.keys(filter.where || {}).length === 0) return false;

  var foundIndex = [];
  var noIndex = [];
  Object.keys(filter.where).forEach(function(key) {
    var i = this.indexes[model][key];
    if (i) {
      var val = filter.where[key];
      if (val === null) {
        foundIndex.push('i:' + model + ':' + key + ':');
        return;
      }
      if (typeof val === 'object' && val.inq && val.inq.length) {
        val.inq.forEach(function(v) {
          foundIndex.push('i:' + model + ':' + key + ':' + v);
        });
        return;
      }
      if (i.name === 'Date') {
        if (val.getTime) {
          val = val.getTime();
        } else if (typeof val === 'number' || (typeof val === 'string' &&
            !isNaN(parseInt(val, 10)))) {
          val = parseInt(val, 10).toString();
        } else if (!val) {
          val = '0';
        }
      }
      foundIndex.push('i:' + model + ':' + key + ':' + val);
    } else {
      noIndex.push(key);
    }
  }.bind(this));

  return [foundIndex, noIndex];
};

BridgeToRedis.prototype.all = function all(model, filter, callback) {
  var ts = Date.now();
  var client = this.client;
  var cmd;
  var redis = this;
  var sortCmd = [];
  var props = this._models[model].properties;
  var allNumeric = true;
  var dest = 'temp' + (Date.now() * Math.random());
  var innerSetUsed = false;
  var trans = this.client.transaction();

  // fixes issue #44 where destroy all returns an error due to trying to execute
  // an empty where filter
  if (filter.where && Object.keys(filter.where).length === 0) {
    delete filter.where;
  }

  if (!filter) {
    filter = {order: 'id'};
  }

  // WHERE
  if (filter.where) {
    // special case: search by id
    if (filter.where.id) {
      if (filter.where.id.inq) {
        return handleKeys(null, filter.where.id.inq.map(function(id) {
          return model + ':' + id;
        }));
      } else {
        return handleKeys(null, [model + ':' + filter.where.id]);
      }
    }

    var pi = this.possibleIndexes(model, filter);
    var indexes = pi[0];
    var noIndexes = pi[1];

    if (noIndexes && noIndexes.length) {
      throw new Error(g.f('%s: no indexes found for %s ' +
            'impossible to sort and filter using {{redis}} connector',
            model, noIndexes.join(', ')));
    }

    if (indexes && indexes.length) {
      innerSetUsed = true;
      if (indexes.length > 1) {
        indexes.unshift(dest);
        trans.sinterstore(indexes);
      } else {
        dest = indexes[0];
      }
    } else {
      throw new Error(g.f('No indexes for %s', filter.where));
    }
  } else {
    dest = 's:' + model;
    // no filtering, just sort/limit (if any)
  }

  // only counting?
  if (filter.getCount) {
    trans.scard(dest, callback);
    return trans.run();
  }

  // ORDER
  var reverse = false;
  if (!filter.order) {
    filter.order = 'id';
  }
  var orders = filter.order;
  if (typeof filter.order === 'string') {
    orders = [filter.order];
  }

  orders.forEach(function(key) {
    var m = key.match(/\s+(A|DE)SC$/i);
    if (m) {
      key = key.replace(/\s+(A|DE)SC/i, '');
      if (m[1] === 'DE') reverse = true;
    }
    if (props[key].type.name !== 'Number' && props[key].type.name !== 'Date') {
      allNumeric = false;
    }
    sortCmd.push('BY', model + ':*->' + key);
  });

  // LIMIT
  if (filter.limit || filter.skip) {
    var offset = (filter.skip || 0);
    var limit = (filter.limit || -1);
    sortCmd.push('LIMIT', offset, limit);
  }

  // we need ALPHA modifier when sorting string values
  // the only case it's not required - we sort numbers
  if (!allNumeric) {
    sortCmd.push('ALPHA');
  }

  if (reverse) {
    sortCmd.push('DESC');
  }

  sortCmd.unshift(dest);
  sortCmd.push('GET', '#');
  cmd = 'SORT ' + sortCmd.join(' ');
  var ttt = Date.now();
  trans.sort(sortCmd, function(err, ids) {
    if (err) {
      return callback(err, []);
    }
    var sortedKeys = ids.map(function(i) {
      return model + ':' + i;
    });
    handleKeys(err, sortedKeys);
  });

  if (dest.match(/^temp/)) {
    trans.del(dest);
  }

  trans.run(callback);

  function handleKeys(err, keys) {
    var t2 = Date.now();
    var query = keys.map(function(key) {
      return ['hgetall', key];
    });
    client.multi(query, function(err, replies) {
      if (err) {
        return callback(err);
      }

      var objs = (replies || []).map(function(r) {
        return redis.fromDb(model, r, filter.fields);
      });

      if (filter && filter.include) {
        redis._models[model].model.include(objs, filter.include, callback);
      } else {
        callback(err, objs);
      }
    });
  }
};

BridgeToRedis.prototype.destroyAll =
function destroyAll(model, where, callback) {
  var br = this;
  if (!callback && 'function' === typeof where) {
    callback = where;
    where = undefined;
  }
  if (where) {
    this.all(model, {where: where}, function(err, results) {
      if (err || !results) {
        if (callback)
          callback(err, results);
        return;
      }
      var tasks = [];
      results.filter(Boolean).forEach(function(result) {
        tasks.push(function(done) { br.destroy(model, result.id, done); });
      });
      async.parallel(tasks, callback);
    });
  } else {
    this.client.multi([
        ['KEYS', model + ':*'],
        ['KEYS', '*:' + model + ':*'],
    ], function(err, k) {
      br.client.del(k[0].concat(k[1]).concat('s:' + model), callback);
    });
  }
};

BridgeToRedis.prototype.count = function count(model, callback, where) {
  if (where && Object.keys(where).length) {
    this.all(model, {where: where, getCount: true}, callback);
  } else {
    this.client.scard('s:' + model, callback);
  }
};

BridgeToRedis.prototype.updateAttributes =
function updateAttrs(model, id, data, cb) {
  data.id = id;
  this.save(model, data, cb);
};

function deleteNulls(data) {
  Object.keys(data).forEach(function(key) {
    if (data[key] === null) delete data[key];
  });
}

BridgeToRedis.prototype.disconnect = function disconnect(cb) {
  this.client.quit(cb);
};

BridgeToRedis.prototype.transaction = function() {
  throw new Error(g.f('not implemented'));
};
