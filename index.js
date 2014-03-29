'use strict';

var bops     = require('bops');
var bytewise = require('bytewise');

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');


var encode = function(data) {
  return bops.to(bytewise.encode(data), 'hex');
};


var decode = function(code) {
  return bytewise.decode(bops.from(code, 'hex'));
};


var indexKeys = function(value, indexer) {
  if (typeof indexer == 'function')
    return indexer(value);
  else
    return [value];
};


var collated = function(input, getSchema) {
  return cc.go(function*() {
    var result = {};

    yield chan.each(
      function(item) {
        var key = item.key[0];
        var val = item.key[1];
        if (!getSchema(key).multiple)
          result[key] = val;
        else if (result[key])
          result[key].push(val);
        else
          result[key] = [val];
      },
      input);

    return result;
  });
};


var addLog = function(batch, time, entity, attr, op) {
  var vals = Array.prototype.slice.call(arguments, 5);
  batch.put(encode(['log', time, entity, attr, op].concat(vals)), '');
};


var entriesFor = function(entity, attr, val, attrSchema) {
  var tmp = [
    ['eav', entity, attr, val],
    ['aev', attr, entity, val]];

  if (attrSchema.indexed)
    indexKeys(val, attrSchema.indexed).forEach(function(key) {
      tmp.push(['ave', attr, key, entity]);
    });
  if (attrSchema.reference)
    tmp.push(['vae', val, attr, entity]);

  return tmp.map(encode);
};


var removeDatum = function(batch, entity, attr, val, attrSchema, time, log) {
  if (log)
    addLog(batch, time, entity, attr, 'del', val);

  entriesFor(entity, attr, val, attrSchema).forEach(function(e) {
    batch.del(e);
  });
};


var putDatum = function(batch, entity, attr, val, old, attrSchema, time) {
  if (old === undefined)
    addLog(batch, time, entity, attr, 'add', val);
  else {
    removeDatum(batch, entity, attr, old, attrSchema, time, false);
    addLog(batch, time, entity, attr, 'chg', old, val);
  }

  entriesFor(entity, attr, val, attrSchema).forEach(function(e) {
    batch.put(e, time);
  });
};


module.exports = function(storage, schema) {
  schema = schema || {};

  return cc.go(function*() {
    var lock = chan.createLock();

    var scan = function(prefix, range, limit) {
      var n = prefix.length;
      var from = range ? range.from : null;
      var to   = range ? range.to : undefined;

      return cf.map(
        function(item) {
          return {
            key  : decode(item.key).slice(n),
            value: item.value
          }
        },
        storage.readRange({
          start: encode(prefix.concat(from)),
          end  : encode(prefix.concat(to)),
          limit: (limit == null ? -1 : limit)
        }));
    };

    var exists = function(entity, attribute, value) {
      return cc.go(function*() {
        var result = false;
        yield chan.each(
          function(item) {
            if (encode(item.key[0]) == encode(value)) result = true;
          },
          scan(['eav', entity, attribute], { from: value }, 1));
        return result;
      });
    };

    var values = function(entity, attribute) {
      return cc.go(function*() {
        var result = [];
        yield chan.each(
          function(item) { result.push(item.key[0]); },
          scan(['eav', entity, attribute]));
        return result;
      });
    };

    var nextTimestamp = function(batch) {
      return cc.go(function*() {
        var t = yield chan.pull(scan(['seq'], null, 1));
        var next = (t === undefined) ? -1 : t.key[0] - 1;
        batch.put(encode(['seq', next]), Date.now());
        return next;
      });
    };

    var atomically = function(action) {
      return cc.go(function*() {
        yield lock.acquire();
        var batch = storage.batch();
        yield cc.go(action, batch, yield nextTimestamp(batch));
        yield batch.write();
        lock.release();
      });
    };

    var attrSchema = function(key) {
      return schema[key] || {};
    };

    var removeData = function(batch, entity, attr, val, time) {
      var schema = attrSchema(attr);
      return cc.go(function*() {
        var a = (schema.multiple && Array.isArray(val)) ? val : [val];
        var i, v;
        for (i in a) {
          v = a[i];
          if (yield exists(entity, attr, v))
            removeDatum(batch, entity, attr, v, schema, time, true);
        }
      });
    };

    var putData = function(batch, entity, attr, val, time) {
      var schema = attrSchema(attr);
      return cc.go(function*() {
        var a = (schema.multiple && Array.isArray(val)) ? val : [val];
        var i, v, old;
        for (i in a) {
          v = a[i];
          if (!(yield exists(entity, attr, v))) {
            old = schema.multiple ? [] : (yield values(entity, attr));
            putDatum(batch, entity, attr, v, old[0], schema, time);
          }
        }
      });
    };

    return {
      close: storage.close,

      byEntity: function(entity) {
        return collated(scan(['eav', entity]), attrSchema);
      },

      references: function(entity) {
        return collated(scan(['vae', entity]),
                        function(_) { return { multiple: true }; });
      },

      byAttribute: function(key, range) {
        return cc.go(function*() {
          var data;
          if (range) {
            if (attrSchema(key).indexed)
              data = cf.map(
                function(item) {
                  return {
                    key  : [item.key[1], item.key[0]],
                    value: item.value
                  }
                },
                scan(['ave', key], range));
            else
              data = cf.filter(
                function(item) {
                  var val = item.key[1];
                  return val >= range.from && val <= range.to;
                },
                scan(['aev', key]));
          }
          else
            data = scan(['aev', key]);

          return yield collated(data, function(_) { return attrSchema(key); });
        });
      },

      updateEntity: function(entity, attr) {
        return atomically(function*(batch, time) {
          for (var key in attr)
            yield putData(batch, entity, key, attr[key], time);
        }.bind(this));
      },

      destroyEntity: function(entity) {
        return atomically(function*(batch, time) {
          var old = yield this.byEntity(entity);
          for (var key in old)
            yield removeData(batch, entity, key, old[key], time);

          yield chan.each(
            function(item) {
              var attr = item.key[0];
              var other = item.key[1];
              return removeData(batch, other, attr, entity, time);
            },
            scan(['vae', entity]));
        }.bind(this));
      },

      updateAttribute: function(key, assign) {
        return atomically(function*(batch, time) {
          for (var e in assign)
            yield putData(batch, e, key, assign[e], time);
        }.bind(this));
      },

      destroyAttribute: function(key) {
        return atomically(function*(batch, time) {
          var old = yield this.byAttribute(key);
          for (var e in old)
            yield removeData(batch, e, key, old[e], time);
        }.bind(this));
      },

      unlist: function(entity, attribute, values) {
        return atomically(function*(batch, time) {
          yield removeData(batch, entity, attribute, values, time);
        }.bind(this));
      },

      raw: function() {
        return scan([]);
      }
    };
  })
};
