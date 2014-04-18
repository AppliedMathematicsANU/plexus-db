'use strict';

var crypto = require('crypto');

var levelup  = require('levelup');
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


var sha1Hash = function(data) {
  return crypto.createHash('sha1')
    .update('blob ' + data.length + '\0')
    .update(data)
    .digest('hex');
};


var indexKeys = function(value, indexer) {
  if (typeof indexer == 'function')
    return indexer(value);
  else
    return [value];
};


var addLog = function(batch, time, entity, attr, op) {
  var vals = Array.prototype.slice.call(arguments, 5);
  batch.put(encode(['log', time, entity, attr, op].concat(vals)), '.');
};


var entriesFor = function(entity, attr, val, rawVal, attrSchema) {
  var tmp = [
    ['eav', entity, attr, val],
    ['aev', attr, entity, val]];

  if (attrSchema.indexed)
    indexKeys(rawVal, attrSchema.indexed).forEach(function(key) {
      tmp.push(['ave', attr, key, entity]);
    });
  if (attrSchema.reference)
    tmp.push(['vae', val, attr, entity]);

  return tmp.map(encode);
};


var scanOptions = function(prefix, range, limit) {
  var start;
  var end;

  if (range) {
    if (range.hasOwnProperty('value')) {
      start = prefix.concat(range.value, null);
      end   = prefix.concat(range.value, undefined);
    } else {
      start = range.hasOwnProperty('from')
        ? prefix.concat(range.from)
        : prefix;
      end = range.hasOwnProperty('to')
        ? prefix.concat(range.to)
        : prefix.concat(undefined);
    }
  } else {
    start = prefix;
    end   = prefix.concat(undefined);
  }

  return {
    start: encode(start),
    end  : encode(end),
    limit: (limit == null ? -1 : limit)
  };
};


module.exports = function(path, schema, options) {
  schema = schema || {};

  return cc.go(function*() {
    var db = yield cc.nbind(levelup)(path, options);
    var lock = chan.createLock();

    var scan = function(prefix, range, limit) {
      var stream = db.createReadStream(scanOptions(prefix, range, limit));

      return cf.map(
        function(item) {
          return {
            key  : decode(item.key).slice(prefix.length),
            value: item.value
          }
        },
        chan.fromStream(stream, null, stream.destroy.bind(stream))
      );
    };

    var resolveIndirect = function(val) {
      return cc.go(function*() {
        return JSON.parse(yield cc.nbind(db.get, db)(encode(['dat', val])));
      });
    };

    var encodeIndirect = function(val) {
      var text = JSON.stringify(val);
      return { text: text, hash: sha1Hash(text) };
    };

    var collated = function(input, getSchema) {
      return cc.go(function*() {
        var result = {};

        yield chan.each(
          function(item) {
            return cc.go(function*() {
              var key = item.key[0];
              var val = item.key[1];
              if (getSchema(key).indirect)
                val = yield resolveIndirect(val);
              if (!getSchema(key).multiple)
                result[key] = val;
              else if (result[key])
                result[key].push(val);
              else
                result[key] = [val];
            });
          },
          input);

        return result;
      });
    };

    var exists = function(entity, attribute, value) {
      return cc.go(function*() {
        var result = false;
        yield chan.each(
          function(item) { result = true; },
          scan(['eav', entity, attribute, value]));
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
        var batch;
        yield lock.acquire();
        try {
          batch = db.batch();
          yield cc.go(action, batch, yield nextTimestamp(batch));
          yield cc.nbind(batch.write, batch)();
        } finally {
          lock.release();
        }
      });
    };

    var attrSchema = function(key) {
      return schema[key] || {};
    };

    var removeData = function(batch, entity, attr, val, time) {
      var schema = attrSchema(attr);
      return cc.go(function*() {
        var a = (schema.multiple && Array.isArray(val)) ? val : [val];
        var i, raw, v;
        for (i in a) {
          raw = a[i];
          if (schema.indirect)
            v = encodeIndirect(raw).hash;
          else
            v = raw;
          if (yield exists(entity, attr, v)) {
            addLog(batch, time, entity, attr, 'del', v);

            entriesFor(entity, attr, v, raw, schema).forEach(function(e) {
              batch.del(e);
            });
          }
        }
      });
    };

    var putData = function(batch, entity, attr, val, time) {
      var schema = attrSchema(attr);
      return cc.go(function*() {
        var a = (schema.multiple && Array.isArray(val)) ? val : [val];
        var i, raw, v, old, tmp;
        for (i in a) {
          raw = a[i];
          if (schema.indirect) {
            tmp = encodeIndirect(raw);
            batch.put(encode(['dat', tmp.hash]), tmp.text);
            v = tmp.hash;
          }
          else
            v = raw;
          if (!(yield exists(entity, attr, v))) {
            old = schema.multiple ? undefined : (yield values(entity, attr))[0];

            if (old === undefined)
              addLog(batch, time, entity, attr, 'add', v);
            else {
              if (schema.indirect)
                raw = yield resolveIndirect(old);
              else
                raw = old;
              entriesFor(entity, attr, old, raw, schema).forEach(function(e) {
                batch.del(e);
              });
              addLog(batch, time, entity, attr, 'chg', old, v);
            }

            entriesFor(entity, attr, v, raw, schema).forEach(function(e) {
              batch.put(e, time);
            });
          }
        }
      });
    };

    var replay = function(group) {
      if (group.length == 0)
        return;

      return atomically(function*(batch, time) {
        var i, e;
        for (i in group) {
          e = group[i];
          if (e.operation == 'del')
            yield removeData(batch,
                             e.entity, e.attribute, e.values[0], time);
          else if (e.operation == 'add')
            yield putData(batch,
                          e.entity, e.attribute, e.values[0], time);
          else if (e.operation == 'chg')
            yield putData(batch,
                          e.entity, e.attribute, e.values[1], time);
        }
      });
    };

    return {
      close: cc.nbind(db.close, db),

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
            if (attrSchema(key).indexed) {
              data = cf.map(
                function(item) {
                  return {
                    key  : [item.key[1], item.key[0]],
                    value: item.value
                  }
                },
                scan(['ave', key], range));
            } else {
              data = cf.filter(
                function(item) {
                  var val = item.key[1];
                  return val >= range.from && val <= range.to;
                },
                scan(['aev', key]));
            }
          }
          else
            data = scan(['aev', key]);

          return yield collated(data, function(_) {
            return {
              multiple: attrSchema(key).multiple,
              indirect: (attrSchema(key).indirect &&
                         !(range && attrSchema(key).indexed))
            };
          });
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

      reverseLog: function() {
        return cc.go(function*() {
          var timestamp = {};
          yield chan.each(function(item) {
            timestamp[item.key] = item.value;
          }, scan(['seq']));

          return cf.map(
            function(item) {
              return cc.go(function*() {
                var data   = item.key;
                var time   = timestamp[data[0]];
                var entity = data[1];
                var attr   = data[2];
                var op     = data[3];
                var values = data.slice(4);

                if (attrSchema(attr).indirect)
                  values = yield cc.join(values.map(resolveIndirect));

                return {
                  timestamp: time,
                  entity   : entity,
                  attribute: attr,
                  operation: op,
                  values   : values
                };
              });
            },
            scan(['log']));
        });
      },

      replay: function(log) {
        return cc.go(function*() {
          var lastTime = 0;
          var i, e, group = [];

          for (i in log) {
            e = log[i];
            if (e.timestamp > lastTime) {
              yield replay(group);
              group = [];
              lastTime = e.timestamp;
            }
            group.push(e);
          };
          yield replay(group);
        });
      },

      raw: function() {
        return scan([]);
      }
    };
  })
};
