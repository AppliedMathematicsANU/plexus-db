'use strict';

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


var indexKeys = function(value, indexer) {
  if (typeof indexer == 'function')
    return indexer(value);
  else
    return [value];
};


var collated = function(input, getSchema) {
  return cc.go(wrapGenerator.mark(function() {
    var result;

    return wrapGenerator(function($ctx0) {
      while (1) switch ($ctx0.next) {
      case 0:
        result = {};
        $ctx0.next = 3;

        return chan.each(
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
          input)
      case 3:
        $ctx0.rval = result;
        delete $ctx0.thrown;
        $ctx0.next = 7;
        break;
      case 7:
      case "end":
        return $ctx0.stop();
      }
    }, this);
  }));
};


var addLog = function(batch, time, entity, attr, op) {
  var vals = Array.prototype.slice.call(arguments, 5);
  batch.put(encode(['log', time, entity, attr, op].concat(vals)), '.');
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

  return cc.go(wrapGenerator.mark(function() {
    var db, lock, scan, exists, values, nextTimestamp, atomically, attrSchema, removeData, putData;

    return wrapGenerator(function($ctx1) {
      while (1) switch ($ctx1.next) {
      case 0:
        $ctx1.next = 2;
        return cc.nbind(levelup)(path, options);
      case 2:
        db = $ctx1.sent;
        lock = chan.createLock();

        scan = function(prefix, range, limit) {
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

        exists = function(entity, attribute, value) {
          return cc.go(wrapGenerator.mark(function() {
            var result;

            return wrapGenerator(function($ctx2) {
              while (1) switch ($ctx2.next) {
              case 0:
                result = false;
                $ctx2.next = 3;

                return chan.each(
                  function(item) { result = true; },
                  scan(['eav', entity, attribute, value]))
              case 3:
                $ctx2.rval = result;
                delete $ctx2.thrown;
                $ctx2.next = 7;
                break;
              case 7:
              case "end":
                return $ctx2.stop();
              }
            }, this);
          }));
        };

        values = function(entity, attribute) {
          return cc.go(wrapGenerator.mark(function() {
            var result;

            return wrapGenerator(function($ctx3) {
              while (1) switch ($ctx3.next) {
              case 0:
                result = [];
                $ctx3.next = 3;

                return chan.each(
                  function(item) { result.push(item.key[0]); },
                  scan(['eav', entity, attribute]))
              case 3:
                $ctx3.rval = result;
                delete $ctx3.thrown;
                $ctx3.next = 7;
                break;
              case 7:
              case "end":
                return $ctx3.stop();
              }
            }, this);
          }));
        };

        nextTimestamp = function(batch) {
          return cc.go(wrapGenerator.mark(function() {
            var t, next;

            return wrapGenerator(function($ctx4) {
              while (1) switch ($ctx4.next) {
              case 0:
                $ctx4.next = 2;
                return chan.pull(scan(['seq'], null, 1));
              case 2:
                t = $ctx4.sent;
                next = (t === undefined) ? -1 : t.key[0] - 1;
                batch.put(encode(['seq', next]), Date.now());
                $ctx4.rval = next;
                delete $ctx4.thrown;
                $ctx4.next = 9;
                break;
              case 9:
              case "end":
                return $ctx4.stop();
              }
            }, this);
          }));
        };

        atomically = function(action) {
          return cc.go(wrapGenerator.mark(function() {
            var batch;

            return wrapGenerator(function($ctx5) {
              while (1) switch ($ctx5.next) {
              case 0:
                $ctx5.next = 2;
                return lock.acquire();
              case 2:
                $ctx5.t0 = 16;
                $ctx5.pushTry(null, 12, "t0");
                batch = db.batch();
                $ctx5.next = 7;
                return nextTimestamp(batch);
              case 7:
                $ctx5.t1 = $ctx5.sent;
                $ctx5.next = 10;
                return cc.go(action, batch, $ctx5.t1);
              case 10:
                $ctx5.next = 12;
                return cc.nbind(batch.write, batch)();
              case 12:
                $ctx5.popFinally(12);
                lock.release();
                $ctx5.next = $ctx5.t0;
                break;
              case 16:
              case "end":
                return $ctx5.stop();
              }
            }, this);
          }));
        };

        attrSchema = function(key) {
          return schema[key] || {};
        };

        removeData = function(batch, entity, attr, val, time) {
          var schema = attrSchema(attr);
          return cc.go(wrapGenerator.mark(function() {
            var a, i, v;

            return wrapGenerator(function($ctx6) {
              while (1) switch ($ctx6.next) {
              case 0:
                a = (schema.multiple && Array.isArray(val)) ? val : [val];
                $ctx6.t2 = $ctx6.keys(a);
              case 2:
                if (!$ctx6.t2.length) {
                  $ctx6.next = 11;
                  break;
                }

                i = $ctx6.t2.pop();
                v = a[i];
                $ctx6.next = 7;
                return exists(entity, attr, v);
              case 7:
                if (!$ctx6.sent) {
                  $ctx6.next = 9;
                  break;
                }

                removeDatum(batch, entity, attr, v, schema, time, true);
              case 9:
                $ctx6.next = 2;
                break;
              case 11:
              case "end":
                return $ctx6.stop();
              }
            }, this);
          }));
        };

        putData = function(batch, entity, attr, val, time) {
          var schema = attrSchema(attr);
          return cc.go(wrapGenerator.mark(function() {
            var a, i, v, old;

            return wrapGenerator(function($ctx7) {
              while (1) switch ($ctx7.next) {
              case 0:
                a = (schema.multiple && Array.isArray(val)) ? val : [val];
                $ctx7.t3 = $ctx7.keys(a);
              case 2:
                if (!$ctx7.t3.length) {
                  $ctx7.next = 19;
                  break;
                }

                i = $ctx7.t3.pop();
                v = a[i];
                $ctx7.next = 7;
                return exists(entity, attr, v);
              case 7:
                if (!!$ctx7.sent) {
                  $ctx7.next = 17;
                  break;
                }

                if (!schema.multiple) {
                  $ctx7.next = 12;
                  break;
                }

                $ctx7.t4 = [];
                $ctx7.next = 15;
                break;
              case 12:
                $ctx7.next = 14;
                return values(entity, attr);
              case 14:
                $ctx7.t4 = $ctx7.sent;
              case 15:
                old = $ctx7.t4;
                putDatum(batch, entity, attr, v, old[0], schema, time);
              case 17:
                $ctx7.next = 2;
                break;
              case 19:
              case "end":
                return $ctx7.stop();
              }
            }, this);
          }));
        };

        $ctx1.rval = {
          close: cc.nbind(db.close, db),

          byEntity: function(entity) {
            return collated(scan(['eav', entity]), attrSchema);
          },

          references: function(entity) {
            return collated(scan(['vae', entity]),
                            function(_) { return { multiple: true }; });
          },

          byAttribute: function(key, range) {
            return cc.go(wrapGenerator.mark(function() {
              var data;

              return wrapGenerator(function($ctx8) {
                while (1) switch ($ctx8.next) {
                case 0:
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

                  $ctx8.next = 3;
                  return collated(data, function(_) { return attrSchema(key); });
                case 3:
                  $ctx8.rval = $ctx8.sent;
                  delete $ctx8.thrown;
                  $ctx8.next = 7;
                  break;
                case 7:
                case "end":
                  return $ctx8.stop();
                }
              }, this);
            }));
          },

          updateEntity: function(entity, attr) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var key;

              return wrapGenerator(function($ctx9) {
                while (1) switch ($ctx9.next) {
                case 0:
                  $ctx9.t5 = $ctx9.keys(attr);
                case 1:
                  if (!$ctx9.t5.length) {
                    $ctx9.next = 7;
                    break;
                  }

                  key = $ctx9.t5.pop();
                  $ctx9.next = 5;
                  return putData(batch, entity, key, attr[key], time);
                case 5:
                  $ctx9.next = 1;
                  break;
                case 7:
                case "end":
                  return $ctx9.stop();
                }
              }, this);
            }).bind(this));
          },

          destroyEntity: function(entity) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var old, key;

              return wrapGenerator(function($ctx10) {
                while (1) switch ($ctx10.next) {
                case 0:
                  $ctx10.next = 2;
                  return this.byEntity(entity);
                case 2:
                  old = $ctx10.sent;
                  $ctx10.t6 = $ctx10.keys(old);
                case 4:
                  if (!$ctx10.t6.length) {
                    $ctx10.next = 10;
                    break;
                  }

                  key = $ctx10.t6.pop();
                  $ctx10.next = 8;
                  return removeData(batch, entity, key, old[key], time);
                case 8:
                  $ctx10.next = 4;
                  break;
                case 10:
                  $ctx10.next = 12;

                  return chan.each(
                    function(item) {
                      var attr = item.key[0];
                      var other = item.key[1];
                      return removeData(batch, other, attr, entity, time);
                    },
                    scan(['vae', entity]))
                case 12:
                case "end":
                  return $ctx10.stop();
                }
              }, this);
            }).bind(this));
          },

          updateAttribute: function(key, assign) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var e;

              return wrapGenerator(function($ctx11) {
                while (1) switch ($ctx11.next) {
                case 0:
                  $ctx11.t7 = $ctx11.keys(assign);
                case 1:
                  if (!$ctx11.t7.length) {
                    $ctx11.next = 7;
                    break;
                  }

                  e = $ctx11.t7.pop();
                  $ctx11.next = 5;
                  return putData(batch, e, key, assign[e], time);
                case 5:
                  $ctx11.next = 1;
                  break;
                case 7:
                case "end":
                  return $ctx11.stop();
                }
              }, this);
            }).bind(this));
          },

          destroyAttribute: function(key) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var old, e;

              return wrapGenerator(function($ctx12) {
                while (1) switch ($ctx12.next) {
                case 0:
                  $ctx12.next = 2;
                  return this.byAttribute(key);
                case 2:
                  old = $ctx12.sent;
                  $ctx12.t8 = $ctx12.keys(old);
                case 4:
                  if (!$ctx12.t8.length) {
                    $ctx12.next = 10;
                    break;
                  }

                  e = $ctx12.t8.pop();
                  $ctx12.next = 8;
                  return removeData(batch, e, key, old[e], time);
                case 8:
                  $ctx12.next = 4;
                  break;
                case 10:
                case "end":
                  return $ctx12.stop();
                }
              }, this);
            }).bind(this));
          },

          unlist: function(entity, attribute, values) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              return wrapGenerator(function($ctx13) {
                while (1) switch ($ctx13.next) {
                case 0:
                  $ctx13.next = 2;
                  return removeData(batch, entity, attribute, values, time);
                case 2:
                case "end":
                  return $ctx13.stop();
                }
              }, this);
            }).bind(this));
          },

          log: function() {
            return cc.go(wrapGenerator.mark(function() {
              var timestamp;

              return wrapGenerator(function($ctx14) {
                while (1) switch ($ctx14.next) {
                case 0:
                  timestamp = {};
                  $ctx14.next = 3;

                  return chan.each(function(item) {
                    timestamp[item.key] = item.value;
                  }, scan(['seq']))
                case 3:
                  $ctx14.rval = cf.map(
                    function(item) {
                      var data = item.key;
                      return {
                        timestamp: timestamp[data[0]],
                        entity   : data[1],
                        attribute: data[2],
                        operation: data[3],
                        values   : data.slice(4)
                      };
                    },
                    scan(['log']));

                  delete $ctx14.thrown;
                  $ctx14.next = 7;
                  break;
                case 7:
                case "end":
                  return $ctx14.stop();
                }
              }, this);
            }));
          },

          raw: function() {
            return scan([]);
          }
        };

        delete $ctx1.thrown;
        $ctx1.next = 16;
        break;
      case 16:
      case "end":
        return $ctx1.stop();
      }
    }, this);
  }))
};
