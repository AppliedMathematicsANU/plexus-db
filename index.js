'use strict';

var crypto = require('crypto');

var levelup  = require('levelup');
var bops     = require('bops');
var bytewise = require('bytewise');

var cc   = require('ceci-core');
var chan = require('ceci-channels');


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


function mapChannel(fn, ch) {
  var out = chan.chan();

  cc.go(wrapGenerator.mark(function() {
    var item;

    return wrapGenerator(function($ctx0) {
      while (1) switch ($ctx0.next) {
      case 0:
        $ctx0.next = 2;
        return chan.pull(ch);
      case 2:
        $ctx0.t0 = item = $ctx0.sent;

        if (!(undefined !== $ctx0.t0)) {
          $ctx0.next = 11;
          break;
        }

        $ctx0.next = 6;
        return fn(item);
      case 6:
        $ctx0.t1 = $ctx0.sent;
        $ctx0.next = 9;
        return chan.push(out, $ctx0.t1);
      case 9:
        $ctx0.next = 0;
        break;
      case 11:
        chan.close(out);
      case 12:
      case "end":
        return $ctx0.stop();
      }
    }, this);
  }));

  return out;
};


function filterChannel(fn, ch) {
  var out = chan.chan();

  cc.go(wrapGenerator.mark(function() {
    var item;

    return wrapGenerator(function($ctx1) {
      while (1) switch ($ctx1.next) {
      case 0:
        $ctx1.next = 2;
        return chan.pull(ch);
      case 2:
        $ctx1.t2 = item = $ctx1.sent;

        if (!(undefined !== $ctx1.t2)) {
          $ctx1.next = 11;
          break;
        }

        $ctx1.next = 6;
        return fn(item);
      case 6:
        if (!$ctx1.sent) {
          $ctx1.next = 9;
          break;
        }

        $ctx1.next = 9;
        return chan.push(out, item);
      case 9:
        $ctx1.next = 0;
        break;
      case 11:
        chan.close(out);
      case 12:
      case "end":
        return $ctx1.stop();
      }
    }, this);
  }));

  return out;
};


module.exports = function(path, schema, options) {
  schema = schema || {};

  return cc.go(wrapGenerator.mark(function() {
    var db, lock, scan, resolveIndirect, encodeIndirect, collated, exists, values, nextTimestamp, atomically, attrSchema, removeData, putData, replay;

    return wrapGenerator(function($ctx2) {
      while (1) switch ($ctx2.next) {
      case 0:
        $ctx2.next = 2;
        return cc.nbind(levelup)(path, options);
      case 2:
        db = $ctx2.sent;
        lock = chan.createLock();

        scan = function(prefix, range, limit) {
          var stream = db.createReadStream(scanOptions(prefix, range, limit));

          return mapChannel(
            function(item) {
              return {
                key  : decode(item.key).slice(prefix.length),
                value: item.value
              }
            },
            chan.fromStream(stream, null, stream.destroy.bind(stream))
          );
        };

        resolveIndirect = function(val) {
          return cc.go(wrapGenerator.mark(function() {
            return wrapGenerator(function($ctx3) {
              while (1) switch ($ctx3.next) {
              case 0:
                $ctx3.next = 2;
                return cc.nbind(db.get, db)(encode(['dat', val]));
              case 2:
                $ctx3.t3 = $ctx3.sent;
                $ctx3.rval = JSON.parse($ctx3.t3);
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

        encodeIndirect = function(val) {
          var text = JSON.stringify(val);
          return { text: text, hash: sha1Hash(text) };
        };

        collated = function(input, getSchema) {
          return cc.go(wrapGenerator.mark(function() {
            var result;

            return wrapGenerator(function($ctx4) {
              while (1) switch ($ctx4.next) {
              case 0:
                result = {};
                $ctx4.next = 3;

                return chan.each(
                  function(item) {
                    return cc.go(wrapGenerator.mark(function() {
                      var key, val;

                      return wrapGenerator(function($ctx5) {
                        while (1) switch ($ctx5.next) {
                        case 0:
                          key = item.key[0];
                          val = item.key[1];

                          if (!getSchema(key).indirect) {
                            $ctx5.next = 6;
                            break;
                          }

                          $ctx5.next = 5;
                          return resolveIndirect(val);
                        case 5:
                          val = $ctx5.sent;
                        case 6:
                          if (!getSchema(key).multiple)
                            result[key] = val;
                          else if (result[key])
                            result[key].push(val);
                          else
                            result[key] = [val];
                        case 7:
                        case "end":
                          return $ctx5.stop();
                        }
                      }, this);
                    }));
                  },
                  input);
              case 3:
                $ctx4.rval = result;
                delete $ctx4.thrown;
                $ctx4.next = 7;
                break;
              case 7:
              case "end":
                return $ctx4.stop();
              }
            }, this);
          }));
        };

        exists = function(entity, attribute, value) {
          return cc.go(wrapGenerator.mark(function() {
            var result;

            return wrapGenerator(function($ctx6) {
              while (1) switch ($ctx6.next) {
              case 0:
                result = false;
                $ctx6.next = 3;

                return chan.each(
                  function(item) { result = true; },
                  scan(['eav', entity, attribute, value]));
              case 3:
                $ctx6.rval = result;
                delete $ctx6.thrown;
                $ctx6.next = 7;
                break;
              case 7:
              case "end":
                return $ctx6.stop();
              }
            }, this);
          }));
        };

        values = function(entity, attribute) {
          return cc.go(wrapGenerator.mark(function() {
            var result;

            return wrapGenerator(function($ctx7) {
              while (1) switch ($ctx7.next) {
              case 0:
                result = [];
                $ctx7.next = 3;

                return chan.each(
                  function(item) { result.push(item.key[0]); },
                  scan(['eav', entity, attribute]));
              case 3:
                $ctx7.rval = result;
                delete $ctx7.thrown;
                $ctx7.next = 7;
                break;
              case 7:
              case "end":
                return $ctx7.stop();
              }
            }, this);
          }));
        };

        nextTimestamp = function(batch) {
          return cc.go(wrapGenerator.mark(function() {
            var t, next;

            return wrapGenerator(function($ctx8) {
              while (1) switch ($ctx8.next) {
              case 0:
                $ctx8.next = 2;
                return chan.pull(scan(['seq'], null, 1));
              case 2:
                t = $ctx8.sent;
                next = (t === undefined) ? -1 : t.key[0] - 1;
                batch.put(encode(['seq', next]), Date.now());
                $ctx8.rval = next;
                delete $ctx8.thrown;
                $ctx8.next = 9;
                break;
              case 9:
              case "end":
                return $ctx8.stop();
              }
            }, this);
          }));
        };

        atomically = function(action) {
          return cc.go(wrapGenerator.mark(function() {
            var batch;

            return wrapGenerator(function($ctx9) {
              while (1) switch ($ctx9.next) {
              case 0:
                $ctx9.next = 2;
                return lock.acquire();
              case 2:
                $ctx9.t4 = 16;
                $ctx9.pushTry(null, 12, "t4");
                batch = db.batch();
                $ctx9.next = 7;
                return nextTimestamp(batch);
              case 7:
                $ctx9.t5 = $ctx9.sent;
                $ctx9.next = 10;
                return cc.go(action, batch, $ctx9.t5);
              case 10:
                $ctx9.next = 12;
                return cc.nbind(batch.write, batch)();
              case 12:
                $ctx9.popFinally(12);
                lock.release();
                $ctx9.next = $ctx9.t4;
                break;
              case 16:
              case "end":
                return $ctx9.stop();
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
            var a, i, raw, v;

            return wrapGenerator(function($ctx10) {
              while (1) switch ($ctx10.next) {
              case 0:
                a = (schema.multiple && Array.isArray(val)) ? val : [val];
                $ctx10.t6 = $ctx10.keys(a);
              case 2:
                if (!$ctx10.t6.length) {
                  $ctx10.next = 13;
                  break;
                }

                i = $ctx10.t6.pop();
                raw = a[i];

                if (schema.indirect)
                  v = encodeIndirect(raw).hash;
                else
                  v = raw;

                $ctx10.next = 8;
                return exists(entity, attr, v);
              case 8:
                if (!$ctx10.sent) {
                  $ctx10.next = 11;
                  break;
                }

                addLog(batch, time, entity, attr, 'del', v);

                entriesFor(entity, attr, v, raw, schema).forEach(function(e) {
                  batch.del(e);
                });
              case 11:
                $ctx10.next = 2;
                break;
              case 13:
              case "end":
                return $ctx10.stop();
              }
            }, this);
          }));
        };

        putData = function(batch, entity, attr, val, time) {
          var schema = attrSchema(attr);
          return cc.go(wrapGenerator.mark(function() {
            var a, i, raw, v, old, oRaw, tmp;

            return wrapGenerator(function($ctx11) {
              while (1) switch ($ctx11.next) {
              case 0:
                a = (schema.multiple && Array.isArray(val)) ? val : [val];
                $ctx11.t7 = $ctx11.keys(a);
              case 2:
                if (!$ctx11.t7.length) {
                  $ctx11.next = 33;
                  break;
                }

                i = $ctx11.t7.pop();
                raw = a[i];

                if (schema.indirect) {
                  tmp = encodeIndirect(raw);
                  batch.put(encode(['dat', tmp.hash]), tmp.text);
                  v = tmp.hash;
                }
                else
                  v = raw;

                $ctx11.next = 8;
                return exists(entity, attr, v);
              case 8:
                if (!!$ctx11.sent) {
                  $ctx11.next = 31;
                  break;
                }

                if (!schema.multiple) {
                  $ctx11.next = 13;
                  break;
                }

                $ctx11.t8 = undefined;
                $ctx11.next = 16;
                break;
              case 13:
                $ctx11.next = 15;
                return values(entity, attr);
              case 15:
                $ctx11.t8 = $ctx11.sent[0];
              case 16:
                old = $ctx11.t8;

                if (!(old === undefined)) {
                  $ctx11.next = 21;
                  break;
                }

                addLog(batch, time, entity, attr, 'add', v);
                $ctx11.next = 30;
                break;
              case 21:
                if (!schema.indirect) {
                  $ctx11.next = 27;
                  break;
                }

                $ctx11.next = 24;
                return resolveIndirect(old);
              case 24:
                oRaw = $ctx11.sent;
                $ctx11.next = 28;
                break;
              case 27:
                oRaw = old;
              case 28:
                entriesFor(entity, attr, old, oRaw, schema).forEach(function(e) {
                  batch.del(e);
                });

                addLog(batch, time, entity, attr, 'chg', old, v);
              case 30:
                entriesFor(entity, attr, v, raw, schema).forEach(function(e) {
                  batch.put(e, time);
                });
              case 31:
                $ctx11.next = 2;
                break;
              case 33:
              case "end":
                return $ctx11.stop();
              }
            }, this);
          }));
        };

        replay = function(group) {
          if (group.length == 0)
            return;

          return atomically(wrapGenerator.mark(function(batch, time) {
            var i, e;

            return wrapGenerator(function($ctx12) {
              while (1) switch ($ctx12.next) {
              case 0:
                $ctx12.t9 = $ctx12.keys(group);
              case 1:
                if (!$ctx12.t9.length) {
                  $ctx12.next = 19;
                  break;
                }

                i = $ctx12.t9.pop();
                e = group[i];

                if (!(e.operation == 'del')) {
                  $ctx12.next = 9;
                  break;
                }

                $ctx12.next = 7;

                return removeData(batch,
                                 e.entity, e.attribute, e.values[0], time);
              case 7:
                $ctx12.next = 17;
                break;
              case 9:
                if (!(e.operation == 'add')) {
                  $ctx12.next = 14;
                  break;
                }

                $ctx12.next = 12;

                return putData(batch,
                              e.entity, e.attribute, e.values[0], time);
              case 12:
                $ctx12.next = 17;
                break;
              case 14:
                if (!(e.operation == 'chg')) {
                  $ctx12.next = 17;
                  break;
                }

                $ctx12.next = 17;

                return putData(batch,
                              e.entity, e.attribute, e.values[1], time);
              case 17:
                $ctx12.next = 1;
                break;
              case 19:
              case "end":
                return $ctx12.stop();
              }
            }, this);
          }));
        };

        $ctx2.rval = {
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

              return wrapGenerator(function($ctx13) {
                while (1) switch ($ctx13.next) {
                case 0:
                  if (range) {
                    if (attrSchema(key).indexed) {
                      data = mapChannel(
                        function(item) {
                          return {
                            key  : [item.key[1], item.key[0]],
                            value: item.value
                          }
                        },
                        scan(['ave', key], range));
                    } else {
                      data = filterChannel(
                        function(item) {
                          var val = item.key[1];
                          return val >= range.from && val <= range.to;
                        },
                        scan(['aev', key]));
                    }
                  }
                  else
                    data = scan(['aev', key]);

                  $ctx13.next = 3;

                  return collated(data, function(_) {
                    return {
                      multiple: attrSchema(key).multiple,
                      indirect: (attrSchema(key).indirect &&
                                 !(range && attrSchema(key).indexed))
                    };
                  });
                case 3:
                  $ctx13.rval = $ctx13.sent;
                  delete $ctx13.thrown;
                  $ctx13.next = 7;
                  break;
                case 7:
                case "end":
                  return $ctx13.stop();
                }
              }, this);
            }));
          },

          updateEntity: function(entity, attr) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var key;

              return wrapGenerator(function($ctx14) {
                while (1) switch ($ctx14.next) {
                case 0:
                  $ctx14.t10 = $ctx14.keys(attr);
                case 1:
                  if (!$ctx14.t10.length) {
                    $ctx14.next = 7;
                    break;
                  }

                  key = $ctx14.t10.pop();
                  $ctx14.next = 5;
                  return putData(batch, entity, key, attr[key], time);
                case 5:
                  $ctx14.next = 1;
                  break;
                case 7:
                case "end":
                  return $ctx14.stop();
                }
              }, this);
            }).bind(this));
          },

          destroyEntity: function(entity) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var old, key;

              return wrapGenerator(function($ctx15) {
                while (1) switch ($ctx15.next) {
                case 0:
                  $ctx15.next = 2;
                  return this.byEntity(entity);
                case 2:
                  old = $ctx15.sent;
                  $ctx15.t11 = $ctx15.keys(old);
                case 4:
                  if (!$ctx15.t11.length) {
                    $ctx15.next = 10;
                    break;
                  }

                  key = $ctx15.t11.pop();
                  $ctx15.next = 8;
                  return removeData(batch, entity, key, old[key], time);
                case 8:
                  $ctx15.next = 4;
                  break;
                case 10:
                  $ctx15.next = 12;

                  return chan.each(
                    function(item) {
                      var attr = item.key[0];
                      var other = item.key[1];
                      return removeData(batch, other, attr, entity, time);
                    },
                    scan(['vae', entity]));
                case 12:
                case "end":
                  return $ctx15.stop();
                }
              }, this);
            }).bind(this));
          },

          updateAttribute: function(key, assign) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var e;

              return wrapGenerator(function($ctx16) {
                while (1) switch ($ctx16.next) {
                case 0:
                  $ctx16.t12 = $ctx16.keys(assign);
                case 1:
                  if (!$ctx16.t12.length) {
                    $ctx16.next = 7;
                    break;
                  }

                  e = $ctx16.t12.pop();
                  $ctx16.next = 5;
                  return putData(batch, e, key, assign[e], time);
                case 5:
                  $ctx16.next = 1;
                  break;
                case 7:
                case "end":
                  return $ctx16.stop();
                }
              }, this);
            }).bind(this));
          },

          destroyAttribute: function(key) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              var old, e;

              return wrapGenerator(function($ctx17) {
                while (1) switch ($ctx17.next) {
                case 0:
                  $ctx17.next = 2;
                  return this.byAttribute(key);
                case 2:
                  old = $ctx17.sent;
                  $ctx17.t13 = $ctx17.keys(old);
                case 4:
                  if (!$ctx17.t13.length) {
                    $ctx17.next = 10;
                    break;
                  }

                  e = $ctx17.t13.pop();
                  $ctx17.next = 8;
                  return removeData(batch, e, key, old[e], time);
                case 8:
                  $ctx17.next = 4;
                  break;
                case 10:
                case "end":
                  return $ctx17.stop();
                }
              }, this);
            }).bind(this));
          },

          unlist: function(entity, attribute, values) {
            return atomically(wrapGenerator.mark(function(batch, time) {
              return wrapGenerator(function($ctx18) {
                while (1) switch ($ctx18.next) {
                case 0:
                  $ctx18.next = 2;
                  return removeData(batch, entity, attribute, values, time);
                case 2:
                case "end":
                  return $ctx18.stop();
                }
              }, this);
            }).bind(this));
          },

          reverseLog: function() {
            return cc.go(wrapGenerator.mark(function() {
              var timestamp;

              return wrapGenerator(function($ctx19) {
                while (1) switch ($ctx19.next) {
                case 0:
                  timestamp = {};
                  $ctx19.next = 3;

                  return chan.each(function(item) {
                    timestamp[item.key] = item.value;
                  }, scan(['seq']));
                case 3:
                  $ctx19.rval = mapChannel(
                    function(item) {
                      return cc.go(wrapGenerator.mark(function() {
                        var data, time, entity, attr, op, values;

                        return wrapGenerator(function($ctx20) {
                          while (1) switch ($ctx20.next) {
                          case 0:
                            data = item.key;
                            time = timestamp[data[0]];
                            entity = data[1];
                            attr = data[2];
                            op = data[3];
                            values = data.slice(4);

                            if (!attrSchema(attr).indirect) {
                              $ctx20.next = 10;
                              break;
                            }

                            $ctx20.next = 9;
                            return cc.join(values.map(resolveIndirect));
                          case 9:
                            values = $ctx20.sent;
                          case 10:
                            $ctx20.rval = {
                              timestamp: time,
                              entity   : entity,
                              attribute: attr,
                              operation: op,
                              values   : values
                            };

                            delete $ctx20.thrown;
                            $ctx20.next = 14;
                            break;
                          case 14:
                          case "end":
                            return $ctx20.stop();
                          }
                        }, this);
                      }));
                    },
                    scan(['log']));

                  delete $ctx19.thrown;
                  $ctx19.next = 7;
                  break;
                case 7:
                case "end":
                  return $ctx19.stop();
                }
              }, this);
            }));
          },

          replay: function(log) {
            return cc.go(wrapGenerator.mark(function() {
              var lastTime, i, e, group;

              return wrapGenerator(function($ctx21) {
                while (1) switch ($ctx21.next) {
                case 0:
                  lastTime = 0;
                  group = [];
                  $ctx21.t14 = $ctx21.keys(log);
                case 3:
                  if (!$ctx21.t14.length) {
                    $ctx21.next = 14;
                    break;
                  }

                  i = $ctx21.t14.pop();
                  e = log[i];

                  if (!(e.timestamp > lastTime)) {
                    $ctx21.next = 11;
                    break;
                  }

                  $ctx21.next = 9;
                  return replay(group);
                case 9:
                  group = [];
                  lastTime = e.timestamp;
                case 11:
                  group.push(e);
                  $ctx21.next = 3;
                  break;
                case 14:
                  $ctx21.next = 17;
                  return replay(group);
                case 17:
                case "end":
                  return $ctx21.stop();
                }
              }, this);
            }));
          },

          raw: function() {
            return scan([]);
          }
        };

        delete $ctx2.thrown;
        $ctx2.next = 20;
        break;
      case 20:
      case "end":
        return $ctx2.stop();
      }
    }, this);
  }))
};
