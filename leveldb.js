'use strict';

var levelup = require('levelup');
var cc      = require('ceci-core');
var chan    = require('ceci-channels');


var merge = function(obj1, obj2) {
  var result = {};
  var key;
  for (key in obj1)
    result[key] = obj1[key];
  for (key in obj2)
    result[key] = obj2[key];
  return result;
};


module.exports = function(path, options) {
  return cc.go(function*() {
    options = merge({ valueEncoding: 'json' }, options)
    var db = yield cc.nbind(levelup)(path, options);

    try {
      return {
        read: function(key) {
          var result = cc.defer();

          db.get(key, function(err, val) {
            if (!err)
              result.resolve(val);
            else if (err.notFound)
              result.resolve();
            else
              result.reject(err);
          });

          return result;
        },

        batch: function(ops) {
          if (ops)
            return cc.nbind(db.batch, db)(ops)
          else {
            var batch = db.batch();
            batch.write = cc.nbind(batch.write, batch);
            return batch;
          }
        },

        readRange: function(options) {
          var stream = db.createReadStream(options);
          return chan.fromStream(stream, null, stream.destroy.bind(stream));
        },

        write  : cc.nbind(db.put, db),
        destroy: cc.nbind(db.del, db),
        close  : cc.nbind(db.close, db)
      }
    } catch(ex) {
      yield cc.nbind(db.close, db)();
      throw new Error(ex);
    }
  });
};
