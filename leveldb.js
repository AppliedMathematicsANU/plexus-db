'use strict';

var levelup = require('levelup');
var cc      = require('ceci-core');
var chan    = require('ceci-channels');


module.exports = function(path, options) {
  return cc.go(function*() {
    var db = yield cc.nbind(levelup)(path, options);

    return {
      batch: function() {
        var batch = db.batch();
        batch.write = cc.nbind(batch.write, batch);
        return batch;
      },

      read: function(options) {
        var stream = db.createReadStream(options);
        return chan.fromStream(stream, null, stream.destroy.bind(stream));
      },

      close: cc.nbind(db.close, db)
    }
  });
};
