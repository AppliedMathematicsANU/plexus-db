'use strict';

var cc     = require('ceci-core');
var chan   = require('ceci-channels');
var engine = require('plexus-db');


var schema = {
  type         : { indexed: true },
  organisations: { multiple: true },
  lithologies  : { multiple: true },
  project      : { reference: true },
  user         : { reference: true },
  well         : { reference: true },
  name: {
    indexed: function(text) {
      return [text.trim().toLowerCase()];
    }
  },
  notes: {
    multiple: true,
    indirect: true,  // indirect values are not implemented yet
    indexed : function(text) {
      return text.trim().toLowerCase().split(/\s*\b/);
    }
  }
};


var dump = function(db) {
  return chan.each(
    function(e) {
      console.log(JSON.stringify(e.key) + ':', JSON.stringify(e.value));
    },
    db.raw());
};


cc.top(cc.go(function*() {
  var dbIn  = yield engine(process.argv[2], schema);
  var dbOut = yield engine(process.argv[3], schema);
  var log = [];

  yield dump(dbIn);

  yield chan.each(function(entry) { log.push(entry); },
                  yield dbIn.reverseLog());
  log.reverse();
  dbIn.close();

  yield dbOut.replay(log.filter(function(e) {
    return e.operation != 'chg' || typeof e.values[1] == 'string';
  }));

  yield dump(dbOut);

  dbOut.close();
}));
