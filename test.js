'use strict';

var memdown  = require('memdown');

var cc     = require('ceci-core');
var chan   = require('ceci-channels');
var engine = require('./index');


var formatEntity = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.byEntity(key)) || null;
    tmp[key].references = (yield db.references(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};

var formatAttribute = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.byAttribute(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};


var show = function(db, entities, attributes) {
  return cc.go(function*() {
    var i;

    for (i in entities)
      console.log(yield formatEntity(db, entities[i]));
    console.log();

    for (i in attributes)
      console.log(yield formatAttribute(db, attributes[i]));
    console.log();

    yield chan.each(
      function(e) {
        console.log(JSON.stringify(e.key) + ':', JSON.stringify(e.value));
      },
      db.raw());

    console.log();
  });
};


var schema = {
  greeting: {
    indexed: function(text) { return text.trim().split(/\s*\b/); }
  },
  weight: {
    indexed: true
  },
  parents: {
    reference: true,
    multiple : true
  }
};


cc.top(cc.go(function*() {
  var db = yield engine('', schema, { db: memdown });
  var entities = ['olaf', 'delaney', 'grace'];
  var attributes = ['greeting', 'age', 'weight', 'height', 'parents'];

  yield cc.join([
    db.updateEntity('olaf', {
      greeting: 'Hello, I am Olaf!',
      age     : 50,
      weight  : 87.5,
      height  : 187.0
    }),
    db.updateEntity('delaney', {
      greeting: 'Hi there.',
      age     : 5,
      weight  : 2.5,
      height  : 2.5,
      parents : 'olaf'
    }),
    db.updateEntity('grace', {
      greeting: 'Nice to meet you!',
      age     : 0,
      weight  : 30,
      height  : 40,
      parents : 'olaf'
    })]);

  yield show(db, entities, attributes);

  console.log('weights between 20 and 50:',
              yield db.byAttribute('weight', { from: 20, to: 50 }));
  console.log('heights between 0 and 50:',
              yield db.byAttribute('height', { from: 0, to: 50 }));
  console.log('words starting with H in greetings',
              yield db.byAttribute('greeting', { from: 'H', to: 'H~' }));
  console.log('occurrences of Hello in greetings',
              yield db.byAttribute('greeting', { value: 'Hello' }));
  console.log();

  console.log('--- after adding olaf and delaney to grace\'s parents: ---');
  yield db.updateEntity('grace', { parents: ['olaf', 'delaney'] });
  yield show(db, entities, attributes);

  console.log('--- after changing olaf\'s weight: ---');
  yield db.updateAttribute('weight', { olaf: 86 });
  yield show(db, entities, attributes);

  console.log('--- after deleting delaney: ---');
  yield db.destroyEntity('delaney');
  yield show(db, entities, attributes);

  console.log('--- after deleting weights: ---');
  yield db.destroyAttribute('weight');
  yield show(db, entities, attributes);

  console.log('--- after unlisting olaf and delaney as grace\'s parents: ---');
  yield db.unlist('grace', 'parents', ['olaf', 'delaney']);
  yield show(db, entities, attributes);

  yield chan.each(console.log, yield db.log());

  db.close();
}));
