'use strict';

var cc     = require('ceci-core');
var chan   = require('ceci-channels');
var engine = require('../index');


var schema = {
  "value": {
    "multiple": true
  },
  "notes": {
    "indirect": true
  },
  "image_attachment": {
    "multiple": true,
    "indirect": true
  },
  "text_attachment": {
    "multiple": true,
    "indirect": true
  },
  "parent": {
    "reference": true
  },
  "sedimentological_description": {
    "indirect": true
  },
  "plug_photo": {
    "multiple": true,
    "indirect": true
  },
  "brothers_ids": {
    "reference": true,
    "multiple": true
  },
  "sisters_ids": {
    "reference": true,
    "multiple": true
  },
  "photo": {
    "multiple": true,
    "indirect": true
  },
  "lab_technique_&_acquisition_settings": {
    "indirect": true
  },
  "project_name": {
    "indexed": true
  },
  "organisation": {
    "indexed": true
  },
  "project_leaders": {
    "multiple": true
  },
  "member_users": {
    "multiple": true
  },
  "member_data_ids": {
    "multiple": true
  },
  "image_of_location_in_sub-plug": {
    "indirect": true
  },
  "images": {
    "multiple": true,
    "indirect": true
  },
  "preserved_core": {
    "multiple": true
  },
  "image_of_log": {
    "indirect": true
  },
  "log_data": {
    "multiple": true
  },
  "core_photo": {
    "multiple": true,
    "indirect": true
  },
  "unit_id": {
    "reference": true
  },
  "sub-unit_id": {
    "reference": true
  }
};


cc.top(cc.go(function*() {
  var db, entities, key;

  db = yield engine(process.argv[2], schema);
  entities = yield db.byAttribute('type');

  for (key in entities)
    if (entities.hasOwnProperty(key))
      entities[key] = yield db.byEntity(key);

  console.log(JSON.stringify(entities, null, 4));
}));
