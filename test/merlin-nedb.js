
// modules
var test = require('tape');
var path = require('path');
var fs = require('fs');

// libs
var MerlinNeDB = require('../').MerlinNeDB;

test('MerlinNeDB()', function(t) {

  t.throws(function() { new MerlinNeDB(); });
  t.throws(function() { new MerlinNeDB(null); });
  t.throws(function() { new MerlinNeDB(1); });
  t.throws(function() { new MerlinNeDB(false); });
  t.throws(function() { new MerlinNeDB('s'); });
  t.throws(function() { new MerlinNeDB({}); });
  t.throws(function() { new MerlinNeDB({}, null); });
  t.throws(function() { new MerlinNeDB({}, 1); });
  t.throws(function() { new MerlinNeDB({}, false); });
  t.throws(function() { new MerlinNeDB({}, 's'); });
  t.doesNotThrow(function() { new MerlinNeDB({}, {}); });

  t.end();
});

test('merlinNeDB{}', function(t) {

  var merlinNeDB = new MerlinNeDB({}, {});

  t.equal(typeof merlinNeDB.connect, 'function');
  t.equal(typeof merlinNeDB.close, 'function');
  t.equal(typeof merlinNeDB.index, 'function');
  t.equal(typeof merlinNeDB.count, 'function');
  t.equal(typeof merlinNeDB.find, 'function');
  t.equal(typeof merlinNeDB.insert, 'function');
  t.equal(typeof merlinNeDB.update, 'function');
  t.equal(typeof merlinNeDB.remove, 'function');

  t.end();
});

test('merlinNeDB.connect()', function(t) {

  var merlinNeDB = new MerlinNeDB({ models: {}}, {});

  t.throws(function() { merlinNeDB.connect(1); });
  t.throws(function() { merlinNeDB.connect('s'); });
  t.throws(function() { merlinNeDB.connect({}); });
  t.doesNotThrow(function() {
    merlinNeDB.connect(function(err) {
      t.error(err);
    });
  });

  t.end();
});

test('merlinNeDB.connect() - In memory', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);
    t.equal(typeof merlinNeDB.collections.tests, 'object');
    t.end();
  });
});

test('merlinNeDB.connect() - On disk', function(t) {

  var dbPath = path.resolve(__dirname, '../tmp/database');
  var dbCollectionPath = path.join(dbPath, 'tests.nedb');

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {
    databasePath: dbPath
  });

  t.throws(function() { merlinNeDB.connect(1); });
  t.throws(function() { merlinNeDB.connect('s'); });
  t.throws(function() { merlinNeDB.connect({}); });

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.ok(fs.existsSync(dbPath));
    t.ok(fs.existsSync(dbCollectionPath));
    fs.unlinkSync(dbCollectionPath);
    fs.rmdirSync(dbPath);

    t.end();
  });
});

test('merlinNeDB.close()', function(t) {

  var dbPath = path.resolve(__dirname, '../tmp/database');
  var dbCollectionPath = path.join(dbPath, 'tests.nedb');

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);
    t.equal(typeof merlinNeDB.collections.tests, 'object');
    merlinNeDB.close(function(err) {
      t.equal(merlinNeDB.collections.tests, undefined);
      t.end();
    });
  });
});

test('merlinNeDB.index()', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.doesNotThrow(function() { merlinNeDB.index(); });
    t.doesNotThrow(function() { merlinNeDB.index(1); });
    t.doesNotThrow(function() { merlinNeDB.index('s'); });
    t.doesNotThrow(function() { merlinNeDB.index({}); });
    t.doesNotThrow(function() { merlinNeDB.index(false); });
    t.doesNotThrow(function() { merlinNeDB.index('tests', 1, function(err) { t.ok(err); }); });
    t.doesNotThrow(function() { merlinNeDB.index('tests', 's', function(err) { t.ok(err); }); });
    t.doesNotThrow(function() { merlinNeDB.index('tests', false, function(err) { t.ok(err); }); });
    t.doesNotThrow(function() { merlinNeDB.index('tests', {}, 1); });
    t.doesNotThrow(function() { merlinNeDB.index('tests', {}, 's'); });
    t.doesNotThrow(function() { merlinNeDB.index('tests', {}, false); });
    t.throws(function() { merlinNeDB.index('tests', {}, 'name', 1); });
    t.throws(function() { merlinNeDB.index('tests', {}, 'name', 's'); });

    merlinNeDB.collections.tests.ensureIndex = function(opts, cb) {
      t.equal(typeof opts, 'object');
      t.equal(opts.sparse, true);
      t.equal(opts.unique, true);
      t.equal(opts.fieldName, 'name');
      cb(null);
    };

    merlinNeDB.index('tests', { sparse: true, unique: true }, 'name', function(err) {
      t.error(err);
      t.end();
    });
  });
});

test('merlinNeDB.count()', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.throws(function() { merlinNeDB.count(); });
    t.throws(function() { merlinNeDB.count(1); });
    t.throws(function() { merlinNeDB.count('s'); });
    t.throws(function() { merlinNeDB.count('tests', 1); });
    t.throws(function() { merlinNeDB.count('tests', 's'); });
    t.throws(function() { merlinNeDB.count('tests', false); });
    t.throws(function() { merlinNeDB.count('tests', {}, 1); });
    t.throws(function() { merlinNeDB.count('tests', {}, 's'); });

    merlinNeDB.collections.tests.count = function(neDBQuery, cb) {
      t.equal(typeof neDBQuery, 'object');
      t.equal(neDBQuery.name, 'test');
      cb(null, 10);
    };

    merlinNeDB.count('tests', {}, {
      query: { name: 'test' },
      opts: {}
    }, {
      write: function(count) { t.equal(count, 10); },
      end: function() { t.end(); }
    });
  });
});

test('merlinNeDB.find()', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.throws(function() { merlinNeDB.find(); });
    t.throws(function() { merlinNeDB.find(1); });
    t.throws(function() { merlinNeDB.find('s'); });
    t.throws(function() { merlinNeDB.find('tests', 1); });
    t.throws(function() { merlinNeDB.find('tests', 's'); });
    t.throws(function() { merlinNeDB.find('tests', false); });
    t.throws(function() { merlinNeDB.find('tests', {}, 1); });
    t.throws(function() { merlinNeDB.find('tests', {}, 's'); });
    t.throws(function() {
      merlinNeDB.find('tests', {}, {
        query: {},
        opts: {}
      }, 1);
    });
    t.throws(function() {
      merlinNeDB.find('tests', {}, {
        query: {},
        opts: {}
      }, 's');
    });

    merlinNeDB.collections.tests.find = function(neDBQuery, cb) {
      t.equal(typeof neDBQuery, 'object');
      t.equal(neDBQuery.name, 'test');
      cb(null, [
        { name: 'test-a' },
        { name: 'test-b' },
        { name: 'test-c' }
      ]);
    };

    var recordNames = [
      'test-a',
      'test-b',
      'test-c'
    ];
    merlinNeDB.find('tests', {}, {
      query: { name: 'test' },
      opts: {}
    }, {
      write: function(record) {
        var i = recordNames.indexOf(record.name);
        if(i !== -1) { recordNames.splice(i, 1); }
        t.notEqual(i, -1);
      },
      end: function() { t.end(); }
    });
  });
});

test('merlinNeDB.insert()', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.throws(function() { merlinNeDB.insert(); });
    t.throws(function() { merlinNeDB.insert(1); });
    t.throws(function() { merlinNeDB.insert('s'); });
    t.throws(function() { merlinNeDB.insert('tests', 1); });
    t.throws(function() { merlinNeDB.insert('tests', 's'); });
    t.throws(function() { merlinNeDB.insert('tests', false); });
    t.throws(function() { merlinNeDB.insert('tests', {}, 1); });
    t.throws(function() { merlinNeDB.insert('tests', {}, 's'); });
    t.throws(function() {
      merlinNeDB.insert('tests', {}, {
        all: function() {}
      }, 1);
    });
    t.throws(function() {
      merlinNeDB.insert('tests', {}, {
        all: function() {}
      }, 's');
    });

    merlinNeDB.collections.tests.insert = function(records, cb) {
      t.equal(typeof records, 'object');
      t.equal(typeof records.length, 'number');

      cb(null, records);
    };

    var recordNames = [
      'test-a',
      'test-b',
      'test-c'
    ];
    merlinNeDB.insert('tests', {}, {
      all: function(cb) {
        cb(null, [
          { name: 'test-a' },
          { name: 'test-b' },
          { name: 'test-c' }
        ]);
      }
    }, {
      write: function(record) {
        var i = recordNames.indexOf(record.name);
        if(i !== -1) { recordNames.splice(i, 1); }
        t.notEqual(i, -1);
      },
      end: function() { t.end(); }
    });
  });
});

test('merlinNeDB.update()', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.throws(function() { merlinNeDB.update(); });
    t.throws(function() { merlinNeDB.update(1); });
    t.throws(function() { merlinNeDB.update('s'); });
    t.throws(function() { merlinNeDB.update('tests', 1); });
    t.throws(function() { merlinNeDB.update('tests', 's'); });
    t.throws(function() { merlinNeDB.update('tests', false); });
    t.throws(function() { merlinNeDB.update('tests', {}, 1); });
    t.throws(function() { merlinNeDB.update('tests', {}, 's'); });
    t.throws(function() { merlinNeDB.update('tests', {}, false); });
    t.throws(function() {
      merlinNeDB.update('tests', {}, {
        query: {},
        opts: {}
      }, 1);
    });
    t.throws(function() {
      merlinNeDB.update('tests', {}, {
        query: {},
        opts: {}
      }, 's');
    });
    t.throws(function() {
      merlinNeDB.update('tests', {}, {
        query: {},
        opts: {}
      }, {}, 1);
    });
    t.throws(function() {
      merlinNeDB.update('tests', {}, {
        query: {},
        opts: {}
      }, {}, 's');
    });

    merlinNeDB.collections.tests.update = function(neDBQuery, neDBDelta, cb) {
      t.equal(typeof neDBQuery, 'object');
      t.equal(neDBQuery.name, 'test');
      t.equal(typeof neDBDelta, 'object');
      t.equal(typeof neDBDelta.$set, 'object');
      t.equal(neDBDelta.$set.name, 'test-a');
      cb(null, 3);
    };

    merlinNeDB.update('tests', {}, {
      query: { name: 'test' },
      opts: {}
    }, { diff: { $set: { name: 'test-a' }}}, {
      write: function(count) { t.equal(count, 3); },
      end: function() { t.end(); }
    });
  });
});

test('merlinNeDB.remove()', function(t) {

  var merlinNeDB = new MerlinNeDB({
    models: { Test: { collectionName: 'tests' }}
  }, {});

  merlinNeDB.connect(function(err) {
    t.error(err);

    t.throws(function() { merlinNeDB.remove(); });
    t.throws(function() { merlinNeDB.remove(1); });
    t.throws(function() { merlinNeDB.remove('s'); });
    t.throws(function() { merlinNeDB.remove('tests', 1); });
    t.throws(function() { merlinNeDB.remove('tests', 's'); });
    t.throws(function() { merlinNeDB.remove('tests', false); });
    t.throws(function() { merlinNeDB.remove('tests', {}, 1); });
    t.throws(function() { merlinNeDB.remove('tests', {}, 's'); });
    t.throws(function() { merlinNeDB.remove('tests', {}, false); });
    t.throws(function() {
      merlinNeDB.remove('tests', {}, {
        query: {},
        opts: {}
      }, 1);
    });
    t.throws(function() {
      merlinNeDB.remove('tests', {}, {
        query: {},
        opts: {}
      }, 's');
    });

    merlinNeDB.collections.tests.remove = function(neDBQuery, cb) {
      t.equal(typeof neDBQuery, 'object');
      t.equal(neDBQuery.name, 'test');
      cb(null, 3);
    };

    merlinNeDB.remove('tests', {}, {
      query: { name: 'test' },
      opts: {}
    }, {
      write: function(count) { t.equal(count, 3); },
      end: function() { t.end(); }
    });
  });
});




