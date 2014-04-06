
// modules
var test = require('tape');

// libs
var merlinNeDB = require('../');

test('merlinNeDBFactory()', function(t) {
  t.throws(function() { merlinNeDB(1); });
  t.throws(function() { merlinNeDB('s'); });
  t.throws(function() { merlinNeDB(true); });
  t.doesNotThrow(function() { merlinNeDB(); });
  t.doesNotThrow(function() { merlinNeDB({}); });

  t.equal(typeof merlinNeDB(), 'function');

  t.end();
});

test('merlinNeDBInnerFactory()', function(t) {
  var inner = merlinNeDB({});
  t.throws(function() { inner(); });
  t.throws(function() { inner(null); });
  t.throws(function() { inner(1); });
  t.throws(function() { inner(false); });
  t.throws(function() { inner('s'); });
  t.doesNotThrow(function() { inner({}); });

  var NeDB = inner({});
  t.equal(typeof NeDB, 'object');

  t.end();
});
