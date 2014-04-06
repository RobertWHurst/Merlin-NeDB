

// lib
var MerlinNeDB = require('./lib/merlin-nedb');


/**
 * Merlin NeDB factory.
 * @param  {Object}   opts MerlinNeDB opts.
 * @return {Function}      MerlinNeDB inner factory.
 */
function merlinNeDBFactory(opts) {

  // defaults
  opts = opts || {};

  // validate
  if(typeof opts != 'object') { throw new Error('opts must be an object'); }

  /**
   * Merlin NeDB inner factory.
   * @param  {Object}   merlin Merlin instance.
   * @return {Function}         MerlinNeDB instance.
   */
  return function(merlin) {

    // validate
    if(typeof merlin != 'object') { throw new Error('merlin must be an object'); }

    // return new merlin nedb instance.
    return new MerlinNeDB(merlin, opts);
  };
}


exports = module.exports = merlinNeDBFactory;
exports.MerlinNeDB = MerlinNeDB;
