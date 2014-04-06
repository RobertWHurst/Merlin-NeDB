
// modules
var Datastore = require('nedb');
var path = require('path');


/**
 * Merlin NeDB constructor
 * @constructor
 * @param {Merlin} merlin Merlin ORM instance.
 * @param {Object}  opts    Merlin NeDB opts.
 */
function MerlinNeDB(merlin, opts) {

  // validate
  if(merlin === null || typeof merlin != 'object') {
    throw new Error('merlin must be an object');
  }
  if(opts === null || typeof opts != 'object') {
    throw new Error('opts must be an object');
  }
  if(opts.databasePath && typeof opts.databasePath != 'string') {
    throw new Error('opts.databasePath must be a string');
  }

  // setup
  this.merlin = merlin;
  this.opts = opts;
  this.collections = {};
  this._dbPath = opts.databasePath || null;

  // set merlin config
  this.merlin.opts.idKey = '_id';
  this.merlin.opts.pluralForeignKey = '_{modelName}Ids';
  this.merlin.opts.singularForeignKey = '_{modelName}Id';
}

/**
 * Setup the Datastores.
 * @param  {Function} cb Executed upon completion.
 */
MerlinNeDB.prototype.connect = function(cb) {

  // defaults
  cb = cb || function() {};

  // validate
  if(typeof cb != 'function') { throw new Error('cb must be a function.'); }

  // grab some needed vars
  var dbPath = this._dbPath;
  var models = this.merlin.models;
  var collections = this.collections;

  // grab all models registered on merlin so we
  // can create/load a Datastore for each of them.
  var modelNames = Object.keys(models);
  var j = modelNames.length;
  if(j === 0) { return cb(null); }
  for(var i = 0; i < modelNames.length; i += 1) {
    var model = models[modelNames[i]];

    // create the datastore opts
    var opts = {};
    if(dbPath) { opts.filename = path.resolve(dbPath, model.collectionName) + '.nedb'; }

    // create a collection datastore.
    var collection = collections[model.collectionName] = new Datastore(opts);

    // load the datastore
    collection.loadDatabase(function(err) {
      if(err) { j = 0; return cb(err); }
      j -= 1;
      if(j === 0) { return cb(null); }
    });
  }
};

/**
 * teardown the Datastores.
 * @param  {Function} cb Executed upon completion.
 */
MerlinNeDB.prototype.close = function(cb) {

  // defaults
  cb = cb || function() {};

  // validate
  if(typeof cb != 'function') { throw new Error('cb must be a function.'); }

  // delete the collections
  this.collections = {};

  // callback
  cb(null);
};

/**
 * Index a field within a collection.
 * @param  {String}   collectionName Collection name.
 * @param  {String}   path           Index field path.
 * @param  {Object}   opts           Indexing options.
 * @param  {Function} [cb]           Executed upon completion.
 */
MerlinNeDB.prototype.index = function(collectionName, opts, path, cb) {

  // defaults
  cb = cb || function() {};

  // validate
  if(typeof cb != 'function') { throw new Error('cb must be a function.'); }
  if(typeof collectionName != 'string') { return cb(new Error('collectionName must be a string.')); }
  if(typeof path != 'string') { return cb(new Error('path must be a string.')); }
  if(opts === null || typeof opts != 'object') { return cb(new Error('opts must be an object.')); }
  opts.unique = opts.unique || false;
  opts.sparse = opts.sparse || false;
  if(typeof opts.unique != 'boolean') { return cb(new Error('opts.unique must be a boolean.')); }
  if(typeof opts.sparse != 'boolean') { return cb(new Error('opts.sparse must be a boolean.')); }

  // get the collection
  var collection = this.collections[collectionName];

  //TODO: Indexing
  collection.ensureIndex({
    fieldName: path,
    unique: opts.unique,
    sparse: opts.sparse
  }, cb);
};

/**
 * Count all records matching a query.
 * @param  {String}      collectionName Collection name.
 * @param  {Query}       query          Query.
 * @param  {Object}      opts           Find options.
 * @param  {CountStream} cout           Count stream.
 */
MerlinNeDB.prototype.count = function(collectionName, opts, query, cout) {

  // validate
  if(typeof collectionName != 'string') { throw new Error('collectionName must be a string.'); }
  if(query === null || typeof query != 'object') { throw new Error('query must be an object.'); }
  if(opts === null || typeof opts != 'object') { throw new Error('opts must be an object.'); }
  if(cout === null || typeof cout != 'object') { throw new Error('cout must be an object.'); }

  // get the collection and the query
  var collection = this.collections[collectionName];
  var neDBQuery = this._convertQuery(query);

  // count
  collection
    .count(neDBQuery)
    .skip(query.opts.offset)
    .limit(query.opts.limit)
    .exec(function(err, count) {
      if(err) { return cout.emit('error', err); }
      cout.write(count);
      cout.end();
    });
};

/**
 * Find all records matching a query.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Find options.
 * @param  {Query}       query          Query.
 * @param  {ModelStream} rout           Records output stream.
 */
MerlinNeDB.prototype.find = function(collectionName, opts, query, rout) {
  var _this = this;

  // validate
  if(typeof collectionName != 'string') { throw new Error('collectionName must be a string.'); }
  if(query === null || typeof query != 'object') { throw new Error('query must be an object.'); }
  if(opts === null || typeof opts != 'object') { throw new Error('opts must be an object.'); }
  if(rout === null || typeof rout != 'object') { throw new Error('rout must be an object.'); }

  // get the collection, query and sort order
  var collection = this.collections[collectionName];
  var neDBQuery = this._convertQuery(query);
  var neDBSort = this._convertSort(query.opts.sort);

  // find the docs
  collection
    .find(neDBQuery)
    .sort(neDBSort)
    .skip(query.opts.offset)
    .limit(query.opts.limit)
    .exec(function(err, records) {
      if(err) { return rout.emit('error', err); }
      records = query.filter(records);
      for(var i = 0; i < records.length; i += 1) {
        rout.write(records[i]);
      }
      rout.end();
    });
};

/**
 * Insert an array of records.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Insert options.
 * @param  {ModelStream} rin            Records input stream.
 * @param  {ModelStream} rout           Records output stream.
 */
MerlinNeDB.prototype.insert = function(collectionName, opts, rin, rout) {
  var _this = this;

  // validate
  if(typeof collectionName != 'string') { return cb(new Error('collectionName must be a string.')); }
  if(opts === null || typeof opts != 'object') { return cb(new Error('opts must be an object.')); }
  if(rin === null || typeof rin != 'object') { return cb(new Error('rin must be an object.')); }
  if(rout === null || typeof rout != 'object') { return cb(new Error('rout must be an object.')); }

  // get the collection, and sort order.
  var collection = this.collections[collectionName];
  var neDBSort = this._convertSort(opts.sort);

  // wait for all of the records.
  rin.all(function(err, records) {
    if(err) { return rout.emit('error', err); }

    // insert the docs.
    collection.insert(records, function(err, records) {
      if(err) { return rout.emit('error', err); }

      // send out the records
      for(var i = 0; i < records.length; i += 1) {
        rout.write(records[i]);
      }
      rout.end();
    });
  });
};

/**
 * Insert an array of records.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Update options.
 * @param  {Query}       query          Query.
 * @param  {Delta}       delta          Delta.
 * @param  {CountStream} cout           Count stream.
 */
MerlinNeDB.prototype.update = function(collectionName, opts, query, delta, cout) {
  var _this = this;

  // validate
  if(typeof collectionName != 'string') { throw new Error('collectionName must be a string.'); }
  if(opts === null || typeof opts != 'object') { throw new Error('opts must be an object.'); }
  if(query === null || typeof query != 'object') { throw new Error('query must be an object.'); }
  if(delta === null || typeof delta != 'object') { throw new Error('delta must be an object.'); }
  if(cout === null || typeof cout != 'object') { throw new Error('cout must be an object.'); }

  // get the collection and the query
  var collection = this.collections[collectionName];
  var neDBDelta = this._convertDelta(delta);
  var neDBQuery = this._convertQuery(query);

  // update the records
  collection.update(neDBQuery, neDBDelta, { multi: !opts.single }, function(err, count) {
    if(err) { return cout.emit('error', err); }
    cout.write(count);
    cout.end();
  });
};

/**
 * Remove records matching a query.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Remove options.
 * @param  {Query}       query          Query.
 * @param  {CountStream} cout           Count stream.
 */
MerlinNeDB.prototype.remove = function(collectionName, opts, query, cout) {

  // validate
  if(typeof collectionName != 'string') { return cb(new Error('collectionName must be a string.')); }
  if(opts === null || typeof opts != 'object') { return cb(new Error('opts must be an object.')); }
  if(query === null || typeof query != 'object') { return cb(new Error('query must be an object.')); }
  if(cout === null || typeof cout != 'object') { return cb(new Error('cout must be an object.')); }

  // get the collection and the query
  var collection = this.collections[collectionName];
  var neDBQuery = this._convertQuery(query);

  // remove the records
  collection.remove(neDBQuery, function(err, count) {
    if(err) { return cout.emit('error', err); }
    cout.write(count);
    cout.end();
  });
};

/**
 * Convert the Merlin Query to a NeDB Query object.
 * @private
 * @param  {Query}  query Merlin Query instance.
 * @return {Object}       NeDB query object.
 */
MerlinNeDB.prototype._convertQuery = function(query) {

  // validate
  if(
    query === null || typeof query != 'object' ||
    query.query === null || typeof query.query != 'object' ||
    query.opts === null || typeof query.opts != 'object'
  ) { throw new Error('query must be an instance of Query.'); }

  // build the new query
  return (function rec(vq) {
    var nq = {};
    var vqProps = Object.keys(vq);
    for(var i = 0; i < vqProps.length; i += 1) {
      var vqProp = vqProps[i];
      var vqVal = vq[vqProp];

      // if the value is an object then check to
      // see if its an operator object or a
      // regex. If it is then preform any
      // convertion that may be needed.
      if(vqVal !== null && typeof vqVal == 'object') {

        // check to see if this is an operator
        // object.
        for(var oProp in vqVal) { break; }
        if(oProp && oProp.charAt(0) == '$') {
          var nqVal = {};
          for(oProp in vqVal) {

            // $notIn => $nin
            if(oProp == '$notIn') { nqVal.$nin = vqVal[oProp]; }

            // $not => $ne
            else if(oProp == '$not') { nqVal.$ne = vqVal[oProp]; }

            // straight copy
            else { nqVal[oProp] = vqVal[oProp]; }
          }
          nq[vqProp] = nqVal;
        }

        // regex
        else if(vqVal.constructor == RegExp) {
          nq[vqProp] = { $regex: vqVal };
        }

        // recurse
        else {
          nq[vqProp] = rec(vq[vqProp]);
        }
      }

      // copy non object properties.
      else {
        nq[vqProp] = vqVal;
      }
    }
    return nq;
  })(query.query);
};

/**
 * Convert the Merlin Delta to a NeDB Delta object.
 * @private
 * @param  {Query}  delta Merlin Delta instance.
 * @return {Object}       NeDB delta object.
 */
MerlinNeDB.prototype._convertDelta = function(delta) {

  // build the new delta
  var vd = delta.diff;
  var nd = {};
  for(var opt in vd) {

    // unset
    if(opt == '$unset') {
      nd[opt] = {};
      for(var i = 0; i < vd[opt].length; i += 1) {
        nd[opt][vd[opt][i]] = true;
      }
    }

    // pull
    else if(opt == '$pull') {
      nd[opt] = {};
      for(var fieldPath in vd[opt]) {
        nd[opt][fieldPath] = { $in: vd[opt][fieldPath] };
      }
    }

    // everything else
    else {
      nd[opt] = vd[opt];
    }
  }
  return nd;
};

MerlinNeDB.prototype._convertSort = function(sort) {
  var ns = null;
  if(sort !== null && typeof sort == 'object') {
    for(var i = 0; i < sort.length; i += 1) {
      for(var fieldPath in sort[i]) {
        if(sort[i].hasOwnProperty(fieldPath)) {
          if(!ns) { ns = {}; }
          ns[fieldPath] = sort[i][fieldPath] === 'asc' && 1 || -1
        }
      }
    }
  }
  return ns;
};

/**
 * Pipe records out to a model stream with a limit
 * and offset.
 * @private
 * @param  {Array}       records  Array of records.
 * @param  {Number}      [offset] Offset count.
 * @param  {Number}      [limit]  Limit count.
 * @param  {ModelStream} rout     Model stream out.
 */
MerlinNeDB.prototype._pipeOutRecords = function(records, offset, limit, rout) {

  if(limit === undefined && rout === undefined) {
    rout = offset;
    offset = undefined;
  }

  // get the limit and offset.
  var limit = limit || records.length;
  var offset = offset || 0;

  // loop through and drop each doc into the
  // model stream.
  while(records.length > 0 && limit > 0) {

    // decrement offset
    if(offset > 0) { offset -= 1; }

    // send doc and decrement limit
    else {
      limit -= 1;
      rout.write(records.shift());
    }
  }

  // close the stream.
  rout.end();
};


module.exports = MerlinNeDB;
