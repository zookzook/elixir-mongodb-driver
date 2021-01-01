## 0.7.1

* Enhancements
    * upgraded decimal to 2.0, jason to 1.2
    * Add proper support for tailable cursors and awaitData (PR #74)

## 0.7.0

* Enhancements
    * refactored event notification system
    * added support for retryable reads and writes
    * refactored the test cases
    * now using mtools for a MongoDB deployment in the travis ci environment
    * travis ci uses only the latest MongoDB version [The failCommand](https://github.com/mongodb/mongo/wiki/The-%22failCommand%22-fail-point)
    * `Session.commit_transaction` returns now `:ok` or an error `{:error, %Mongo.Error{}}`

* Bugfixes
    * Using `max_staleness_ms` > 0 results in a crash
    * Read preferences are sent to mongos
     
## 0.6.5

* Enhancements
    * updated db_connection dependency
    * generalize inconsistent typespecs
    * new function `BSON.ObjectId.decode/1` and `BSON.ObjectId.encode/1`
    * new function `Mongo.uuid/1` 
    
## 0.6.4

* Bugfixes
    * fixed bug in `Mongo.TopologyDescription` in case of a shard cluster deployment (#39)    
    
## 0.6.3

* Enhancements
    * basic support for inserting structs
    * removed duplicated code
    * Cursor-API raises a `Mongo.Error` instead of a `FunctionClauseError`
    
* Bugfixes
    * `:appname` option (typo) #38
    * fixed index creation in `Mongo.GridFs.Bucket`
    
## 0.6.2

* Enhancements
    * refactored the api of `Mongo.limits` and `Mongo.wire_version` 
    * add support for tracking recovery token from response in a sharded transaction
    
## 0.6.1

* Enhancements
    * refactored `Mongo.Session` and `Mongo.Session.SessionPool` because of poor performance
    
## 0.6.0

* Enhancements
    * refactored `writeConcern`
    * refactored `filter_nils`
    * refactored usage of `ReadPreference`
    * added support for sessions (`ServerSession`, `SessionPool`, `Session`)
    * added support for transaction
    * added Decimal128 encoder
    * added support for transaction to gridfs and bulk operation
    * added `create` command (explicitly creating a collection or view)    
    
## 0.5.7

* Bug Fixes
   * Test for existing index in `Bucket` works right now

* Enhancements
   * Better handling for the `:timeout` options 

## 0.5.6

* Bug Fixes
    * Fixed a match error in `after_fun` of cursor module
    * Fixed a match error in the result of function `Monitor.force_check` 
    * Resolved decode problem for the Binary (Old) BinData subtype
    
* Enhancements
    * Added support for `Mongo.BulkWriteResult`

## 0.5.5

* Bug Fixes
    * Fixed a match error when using Windows OS
    
## 0.5.4

* Enhancements
   * The driver provides now client metadata 
   * Added support for connecting via UNIX sockets (`:socket` and `:socket_dir`)
   * Added support for bulk writes (ordered/unordered, in-memory/stream)
   * Added support for `op_msg` with payload type 1 
   * Merged code from https://github.com/ankhers/mongodb/commit/63c20ff7e427744a5df915751adfaf6e5e39ae62
   * Merged changes from https://github.com/ankhers/mongodb/pull/283
   * Merged changes from https://github.com/ankhers/mongodb/pull/281

## 0.5.3

* Enhancements
   * Travis now using the right MongoDB version
   
* Bug Fixes
   * Added test unit for change streams
   * Removed debug code from change streams

## 0.5.2

* Enhancements
  * Added `op_msg` support ([See](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-msg))
  * Fixed change streams in case of error codes: 11601, 136 and 237 from resuming
  * Reorganized error handling   
  
## 0.5.1

* Enhancements
  * Upgraded to DBConnection 2.0.6
  * Refactored code, simplified code and api
  * Replaced deprecated op_code by database commands
  * Update_one, update_many, replace_one, replace_many return upserted ids
  * Add support for all find options
  * Add support for MongoDB 3.6 collection [Change Streams](https://docs.mongodb.com/manual/changeStreams/)
  * Ass support for SCRAM-SHA-256 (MongoDB 4.x)

## 0.4.8-dev

* Enhancements
  * Added hostname and port to exceptions
  * Added support for x509 authentication
  * Allow passing only partial `read_preference` information
  * Add support for GridFS

* Bug Fixes
  * Fixed a connection leak
  * Properly parse write concern for URL
  * Properly follow read preference for `secondary_preferred`

## 0.4.7

* Enhancements
  * Added 4.0 to supported versions
  * Initial support for mongodb+srv URLs
  * Support for Decimal128

## 0.4.6

* Enhancements
  * Added `:connect_timout_ms` to `Mongo.start_link/1`
  * Reorganized documentation

## 0.4.5 (2018-04-08)

* Enhancements
  * Should now be able to send a query to your server before the connection
    is fully made

* Bug Fixes
  * Should actually be able to query for longer than 5 seconds

## 0.4.4 (2018-02-09)

* Enhancements
  * Added support for using a mongo url via the `:url` key
  * Added MongoDB 3.6 to supported versions
  * Added support for the deprecated `undefined` BSON type

* Bug Fixes
  * Added another case for BSON NaN
  * Fixed encoding and decoding of the BSON Timestamp type
  * Should now figure out Topology for replica sets even if you exclude the
    `:type` key
  * Fixed an issue where our monitors would become empty, preventing the driver
    from reconnecting to a downed database

## 0.4.3 (2017-09-16)

* Enhancements
  * Send TLS server name indication (SNI) if none is set in the `:ssl_opts`
  * Fixed a couple dialyzer issues
  * Add basic examples of `$and`, `$or`, and `$in` operators in README

* Bug Fixes
  * Ensure cursor requests are routed to the proper node in the cluster
  * No longer attempting to authenticate against arbiter nodes
  * Prevent monitor errors if you have stopped the mongo process

## 0.4.2 (2017-08-28)

* Bug fixes
  * Fix application crash when a replica set member goes offline
  * Fix application crash on start when a replica set member is offline

## 0.4.1 (2017-08-09)

* Bug fixes
  * Monitors no longer use a pool
  * Can now connect to a Mongo instance using a CNAME
  * Pass options through Mongo.aggregate/4

## 0.4.0 (2017-06-07)

* Replica Set Support

## 0.3.0 (2017-05-11)

* Breaking changes
  * Remove `BSON.DateTime` and replace it with native Elixir `DateTime`

## 0.2.1 (2017-05-08)

* Enhancements
  * SSL support
  * Add functions `BSON.DateTime.to_elixir_datetime/1` and `BSON.DateTime.from_elixir_datetime/1`

* Changes
  * Requires Elixir ~> 1.3

## 0.2.0 (2016-11-11)

* Enhancements
  * Add `BSON.ObjectID.encode!/1` and `BSON.ObjectID.decode!/1`
  * Optimize and reduce binary copying
  * Add tuple/raising versions of functions in `Mongo`
  * Add `:inserted_count` field to `Mongo.InsertManyResult`
  * Support NaN and infinite numbers in bson float encode/decode
  * Add `Mongo.object_id/0` for generating objectids
  * Add `Mongo.child_spec/2`
  * Add `Mongo.find_one_and_update/5`
  * Add `Mongo.find_one_and_replace/5`
  * Add `Mongo.find_one_and_delete/4`

* Bug fixes
  * Fix float endianness

* Breaking changes
  * Switched to using `db_connection` library, see the current docs for changes

## 0.1.1 (2015-12-17)

* Enhancements
  * Add `BSON.DateTime.from_datetime/1`

* Bug fixes
  * Fix timestamp epoch in generated object ids
  * Fix `Mongo.run_command/3` to accept errors without code

## 0.1.0 (2015-08-25)

Initial release
