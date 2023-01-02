# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2](https://github.com/zookzook/elixir-mongodb-driver/compare/v1.0.1...v1.0.2) (2023-01-02)


### Bug Fixes

* deprecated 'use Bitwise' ([#168](https://github.com/zookzook/elixir-mongodb-driver/issues/168)) ([a980d57](https://github.com/zookzook/elixir-mongodb-driver/commit/a980d5782a56a0a8168c5229fe22b7a935c433b3))
* use the basename of the file path ([3a10c56](https://github.com/zookzook/elixir-mongodb-driver/commit/3a10c5604a2cbc5d284a17578280e96c93f61e63))

## [1.0.1](https://github.com/zookzook/elixir-mongodb-driver/compare/v1.0.0...v1.0.1) (2022-12-17)


### Bug Fixes

* replace :code.priv_dir/1 function ([da0fd6c](https://github.com/zookzook/elixir-mongodb-driver/commit/da0fd6cdc777c4d1cb1df0abac98af1f4a37ad11))

## [1.0.0](https://github.com/zookzook/elixir-mongodb-driver/compare/0.9.2...v1.0.0) (2022-11-27)

### Bug Fixes

* add migration ([da65de4](https://github.com/zookzook/elixir-mongodb-driver/commit/da65de4d8fd7cf6a15ac0c42b3022ca1fe743876))
* remove a bug in the hello handshake protocol (thanks to fireproofsocks for reporting) ([59aa841](https://github.com/zookzook/elixir-mongodb-driver/commit/59aa841cc619f77979cc3027c76e918373685723))
* remove derived attributes in the dump function ([c1b60b4](https://github.com/zookzook/elixir-mongodb-driver/commit/c1b60b413329f32d4e0bd52c3988b89b19fe7f5a))

### Enhancements
* improve the dump and load functions ([#154](https://github.com/zookzook/elixir-mongodb-driver/issues/154)) ([e7f2d44](https://github.com/zookzook/elixir-mongodb-driver/commit/e7f2d44a01fa25cd85cb8fd1f935ba5a201fe011))
* use the same timestamps in new/0 function ([0db61da](https://github.com/zookzook/elixir-mongodb-driver/commit/0db61dac03eb2a27d4d47576ed402ad1e6c452f4))

### Miscellaneous Chores

* release 1.0.0 ([64e274a](https://github.com/zookzook/elixir-mongodb-driver/commit/64e274a761dd9e6757d1b506ee3ba4308ac1448f))

## 0.9.3 (2022-10-14)
* Bugfix
  * fix a bug in the hello handshake protocol (thanks to fireproofsocks for reporting)
* Enhancements
  * add migration

## 0.9.2 (2022-09-24)
* Bugfix
  * fix a crash in the streaming hello monitor, if the server sends more than one response at once 
  * add support for the new hello handshake
  * refactor :timeout option (thanks to JD-Robertson for reporting)
  * add timestamps macro to the collection module to handle inserted_at and updated_at attributes (thanks to carlosliracl) 

## 0.9.1 (2022-05-27)
* Bugfix
  * add backward compatible for Elixir < 1.13 (thanks to ja-jimenez)

## 0.9.0 (2022-05-21)
* Enhancements
  * add colored log output
  * add telemetry support for execution
  * add new Repo module (thanks to daskycodes)
  * add missing typespecs (thanks to fdie)
  * refactor transaction api to support nested transaction
  * add `Mongo.rename_collection/3` command

## 0.8.4 (2022-03-09)
* Bugfix
  * add missing excludes from dump function (collections)
  
## 0.8.3 (2022-02-17)
* Bugfix
  * fix no function clause matching (thanks to bodbdigr)

## 0.8.2 (2022-02-03)
* Enhancements
  * Remove a compiler warning (thanks to a-jimenez )

## 0.8.1 (2022-01-22) 
* Enhancements
  * Fix for serializing BSON Regex without options (thanks to MillionIntegrals)
  * Misc doc changes (thanks to kianmeng)
  * Added support for OP_MSG exhaustAllowed flag
  * Added support for streaming protocol 
  * Added Insights app for development ]()

## 0.8.0 (2021-11-07) (0.7.5 was not published)
* Enhancements
  * replica set connection: faster topology update if the primary is down (thanks to p-mongo)
  * added custom `Mongo.Encoder` protocol (thanks to esse)
  * added collection from yildun project
  * fixed an issue that the bulk operation does not stop after any insert/update/delete failed (thanks to ja-jimenez)

## 0.7.4 (2021-06-21)
* Enhancements
  * added a new option to specify a timeout, when increasing the connection pool is no option

## 0.7.3 (2021-05-29)
* Enhancements
  * added support for OTP 24
  * Add support for tls setting in connection string (tschmittni)
  * Replace deprecated functions (OTP 24) (aenglisc )

## 0.7.2 (2021-05-19)

* Enhancements
  * Adds test to cover one of Mongo.find/4 errors (vukanac)
  * Update specs for Mongo.find/4 with error tuple (vukanac)
  * Fix build warnings and correct typespec (joeapearson)
  * Update db_connection version to remove System.stacktrace warnings (vukanac)
  * Update SCRAM auth procedure (LetThereBeDwight)

## 0.7.1 (2021-01-01)

* Enhancements
    * upgraded decimal to 2.0, jason to 1.2
    * Add proper support for tailable cursors and awaitData (PR #74)

## 0.7.0 (2020-04-17)

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

## 0.6.5 (2020-03-30)

* Enhancements
    * updated db_connection dependency
    * generalize inconsistent typespecs
    * new function `BSON.ObjectId.decode/1` and `BSON.ObjectId.encode/1`
    * new function `Mongo.uuid/1`

## 0.6.4 (2020-01-24)

* Bugfixes
    * fixed bug in `Mongo.TopologyDescription` in case of a shard cluster deployment (#39)

## 0.6.3 (2020-01-23)

* Enhancements
    * basic support for inserting structs
    * removed duplicated code
    * Cursor-API raises a `Mongo.Error` instead of a `FunctionClauseError`

* Bugfixes
    * `:appname` option (typo) #38
    * fixed index creation in `Mongo.GridFs.Bucket`

## 0.6.2 (2019-11-15)

* Enhancements
    * refactored the api of `Mongo.limits` and `Mongo.wire_version`
    * add support for tracking recovery token from response in a sharded transaction

## 0.6.1 (2019-11-01)

* Enhancements
    * refactored `Mongo.Session` and `Mongo.Session.SessionPool` because of poor performance

## 0.6.0 (2019-09-18)

* Enhancements
    * refactored `writeConcern`
    * refactored `filter_nils`
    * refactored usage of `ReadPreference`
    * added support for sessions (`ServerSession`, `SessionPool`, `Session`)
    * added support for transaction
    * added Decimal128 encoder
    * added support for transaction to gridfs and bulk operation
    * added `create` command (explicitly creating a collection or view)

## 0.5.7 (2019-06-25)

* Bug Fixes
   * Test for existing index in `Bucket` works right now

* Enhancements
   * Better handling for the `:timeout` options

## 0.5.6 (2019-06-14)

* Bug Fixes
    * Fixed a match error in `after_fun` of cursor module
    * Fixed a match error in the result of function `Monitor.force_check`
    * Resolved decode problem for the Binary (Old) BinData subtype

* Enhancements
    * Added support for `Mongo.BulkWriteResult`

## 0.5.5 (2019-05-22)

* Bug Fixes
    * Fixed a match error when using Windows OS

## 0.5.4 (2019-05-21)

* Enhancements
   * The driver provides now client metadata
   * Added support for connecting via UNIX sockets (`:socket` and `:socket_dir`)
   * Added support for bulk writes (ordered/unordered, in-memory/stream)
   * Added support for `op_msg` with payload type 1
   * Merged code from https://github.com/ankhers/mongodb/commit/63c20ff7e427744a5df915751adfaf6e5e39ae62
   * Merged changes from https://github.com/ankhers/mongodb/pull/283
   * Merged changes from https://github.com/ankhers/mongodb/pull/281

## 0.5.3 (2019-05-02)

* Enhancements
   * Travis now using the right MongoDB version

* Bug Fixes
   * Added test unit for change streams
   * Removed debug code from change streams

## 0.5.2 (2019-05-01)

* Enhancements
  * Added `op_msg` support ([See](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-msg))
  * Fixed change streams in case of error codes: 11601, 136 and 237 from resuming
  * Reorganized error handling

## 0.5.1 (2019-04-28)

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

## v0.4.7 (2018-09-13)

* Enhancements
  * Added 4.0 to supported versions
  * Initial support for mongodb+srv URLs
  * Support for Decimal128

## v0.4.6 (2018-05-20)

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
