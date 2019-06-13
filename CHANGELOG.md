## v0.5.1

* Enhancements
  * Added support for connecting via UNIX sockets (`:socket` and `:socket_dir`)
  * Start using write commands for acknowledged writes

* Bug Fixes
  * Added missing host parameter in exception call
  * No longer starting `:pool_size` number of connections for internal monitors

## v0.5.0

* Enhancements
  * Added hostname and port to exceptions
  * Added support for x509 authentication
  * Allow passing only partial `read_preference` information
  * Add support for GridFS
  * Update to `db_connection` 2.x

* Bug Fixes
  * Fixed a connection leak
  * Properly parse write concern for URL
  * Properly follow read preference for `secondary_preferred`
  * Fixed an issue where the topology could crash due to a negative timeout

## v0.4.7

* Enhancements
  * Added 4.0 to supported versions
  * Initial support for mongodb+srv URLs
  * Support for Decimal128

## v0.4.6

* Enhancements
  * Added `:connect_timout_ms` to `Mongo.start_link/1`
  * Reorganized documentation

## v0.4.5 (2018-04-08)

* Enhancements
  * Should now be able to send a query to your server before the connection
    is fully made

* Bug Fixes
  * Should actually be able to query for longer than 5 seconds

## v0.4.4 (2018-02-09)

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

## v0.4.3 (2017-09-16)

* Enhancements
  * Send TLS server name indication (SNI) if none is set in the `:ssl_opts`
  * Fixed a couple dialyzer issues
  * Add basic examples of `$and`, `$or`, and `$in` operators in README

* Bug Fixes
  * Ensure cursor requests are routed to the proper node in the cluster
  * No longer attempting to authenticate against arbiter nodes
  * Prevent monitor errors if you have stopped the mongo process

## v0.4.2 (2017-08-28)

* Bug fixes
  * Fix application crash when a replica set member goes offline
  * Fix application crash on start when a replica set member is offline

## v0.4.1 (2017-08-09)

* Bug fixes
  * Monitors no longer use a pool
  * Can now connect to a Mongo instance using a CNAME
  * Pass options through Mongo.aggregate/4

## v0.4.0 (2017-06-07)

* Replica Set Support

## v0.3.0 (2017-05-11)

* Breaking changes
  * Remove `BSON.DateTime` and replace it with native Elixir `DateTime`

## v0.2.1 (2017-05-08)

* Enhancements
  * SSL support
  * Add functions `BSON.DateTime.to_elixir_datetime/1` and `BSON.DateTime.from_elixir_datetime/1`

* Changes
  * Requires Elixir ~> 1.3

## v0.2.0 (2016-11-11)

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

## v0.1.1 (2015-12-17)

* Enhancements
  * Add `BSON.DateTime.from_datetime/1`

* Bug fixes
  * Fix timestamp epoch in generated object ids
  * Fix `Mongo.run_command/3` to accept errors without code

## v0.1.0 (2015-08-25)

Initial release
