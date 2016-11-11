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
