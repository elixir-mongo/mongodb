Mongodb
=======

[![Build Status](https://travis-ci.org/ericmj/mongodb.svg?branch=master)](https://travis-ci.org/ericmj/mongodb)
[![Inline docs](http://inch-ci.org/github/ericmj/mongodb.svg)](http://inch-ci.org/github/ericmj/mongodb)

## Features

  * Supports MongoDB versions 2.4, 2.6, 3.0
  * Connection pooling
  * Streaming cursors
  * Performant ObjectID generation
  * Follows driver specification set by 10gen
  * Safe (by default) and unsafe writes
  * Aggregation pipeline

## Immediate Roadmap

  * Add timeouts for all calls
  * Bang and non-bang `Mongo` functions
  * Move BSON encoding to client process
    - Make sure requests don't go over the 16mb limit
  * Replica sets
    - Block in client (and timeout) when waiting for new primary selection
  * New 2.6 write queries and bulk writes
  * Reconnect backoffs with https://github.com/ferd/backoff
  * Lazy connect
  ? Drop save_* because it was dropped by driver specs

## Tentative Roadmap

  * SSL
  * Use meta-driver test suite
  * Server selection / Read preference
    - https://www.mongodb.com/blog/post/server-selection-next-generation-mongodb-drivers
    - http://docs.mongodb.org/manual/reference/read-preference

## Usage

### Connection Pools

```elixir
defmodule MongoPool do
  use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy
end

# Starts the pool named MongoPool
{:ok, _} = MongoPool.start_link(database: "test")

# Gets an enumerable cursor for the results
cursor = Mongo.find(MongoPool, "test-collection", %{})

Enum.to_list(cursor)
|> IO.inspect
```

### APIs
```elixir
Mongo.find(MongoPool, "test-collection", %{}, limit: 20)
Mongo.find(MongoPool, "test-collection", %{"field" => %{"$gt" => 0}}, limit: 20, sort: %{"field" => 1})

Mongo.insert_one(MongoPool, "test-collection", %{"field" => 10})

Mongo.insert_many(MongoPool, "test-collection", [%{"field" => 10}, %{"field" => 20}])

Mongo.delete_one(MongoPool, "test-collection", %{"field" => 10})

Mongo.delete_many(MongoPool, "test-collection", %{"field" => 10})
```

### Run on a single pool connection
```elixir
# Gets a pool process (conn) to run queries on
MongoPool.run(fn (conn) ->
  # Removes 1 result using the query
  Mongo.Connection.remove(conn, "test-collection", %{"field" => 1})

  # Removes all results using the query
  Mongo.Connection.remove(conn, "test-collection", %{"field" => 1, "otherfield" => 1}, multi: true)
end)
```

## License

Copyright 2015 Eric Meadows-Jönsson

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
