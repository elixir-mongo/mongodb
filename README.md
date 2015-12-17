Mongodb
=======

[![Build Status](https://travis-ci.org/ericmj/mongodb.svg?branch=master)](https://travis-ci.org/ericmj/mongodb)
[![Inline docs](http://inch-ci.org/github/ericmj/mongodb.svg)](http://inch-ci.org/github/ericmj/mongodb)

## Features

  * Supports MongoDB versions 2.4, 2.6, 3.0, 3.2
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

## Data representation

    BSON             	Elixir
    ----------        	------
    double              0.0
    string              "Elixir"
    document            [{"key", "value"}] | %{"key" => "value"} *
    binary              %BSON.Binary{binary: <<42, 43>>, subtype: :generic}
    object id           %BSON.ObjectId{value: <<...>>}
    boolean             true | false
    UTC datetime        %BSON.DateTime{utc: ...}
    null                nil
    regex               %BSON.Regex{pattern: "..."}
    JavaScript          %BSON.JavaScript{code: "..."}
    integer             42
    symbol              "foo" **
    min key             :BSON_min
    max key             :BSON_max

* Since BSON documents are ordered Elixir maps cannot be used to fully represent them. This driver chose to accept both maps and lists of key-value pairs when encoding but will only decode documents to lists. This has the side-effect that it's impossible to discern empty arrays from empty documents. Additionally the driver will accept both atoms and strings for document keys but will only decode to strings.

** BSON symbols can only be decoded.

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

### Examples
```elixir
Mongo.find(MongoPool, "test-collection", %{}, limit: 20)
Mongo.find(MongoPool, "test-collection", %{"field" => %{"$gt" => 0}}, limit: 20, sort: %{"field" => 1})

Mongo.insert_one(MongoPool, "test-collection", %{"field" => 10})

Mongo.insert_many(MongoPool, "test-collection", [%{"field" => 10}, %{"field" => 20}])

Mongo.delete_one(MongoPool, "test-collection", %{"field" => 10})

Mongo.delete_many(MongoPool, "test-collection", %{"field" => 10})
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
