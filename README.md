# Mongodb

[![Build Status](https://travis-ci.org/ankhers/mongodb.svg?branch=master)](https://travis-ci.org/ankhers/mongodb)

## Features

  * Supports MongoDB versions 2.4, 2.6, 3.0, 3.2, 3.4
  * Connection pooling (through db_connection)
  * Streaming cursors
  * Performant ObjectID generation
  * Follows driver specification set by 10gen
  * Safe (by default) and unsafe writes
  * Aggregation pipeline
  * Replica sets

## Immediate Roadmap

  * Make sure requests don't go over the 16mb limit
  * New 2.6 write queries and bulk writes

## Tentative Roadmap

  * Use meta-driver test suite
  * Server selection / Read preference
    - https://www.mongodb.com/blog/post/server-selection-next-generation-mongodb-drivers
    - http://docs.mongodb.org/manual/reference/read-preference

## Data representation

    BSON                Elixir
    ----------          ------
    double              0.0
    string              "Elixir"
    document            [{"key", "value"}] | %{"key" => "value"} (1)
    binary              %BSON.Binary{binary: <<42, 43>>, subtype: :generic}
    object id           %BSON.ObjectId{value: <<...>>}
    boolean             true | false
    UTC datetime        %DateTime{}
    null                nil
    regex               %BSON.Regex{pattern: "..."}
    JavaScript          %BSON.JavaScript{code: "..."}
    integer             42
    symbol              "foo" (2)
    min key             :BSON_min
    max key             :BSON_max

1) Since BSON documents are ordered Elixir maps cannot be used to fully represent them. This driver chose to accept both maps and lists of key-value pairs when encoding but will only decode documents to lists. This has the side-effect that it's impossible to discern empty arrays from empty documents. Additionally the driver will accept both atoms and strings for document keys but will only decode to strings.

2) BSON symbols can only be decoded.

## Usage

### Installation:

Add mongodb to your mix.exs `deps` and `:applications` (replace `>= 0.0.0` in `deps` if you want a specific version). Mongodb supports the same pooling libraries db_connection does (currently: no pooling, poolboy, and sbroker). If you want to use poolboy as pooling library you should set up your project like this:

```elixir
def application do
  [applications: [:mongodb, :poolboy]]
end

defp deps do
  [{:mongodb, ">= 0.0.0"},
   {:poolboy, ">= 0.0.0"}]
end
```

Then run `mix deps.get` to fetch dependencies.

### Connection pooling

By default mongodb will start a single connection, but it also supports pooling with the `:pool` option. For poolboy add the `pool: DBConnection.Poolboy` option to `Mongo.start_link` and to all function calls in `Mongo` using the pool.

```elixir
# Starts an unpooled connection
{:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/db-name")

# Gets an enumerable cursor for the results
cursor = Mongo.find(conn, "test-collection", %{})

cursor
|> Enum.to_list()
|> IO.inspect
```

If you're using pooling it is recommend to add it to your application supervisor:

```elixir
def start(_type, _args) do
  import Supervisor.Spec

  children = [
    worker(Mongo, [[name: :mongo, database: "test", pool: DBConnection.Poolboy]])
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

Then you can use the pool as following:

```elixir
Mongo.find(:mongo, "collection", %{}, limit: 20, pool: DBConnection.Poolboy)
```

### Replica Sets

To connect to a Mongo cluster that is using replica sets, it is recommended to use the `:seeds` list instead of a `:hostname` and `:port` pair.

```elixir
{:ok, pid} = Mongo.start_link(database: "test", seeds: ["hostname1.net:27017", "hostname2.net:27017"])
```

This will allow for scenarios where the first `"hostname1.net:27017"` is unreachable for any reason and will automatically try to connect to each of the following entries in the list to connect to the cluster.

### Examples

Using `$and`

```elixir
Mongo.find(:mongo, "users", %{"$and" => [%{email: "my@email.com"}, %{first_name: "first_name"}]})
```

Using `$or`

```elixir
Mongo.find(:mongo, "users", %{"$or" => [%{email: "my@email.com"}, %{first_name: "first_name"}]})
```

Using `$in`

```elixir
Mongo.find(:mongo, "users", %{email: %{"$in" => ["my@email.com", "other@email.com"]}})
```

## Contributing

The SSL test suite is enabled by default. You have two options. Either exclude
the SSL tests or enable SSL on your Mongo server.

### Disable the SSL tests

`mix test --exclude ssl`

### Enable SSL on your Mongo server

```bash
$ openssl req -newkey rsa:2048 -new -x509 -days 365 -nodes -out mongodb-cert.crt -keyout mongodb-cert.key
$ cat mongodb-cert.key mongodb-cert.crt > mongodb.pem
$ mongod --sslMode allowSSL --sslPEMKeyFile /path/to/mongodb.pem
```

* For `--sslMode` you can use one of `allowSSL` or `preferSSL`
* You can enable any other options you want when starting `mongod`

## License

Copyright 2015 Justin Wood

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
