# MongoDB

[![Build Status](https://travis-ci.org/ankhers/mongodb.svg?branch=master)](https://travis-ci.org/ankhers/mongodb)

[Documentation for MongoDB is available online](http://hexdocs.pm/mongodb/).

## Features

  * Supports MongoDB versions 3.4, 3.6, 4.0, 4.2 (on 1.0-beta)
  * Connection pooling (through `db_connection`)
  * Streaming cursors
  * Performant ObjectID generation
  * Follows driver specification set by 10gen
  * Safe (by default) and unsafe writes
  * Aggregation pipeline
  * Replica sets
  * Sessions and transactions

## Immediate Roadmap

  * Make sure requests don't go over the 16 MiB limit
  * New 2.6 write queries and bulk writes

## Tentative Roadmap

  * Use meta-driver test suite
  * Server selection / Read preference
    - https://www.mongodb.com/blog/post/server-selection-next-generation-mongodb-drivers
    - http://docs.mongodb.org/manual/reference/read-preference

## Data representation

| BSON             | Elixir
| ---------------- | ------
| double           | `0.0`
| string           | `"Elixir"`
| document         | `[{"key", "value"}]` \| `%{"key" => "value"}` ¹
| binary           | `%BSON.Binary{binary: <<42, 43>>, subtype: :generic}`
| object id        | `%BSON.ObjectId{value: <<...>>}`
| boolean          | `true` | `false`
| UTC datetime     | `%DateTime{}`
| null             | `nil`
| regex            | `%BSON.Regex{pattern: "..."}`
| JavaScript       | `%BSON.JavaScript{code: "..."}`
| integer          | `42`
| symbol           | `"foo"` ²
| min key          | `:BSON_min`
| max key          | `:BSON_max`

¹ Since BSON documents are ordered Elixir maps cannot be used to fully
represent them. This driver chose to accept both maps and lists of key-value
pairs when encoding but will only decode documents to maps. This has the
side-effect that the information about order of keys in a BSON document is lost
when it's decoded. Additionally the driver will accept both atoms and strings
for document keys but will only decode to strings.

² BSON symbols can only be decoded.

### Writing your own encoding info

If you want to write a custom struct to your mongo collection - you can do that
by implementing `Mongo.Encoder` protocol for your class. The output should be a map,
which will be passed to the Mongo database.

Example:

```elixir
defmodule CustomStruct do
  @fields [:a, :b, :c, :id]
  @enforce_keys @fields
  defstruct @fields

  defimpl Mongo.Encoder do
    def encode(%{a: a, b: b, id: id}) do
      %{
        _id: id,
        a: a,
        b: b,
        custom_encoded: true
      }
    end
  end
end
```

So, given the struct:
```elixir
%CustomStruct{a: 10, b: 20, c: 30, id: "5ef27e73d2a57d358f812001"}
```

it will be written to database, as:
```json
{
  "a": 10,
  "b": 20,
  "custom_encoded": true,
  "_id": "5ef27e73d2a57d358f812001"
}
```

## Usage

### Installation:

Add `mongodb` to your `mix.exs` `deps`.

```elixir
defp deps do
  [
    {:mongodb, "~> 0.5.1"}
  ]
end
```

Then run `mix deps.get` to fetch dependencies.

### Connection pooling

By default `mongodb` will start a single connection, but it also supports
pooling with the `:pool_size` option.

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
  children = [
    {Mongo, [name: :mongo, database: "test", pool_size: 2]}
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

Simple start with pooling:

```elixir
{:ok, conn} = Mongo.start_link(name: :mongo, database: "test", pool_size: 2)
```

Operate the `mongodb` with specify pool name in each query:

```elixir
Mongo.find(:mongo, "collection", %{}, limit: 20)
```

More pool options in [here](https://hexdocs.pm/db_connection/2.0.6/DBConnection.html#start_link/2-options).


### Using with MongoDB Ecto

If you're using Mongo with the MongoDB Ecto library, where you have it defined in your config/runtime.exs like this: 

```elixir
config :my_app, MyApp.Repo,
  url: "mongo connection url"
```
  
You'll want to do reference mongo like this: 

```elixir
Mongo.find(MyApp.Repo.pool(), collection, %{_id: %{"$in" =>"some_ids"}})
```

### Replica Sets

To connect to a MongoDB cluster that is using replica sets, it is recommended to
use the `:seeds` list instead of a `:hostname` and `:port` pair.

```elixir
{:ok, pid} = Mongo.start_link(database: "test", seeds: ["hostname1.net:27017", "hostname2.net:27017"])
```

This will allow for scenarios where the first `"hostname1.net:27017"` is
unreachable for any reason and will automatically try to connect to each of the
following entries in the list to connect to the cluster.

### Auth mechanisms

For versions of MongoDB 3.0 and greater, the auth mechanism defaults to SCRAM.
If you'd like to use [MONGODB-X509][] authentication, you can specify that as a
`start_link` option.

```elixir
{:ok, pid} = Mongo.start_link(database: "test", auth_mechanism: :x509)
```

### AWS, TLS and Erlang SSL ciphers

Some MongoDB cloud providers (notably AWS) require a particular TLS cipher that
isn't enabled by default in the Erlang SSL module. In order to connect to these
services, you'll want to add this cipher to your `ssl_opts`:

```elixir
{:ok, pid} = Mongo.start_link(database: "test",
      ssl_opts: [
        ciphers: ['AES256-GCM-SHA384'],
        cacertfile: "...",
        certfile: "...")
      ]
)
```

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
the SSL tests or enable SSL on your MongoDB server.

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

Copyright 2015 Justin Wood and Kobil Systems GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[MONGODB-X509]: https://docs.mongodb.com/manual/tutorial/configure-x509-client-authentication/#authenticate-with-a-x-509-certificate
