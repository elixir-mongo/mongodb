Mongodb
=======

## Features

## Immediate Roadmap

  * Remove database override and multi-auth
  * Bang and non-bang `Mongo` functions
  * Documentation
  * Move low-level API to `Connection` and have `Mongo` be the next-gen driver API
    - Split database and collection actions?
  * Move BSON encoding to client process
  * Replica sets
    - Block in client (and timeout) when waiting for new primary selection
  * New 2.6 write queries and bulk writes
  * Cursors
  * Reconnect backoffs with https://github.com/ferd/backoff
  * Lazy connect

## Tentative Roadmap

  * SSL
  * Use meta-driver test suite
  * Smarter pooling
    - A single connection can serve multiple requests concurrently so traditional pooling (like poolboy) a where single process takes exclusive access of a connection may not fit. Furthermore it is not ideal that long running cursors reserves a connection from the pool for the cursor's full duration. Pooling libraries such as sbroker allows a connection to serve multiple requests, ideally it would be combined with a dispatcher that selects an appropriate connection based on its internal queue.
  * Server selection / Read preference
    - https://www.mongodb.com/blog/post/server-selection-next-generation-mongodb-drivers
    - http://docs.mongodb.org/manual/reference/read-preference

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
