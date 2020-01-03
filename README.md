# An alternative Mongodb driver for Elixir
[![Build Status](https://travis-ci.org/zookzook/elixir-mongodb-driver.svg?branch=master)](https://travis-ci.org/zookzook/elixir-mongodb-driver)
[![Hex.pm](https://img.shields.io/hexpm/v/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dt/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dw/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dd/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)

## Features

  * Supports MongoDB versions 3.2, 3.4, 3.6, 4.0, 4.2
  * Connection pooling ([through DBConnection 2.x](https://github.com/elixir-ecto/db_connection))
  * Streaming cursors
  * Performant ObjectID generation
  * Aggregation pipeline
  * Replica sets
  * Support for SCRAM-SHA-256 (MongoDB 4.x)
  * Support for change streams api ([See](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst))
  * Support for bulk writes ([See](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#write))
  * support for driver sessions ([See](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst))
  * support for driver transactions ([See](https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst))

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
    decimal128          Decimal{}

1) Since BSON documents are ordered Elixir maps cannot be used to fully represent them. This driver chose to accept both maps and lists of key-value pairs when encoding but will only decode documents to lists. This has the side-effect that it's impossible to discern empty arrays from empty documents. Additionally the driver will accept both atoms and strings for document keys but will only decode to strings.

2) BSON symbols can only be decoded.

## Usage

### Installation:

Add `mongodb_driver` to your mix.exs `deps` and `:applications`.

```elixir
def application do
  [applications: [:mongodb_driver]]
end

defp deps do
  [{:mongodb_driver, "~> 0.6"}]
end
```

Then run `mix deps.get` to fetch dependencies.

```elixir
# Starts an unpooled connection
{:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/db-name")

# Gets an enumerable cursor for the results
cursor = Mongo.find(conn, "test-collection", %{})

cursor
|> Enum.to_list()
|> IO.inspect
```

To specify a username and password, use the `:username`, `:password`, and `:auth_source` options.

```elixir
# Starts an unpooled connection
{:ok, conn} = 
    Mongo.start_link(url: "mongodb://localhost:27017/db-name", 
                     username: "test_user", 
                     password: "hunter2",
                     auth_source: "admin_test")

# Gets an enumerable cursor for the results
cursor = Mongo.find(conn, "test-collection", %{})

cursor
|> Enum.to_list()
|> IO.inspect
```

For secure requests, you may need to add some more options; see the "AWS, TLS and Erlang SSL ciphers" section below.

### Connection pooling
The driver supports pooling by DBConnection (2.x). By default `mongodb_driver` will start a single 
connection, but it also supports pooling with the `:pool_size` option. For 3 connections add the `pool_size: 3` option to `Mongo.start_link` and to all 
function calls in `Mongo` using the pool:

```elixir
# Starts an pooled connection
{:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/db-name", pool_size: 3)

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
    worker(Mongo, [[name: :mongo, database: "test", pool_size: 3]])
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

Due to the mongodb specification, an additional connection is always set up for the monitor process.

### Replica Sets

To connect to a Mongo cluster that is using replica sets, it is recommended to use the `:seeds` list instead 
of a `:hostname` and `:port` pair.

```elixir
{:ok, pid} = Mongo.start_link(database: "test", seeds: ["hostname1.net:27017", "hostname2.net:27017"])
```

This will allow for scenarios where the first `"hostname1.net:27017"` is unreachable for any reason 
and will automatically try to connect to each of the following entries in the list to connect to the cluster.

### Auth mechanisms

For versions of Mongo 3.0 and greater, the auth mechanism defaults to SCRAM. 
If you'd like to use [MONGODB-X509](https://docs.mongodb.com/manual/tutorial/configure-x509-client-authentication/#authenticate-with-a-x-509-certificate) 
authentication, you can specify that as a `start_link` option.

```elixir
{:ok, pid} = Mongo.start_link(database: "test", auth_mechanism: :x509)
```

### AWS, TLS and Erlang SSL ciphers

Some MongoDB cloud providers (notably AWS) require a particular TLS cipher that isn't enabled 
by default in the Erlang SSL module. In order to connect to these services,
you'll want to add this cipher to your `ssl_opts`: 

```elixir
{:ok, pid} = Mongo.start_link(database: "test", 
      ssl_opts: [
        ciphers: ['AES256-GCM-SHA384'],
        cacertfile: "...",
        certfile: "...")
      ]
)
```
### Change streams

Change streams exist in replica set and cluster systems and tell you about changes to collections. 
They work like endless cursors.
The special thing about the change streams is that they are resumable. In the case of a resumable error, 
no exception is made, but the cursor is re-scheduled at the last successful location. 
The following example will never stop, 
so it is a good idea to use a process for change streams. 

```elixir
seeds = ["hostname1.net:27017", "hostname2.net:27017", "hostname3.net:27017"]
{:ok, top} = Mongo.start_link(database: "my-db", seeds: seeds, appname: "getting rich")
cursor =  Mongo.watch_collection(top, "accounts", [], fn doc -> IO.puts "New Token #{inspect doc}" end, max_time: 2_000 )  
cursor |> Enum.each(fn doc -> IO.puts inspect doc end)
```

An example with a spawned process that sends message to the monitor process: 

```elixir
def for_ever(top, monitor) do
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end)
    cursor |> Enum.each(fn doc -> send(monitor, {:change, doc}) end)
end

spawn(fn -> for_ever(top, self()) end)
```

For more information see

* [Mongo.watch_collection](https://hexdocs.pm/mongodb_driver/Mongo.html#watch_collection/5) 


### Bulk writes

The motivation for bulk writes lies in the possibility of optimization, the same operations
to group. Here, a distinction is made between disordered and ordered bulk writes.
In disordered, inserts, updates, and deletes are grouped as individual commands
sent to the database. There is no influence on the order of the execution. 
A good use case is the import of records from one CSV file. 
The order of the inserts does not matter.

For ordered bulk writers, order compliance is important to keep. 
In this case, only the same consecutive operations are grouped.

Currently, all bulk writes are optimized in memory. This is unfavorable for large bulk writes.
In this case, one can use streaming bulk writes that only have a certain set of
group operation in memory and when the maximum number of operations
has been reached, operations are written to the database. The size can be specified.

Using ordered bulk writes. In this example we first insert some dog's name, add an attribute `kind` 
and change all dogs to cats. After that we delete three cats. This example would not work with 
unordered bulk writes. 

```elixir

bulk = "bulk"
       |> OrderedBulk.new()
       |> OrderedBulk.insert_one(%{name: "Greta"})
       |> OrderedBulk.insert_one(%{name: "Tom"})
       |> OrderedBulk.insert_one(%{name: "Waldo"})
       |> OrderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
       |> OrderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
       |> OrderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
       |> OrderedBulk.update_many(%{kind: "dog"}, %{"$set": %{kind: "cat"}})
       |> OrderedBulk.delete_one(%{kind: "cat"})
       |> OrderedBulk.delete_one(%{kind: "cat"})
       |> OrderedBulk.delete_one(%{kind: "cat"})

result = Mongo.BulkWrite.write(:mongo, bulk, w: 1)
```

In the following example we import 1.000.000 integers into the MongoDB using the stream api:

We need to create an insert operation for each number. Then we call the `Mongo.UnorderedBulk.stream`
function to import it. This function returns a stream function which accumulate 
all inserts operations until the limit `1000` is reached. In this case the operation group is send to
MongoDB. So using the stream api you can reduce the memory using while 
importing big volume of data.

```elixir
1..1_000_000 
|> Stream.map(fn i -> Mongo.BulkOps.get_insert_one(%{number: i}) end) 
|> Mongo.UnorderedBulk.write(:mongo, "bulk", 1_000)
|> Stream.run()
```

For more information see:
* [Mongo.UnorderedBulk](https://hexdocs.pm/mongodb_driver/Mongo.UnorderedBulk.html#content) 
* [Mongo.OrderedBulk](https://hexdocs.pm/mongodb_driver/Mongo.OrderedBulk.html#content) 
* [Mongo.BulkWrite](https://hexdocs.pm/mongodb_driver/Mongo.BulkWrite.html#content) 
* [Mongo.BulkOps](https://hexdocs.pm/mongodb_driver/Mongo.BulkOps.html#content) 

and have a look at the test units as well.

### Transactions

Since MongoDB 4.x, transactions for multiple write operations are possible. The [Mongo.Session](https://hexdocs.pm/mongodb_driver/Mongo.Session.html#content)  is responsible for the details and you can use a convenient api for transactions:

```elixir
alias Mongo.Session

{:ok, ids} = Session.with_transaction(top, fn opts ->
{:ok, %InsertOneResult{:inserted_id => id1}} = Mongo.insert_one(top, "dogs", %{name: "Greta"}, opts)
{:ok, %InsertOneResult{:inserted_id => id2}} = Mongo.insert_one(top, "dogs", %{name: "Waldo"}, opts)
{:ok, %InsertOneResult{:inserted_id => id3}} = Mongo.insert_one(top, "dogs", %{name: "Tom"}, opts)
{:ok, [id1, id2, id3]}
end, w: 1)
```

It is also possible to get more control over the progress of the transaction:

```elixir
alias Mongo.Session

{:ok, session} = Session.start_session(top, :write, [])
:ok = Session.start_transaction(session)

Mongo.insert_one(top, "dogs", %{name: "Greta"}, session: session)
Mongo.insert_one(top, "dogs", %{name: "Waldo"}, session: session)
Mongo.insert_one(top, "dogs", %{name: "Tom"}, session: session)

:ok = Session.commit_transaction(session)
:ok = Session.end_session(top, session)
```

For more information see:

* [Mongo.Session](https://hexdocs.pm/mongodb_driver/Mongo.Session.html#content) 

and have a look at the test units as well.

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

## Testing

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


## Special thanks

Special thanks to [JetBrains](https://www.jetbrains.com/?from=elixir-mongodb-driver) for providing a free JetBrains Open Source license for their complete toolbox.

This is an alternative development from the [original](https://github.com/ankhers/mongodb), which was the starting point
and already contained very nice code.

The [Documentation](https://hexdocs.pm/mongodb_driver/readme.html) is online, but currently not up to date. 
This will be done as soon as possible. In the meantime, look in the source code. Especially 
for the individual options.  

## Motivation

  * [x] I have made a number of changes to understand how the driver works. For example, I reduced cursor modules to just one cursor and
        replaced some op code calls with command calls.
  * [x] Simplify code: remove raw_find (raw_find called from cursors, raw_find called with "$cmd"), so raw_find is more calling a command than a find query.
  * [x] Better support for new MongoDB version, for example the ability to use views
  * [x] Upgraded to ([DBConnection 2.x](https://github.com/elixir-ecto/db_connection))
  * [x] Removed depreacated op codes ([See](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#request-opcodes))
  * [x] Added `op_msg` support ([See](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-msg))
  * [x] Added bulk writes ([See](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#write))
  * [x] Add support for driver sessions ([See](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst))
  * [x] Add support for driver transactions ([See](https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst))
  * [ ] Add support for `op_compressed` ([See](https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst))
  
## License

Copyright 2015 Eric Meadows-Jönsson and Justin Wood

Copyright 2019 Michael Maier

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
