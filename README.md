# An Alternative Elixir Driver for MongoDB
[![Build Status](https://travis-ci.org/zookzook/elixir-mongodb-driver.svg?branch=master)](https://travis-ci.org/zookzook/elixir-mongodb-driver)
[![Coverage Status](https://coveralls.io/repos/github/zookzook/elixir-mongodb-driver/badge.svg?branch=master)](https://coveralls.io/github/zookzook/elixir-mongodb-driver?branch=master)
[![Hex.pm](https://img.shields.io/hexpm/v/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dt/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dw/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dd/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)

## Features

  * Supports MongoDB versions 3.2, 3.4, 3.6, 4.0, 4.2, 4.4
  * Connection pooling ([through DBConnection 2.x](https://github.com/elixir-ecto/db_connection))
  * Streaming cursors
  * Performant ObjectID generation
  * Aggregation pipeline
  * Replica sets
  * Support for SCRAM-SHA-256 (MongoDB 4.x)
  * Support for GridFS ([See](https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst))
  * Support for change streams api ([See](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst))
  * Support for bulk writes ([See](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#write))
  * support for driver sessions ([See](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst))
  * support for driver transactions ([See](https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst))
  * support for command monitoring ([See](https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst))
  * support for retryable reads ([See](https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst))
  * support for retryable writes ([See](https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst))

## Data Representation

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

Since BSON documents are ordered Elixir maps cannot be used to fully represent them. This driver chose to accept both maps and lists of key-value pairs when encoding but will only decode documents to lists. This has the side-effect that it's impossible to discern empty arrays from empty documents. Additionally the driver will accept both atoms and strings for document keys but will only decode to strings.

BSON symbols can only be decoded.

## Usage

### Installation

Add `mongodb_driver` to your mix.exs `deps`.

```elixir
defp deps do
  [{:mongodb_driver, "~> 0.6"}]
end
```

Then run `mix deps.get` to fetch dependencies.

### Simple Connection to MongoDB

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

Failing operations return a `{:error, error}` tuple where `error` is a
`Mongo.Error` object:

```elixir
{:error,
 %Mongo.Error{
   code: 13435,
   error_labels: [],
   host: nil,
   message: "not master and slaveOk=false",
   resumable: true,
   retryable_reads: true,
   retryable_writes: true
 }}
```

### Connection Pooling
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

By default, the driver will discover the deployment's topology and will connect
to the replica set automatically, using either the seed list syntax or the URI
syntax. Assuming the deployment has nodes at `hostname1.net:27017`,
`hostname2.net:27017` and `hostname3.net:27017`, either of the following
invocations will discover the entire deployment:

```elixir
{:ok, pid} = Mongo.start_link(database: "test", seeds: ["hostname1.net:27017"])

{:ok, pid} = Mongo.start_link(url: "mongodb://hostname1.net:27017/test")
```

To ensure that the connection succeeds even when some of the nodes are not
available, it is recommended to list all nodes in both the seed list and the
URI, as follows:

```elixir
{:ok, pid} = Mongo.start_link(database: "test", seeds: ["hostname1.net:27017", "hostname2.net:27017", "hostname3.net:27017"])

{:ok, pid} = Mongo.start_link(url: "mongodb://hostname1.net:27017,hostname2.net:27017,hostname3.net:27017/test")
```

Using an SRV URI also discovers all nodes of the deployment automatically.

### Auth Mechanisms

For versions of Mongo 3.0 and greater, the auth mechanism defaults to SCRAM. 
If you'd like to use [MONGODB-X509](https://docs.mongodb.com/manual/tutorial/configure-x509-client-authentication/#authenticate-with-a-x-509-certificate) 
authentication, you can specify that as a `start_link` option.

```elixir
{:ok, pid} = Mongo.start_link(database: "test", auth_mechanism: :x509)
```

### AWS, TLS and Erlang SSL Ciphers

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

### Find

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

### Change Streams

Change streams are available in replica set and sharded cluster deployments
and tell you about changes to documents in collections. They work like endless
cursors.

The special thing about change streams is that they are resumable: in case of
a resumable error, no exception is propagated to the application, but instead
the cursor is re-scheduled at the last successful location. 

The following example will never stop, thus it is a good idea to use a process
for reading from change streams:

```elixir
seeds = ["hostname1.net:27017", "hostname2.net:27017", "hostname3.net:27017"]
{:ok, top} = Mongo.start_link(database: "my-db", seeds: seeds, appname: "getting rich")
cursor =  Mongo.watch_collection(top, "accounts", [], fn doc -> IO.puts "New Token #{inspect doc}" end, max_time: 2_000 )  
cursor |> Enum.each(fn doc -> IO.puts inspect doc end)
```

An example with a spawned process that sends messages to the monitor process: 

```elixir
def for_ever(top, monitor) do
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end)
    cursor |> Enum.each(fn doc -> send(monitor, {:change, doc}) end)
end

spawn(fn -> for_ever(top, self()) end)
```

For more information see:

* [Mongo.watch_collection](https://hexdocs.pm/mongodb_driver/Mongo.html#watch_collection/5) 

### Inserts

To insert a single document:

```elixir
Mongo.insert_one(top, "users", %{first_name: "John", last_name: "Smith"})
```

To insert a list of documents:

```elixir
Mongo.insert_many(top, "users", [
  %{first_name: "John", last_name: "Smith"},
  %{first_name: "Jane", last_name: "Doe"}
])
```

### Indexes

To create indexes you can call the function `Mongo.createIndexed/4`:

```elixir
indexes =  [[key: [files_id: 1, n: 1], name: "files_n_index", unique: true]]
Mongo.create_indexes(topology_pid, "my_collection", indexes, opts)
```

You specify the `indexes` parameter as a keyword list with all options described in the documentation of the [createIndex](https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes) command.

For more information see:
* [Mongo.create_indexes](https://hexdocs.pm/mongodb_driver/Mongo.html#create_indexes/4) 
* [Mongo.drop_index](https://hexdocs.pm/mongodb_driver/Mongo.html#drop_index/4)
 
### Bulk Writes

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

### GridFS

The driver supports the GridFS specifications. You create a `Mongo.GridFs.Bucket`
struct and with this struct you can upload and download files. For example:

```elixir
    bucket = Bucket.new(top)
    upload_stream = Upload.open_upload_stream(bucket, "test.jpg")
    src_filename = "./test/data/test.jpg"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id
```

In the example a new bucket with default values is used to upload a file from the file system (`./test/data/test.jpg`) to the MongoDB (using the name `test.jpg`). The `upload_stream` struct contains the id of the new file which can be used to download the stored file. The following code fragments downloads the file by using the `file_id`.

```elixir
    dest_filename = "/tmp/my-test-file.jps"

    with {:ok, stream} <- Mongo.GridFs.Download.open_download_stream(bucket, file_id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end
```

For more information see:
 * [Mongo.GridFs.Bucket](https://hexdocs.pm/mongodb_driver/Mongo.GridFs.Bucket.html#content) 
 * [Mongo.GridFs.Download](https://hexdocs.pm/mongodb_driver/Mongo.GridFs.Download.html#content) 
 * [Mongo.GridFs.Upload](https://hexdocs.pm/mongodb_driver/Mongo.GridFs.Upload.html#content) 

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


### Command Monitoring

You can watch all events that are triggered while the driver send requests and processes responses. You can use the 
`Mongo.EventHandler` as a starting point. It logs the events from the topic `:commands` (by ignoring the `:isMaster` command)
to `Logger.info`:

```elixir
iex> Mongo.EventHandler.start()
iex> {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test")
{:ok, #PID<0.226.0>}
iex> Mongo.find_one(conn, "test", %{})
[info] Received command: %Mongo.Events.CommandStartedEvent{command: [find: "test", ...
[info] Received command: %Mongo.Events.CommandSucceededEvent{command_name: :find, ...
```

## Testing and Travis CI

The `travis.yml` file uses only the latest MongoDB. It creates a replica set of three nodes and disables the SSL test case. If you want to
run the test cases against other MongoDB deployments or older versions, you can use the [mtools](https://github.com/rueckstiess/mtools) for deployment and run the test cases locally:

```bash
pyenv global 3.6
pip3 install --upgrade pip
pip3 install mtools[all]
export PATH=to-your-mongodb/bin/:$PATH
mlaunch init --setParameter enableTestCommands=1 --replicaset --name "rs_1"
mix test --exclude ssl --exclude socket
``` 

The SSL test suite is disabled by default.

### Enable the SSL Tests

`mix test --exclude ssl`

### Enable SSL on Your MongoDB Server

```bash
$ openssl req -newkey rsa:2048 -new -x509 -days 365 -nodes -out mongodb-cert.crt -keyout mongodb-cert.key
$ cat mongodb-cert.key mongodb-cert.crt > mongodb.pem
$ mongod --sslMode allowSSL --sslPEMKeyFile /path/to/mongodb.pem
```

* For `--sslMode` you can use one of `allowSSL` or `preferSSL`
* You can enable any other options you want when starting `mongod`

## More Examples

There are some basic examples in the `example` folder. But if you want to see the driver in action 
take a look at [Vega](https://github.com/zookzook/vega), especially the [Board.ex](https://github.com/zookzook/vega/blob/master/lib/vega/board.ex) module for using the transaction api together with
bulk operations.

## Special Thanks

Special thanks to [JetBrains](https://www.jetbrains.com/?from=elixir-mongodb-driver) for providing a free JetBrains Open Source license for their complete toolbox.

The [Documentation](https://hexdocs.pm/mongodb_driver/readme.html) is online, but currently not up to date. 
This will be done as soon as possible. In the meantime, look in the source code. Especially 
for the individual options.  

This driver is based on the [original Elixir driver for MongoDB](https://github.com/ankhers/mongodb).
 
## License

Copyright 2015 Eric Meadows-Jönsson and Justin Wood

Copyright 2019 - 2020 Michael Maier

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
