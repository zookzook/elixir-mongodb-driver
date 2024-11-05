# The Elixir Driver for MongoDB

[![Hex.pm](https://img.shields.io/hexpm/v/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/mongodb_driver/)
[![Hex.pm](https://img.shields.io/hexpm/dt/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dw/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![Hex.pm](https://img.shields.io/hexpm/dd/mongodb_driver.svg)](https://hex.pm/packages/mongodb_driver)
[![License](https://img.shields.io/hexpm/l/mongodb_driver.svg)](https://github.com/zookzook/elixir-mongodb-driver/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/zookzook/elixir-mongodb-driver.svg)](https://github.com/zookzook/elixir-mongodb-driver/commits/master)

## Features

- supports MongoDB versions 4.x, 5.x, 6.x, 7.x, 8.x
- connection pooling ([through DBConnection 2.x](https://github.com/elixir-ecto/db_connection))
- streaming cursors
- performant ObjectID generation
- aggregation pipeline
- replica sets
- support for SCRAM-SHA-256 (MongoDB 4.x)
- support for GridFS ([See](https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.md))
- support for change streams api ([See](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md))
- support for bulk writes ([See](https://github.com/mongodb/specifications/blob/master/source/crud/crud.md#write))
- support for driver sessions ([See](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.md))
- support for driver transactions ([See](https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.md))
- support for command monitoring ([See](https://github.com/mongodb/specifications/blob/master/source/command-logging-and-monitoring/command-logging-and-monitoring.md))
- support for retryable reads ([See](https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.md))
- support for retryable writes ([See](https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.md))
- support for simple structs using the Mongo.Encoder protocol
- support for complex and nested documents using the `Mongo.Collection` macros
- support for streaming protocol ([See](https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.md#streaming-protocol))
- support for migration scripts
- support for compression for zlib and zstd ([See](https://github.com/mongodb/specifications/blob/07b7649cc5c805ef4f85fccddf39226add7114e6/source/compression/OP_COMPRESSED.md))

## Usage

### Installation

Add `mongodb_driver` to your mix.exs `deps`.

```elixir
defp deps do
  [{:mongodb_driver, "~> 1.5.0"}]
end
```

Then run `mix deps.get` to fetch dependencies.

### Simple Connection to MongoDB

```elixir
# Starts an unpooled connection
{:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/my-database")

top
|> Mongo.find("test-collection", %{})
|> Enum.to_list()
```

To specify a username and password, use the `:username`, `:password`, and `:auth_source` options.

```elixir
# Starts an unpooled connection
{:ok, top} =
    Mongo.start_link(url: "mongodb://localhost:27017/db-name",
                     username: "test_user",
                     password: "hunter2",
                     auth_source: "admin_test")

top
|> Mongo.find("test-collection", %{})
|> Enum.to_list()
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

## Examples

### Find

Using `$and`

```elixir
@topology
|> Mongo.find("users", %{"$and" => [%{email: "my@email.com"}, %{first_name: "first_name"}]})
|> Enum.to_list()
```

Using `$or`

```elixir
@topology
|> Mongo.find("users", %{"$or" => [%{email: "my@email.com"}, %{first_name: "first_name"}]})
|> Enum.to_list()
```

Using `$in`

```elixir
@topology
|> Mongo.find("users", %{email: %{"$in" => ["my@email.com", "other@email.com"]}})
|> Enum.to_list()
```

## How to use the `Mongo.Stream`?

Most query functions return a `Mongo.Stream` struct that implements the `Enumerable` protocol. The module checks out 
the session and streams the batches from the server until the last batch has been received. 
The session is then checked in for reuse. [Sessions](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.md) are 
temporary and reusable data structures, e.g. to support transactions. They are required by the Mongo DB driver specification.

The use of internal structures of the `Mongo.Stream` struct is therefore not planned. For example, the following code results in an open session and the `docs` will only contain the first batch:

```elixir
%Mongo.Stream{docs: docs} = Mongo.aggregate(@topology, collection, pipeline, opts)
Enum.map(docs, fn elem -> elem end)
```

The `Mongo.Stream` struct should therefore always be processed by an `Enum` or `Stream` function so that the session management 
can take place automatically:

```elixir
@topology
|> Mongo.aggregate(collection, pipeline, opts)
|> Enum.to_list()
```

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

## mongodb_ecto

The version 1.4.0 supports the [mongodb_ecto](https://github.com/elixir-mongo/mongodb_ecto) package.
A series of changes are required to support the adapter. Some BSON encoders and a missing generic update function were added for the adapter.
Most notably, the `find-then-modify` command functions `find_one_and_update` and `find_one_and_replace` now return appropriate
`FindAndModifyResult` structs that contain additional write information otherwise neglected, which the adapter requires.

After upgrading the driver to version 1.4.0 you need to change the code regarding the results of
* `Mongo.find_one_and_update`
* `Mongo.find_one_and_replace`

## Data Representation

This driver chooses to accept both maps and lists of key-value tuples when encoding BSON documents (1), but will only
decode documents into maps. Maps are convenient to work with, but Elixir map keys are not ordered, unlike BSON document
keys.

That design decision means document key order is lost when encoding Elixir maps to BSON and, conversely, when decoding
BSON documents to Elixir maps. However, see [Preserve Document Key Order](#preserve-document-key-order) to learn how to
preserve key order when it matters.

Additionally, the driver accepts both atoms and strings for document keys, but will only decode them into strings.
Creating atoms from arbitrary input (such as database documents) is
[discouraged](https://elixir-lang.org/getting-started/mix-otp/genserver.html#:~:text=However%2C%20naming%20dynamic,our%20system%20memory!)
because atoms are not garbage collected.

[BSON symbols (deprecated)](https://bsonspec.org/spec.html#:~:text=Symbol.%20%E2%80%94%20Deprecated) can only be decoded (2).

    BSON                Elixir
    ----------          ------
    double              0.0
    string              "Elixir"
    document            [{"key", "value"}] | %{"key" => "value"} (1)
    binary              %BSON.Binary{binary: <<42, 43>>, subtype: :generic}
    UUID                %BSON.Binary{binary: <<42, 43>>, subtype: :uuid}
    UUID (old style)    %BSON.Binary{binary: <<42, 43>>, subtype: :uuid_old}
    object id           %BSON.ObjectId{value: <<...>>}
    boolean             true | false
    UTC datetime        %DateTime{}
    null                nil
    regex               %BSON.Regex{pattern: "..."}
    JavaScript          %BSON.JavaScript{code: "..."}
    timestamp           #BSON.Timestamp<value:ordinal>"
    integer 32          42
    integer 64          #BSON.LongNumber<value>
    symbol              "foo" (2)
    min key             :BSON_min
    max key             :BSON_max
    decimal128          Decimal{}

## Preserve Document Key Order

### Encoding from Elixir to BSON

For some MongoDB operations, the order of the keys in a document affect the result. For example, that is the case when
sorting a query by multiple fields.

In those cases, driver users should represent documents using a list of tuples (or a keyword list) to preserve the
order. Example:

```elixir
@topology
|> Mongo.find("users", %{}, sort: [last_name: 1, first_name: 1, _id: 1])
|> Enum.to_list()
```

The query above will sort users by last name, then by first name and finally by ID. If an Elixir map had been used to
specify `:sort`, query results would end up sorted unexpectedly wrong.

### Decoding from BSON to Elixir

Decoded BSON documents are always represented by Elixir maps because the driver depends on that to implement its
functionality.

If the order of document keys as stored by MongoDB is needed, the driver can be configured to use a BSON decoder module
that puts a list of keys in the original order under the `:__order__` key (and it works recursively).

```elixir
config :mongodb_driver,
  decoder: BSON.PreserveOrderDecoder
```

It is possible to customize the key. For example, to use `:original_order` instead of the default `:__order__`:

```elixir
config :mongodb_driver,
  decoder: {BSON.PreserveOrderDecoder, key: :original_order}
```

The resulting maps with annotated key order can be recursively transformed into lists of tuples. That allows for
preserving the order again when encoding. Here is an example of how to achieve that:

```elixir
defmodule MapWithOrder do
  def to_list(doc, order_key \\ :__order__) do
    do_to_list(doc, order_key)
  end

  defp do_to_list(%{__struct__: _} = elem, _order_key) do
    elem
  end

  defp do_to_list(doc, order_key) when is_map(doc) do
    doc
    |> Map.get(order_key, Map.keys(doc))
    |> Enum.map(fn key -> {key, do_to_list(Map.get(doc, key), order_key)} end)
  end

  defp do_to_list(xs, order_key) when is_list(xs) do
    Enum.map(xs, fn elem -> do_to_list(elem, order_key) end)
  end

  defp do_to_list(elem, _order_key) do
    elem
  end
end

# doc = ...
MapWithOrder.to_list(doc)
```

Note that structs are kept as-is, to handle special values such as `BSON.ObjectId`.

The decoder module is defined at compile time. The default decoder is `BSON.Decoder`, which does not preserve document
key order. As it needs to execute fewer operations when decoding data received from MongoDB, it offers improved
performance. Therefore, the default decoder is recommended for most use cases of this driver.

## Writing your own encoding info

If you want to write a custom struct to your mongo collection - you can do that
by implementing `Mongo.Encoder` protocol for your module. The output should be a map,
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

## Collections

While using the `Mongo.Encoder` protocol give you the possibility to encode your structs into maps the opposite way to decode those maps into structs is missing. To handle it you can use the `Mongo.Collection` which provides some boilerplate code for a better support of structs while using the MongoDB driver

- automatic load and dump function
- reflection functions
- type specification
- support for embedding one and many structs
- support for `after load` function
- support for `before dump` function
- support for id generation
- support for default values
- support for derived values
- support for alias attribute names

But in the case of queries and updates, a rewrite of the attribute names does not take place. It is still up to you
to use the correct attribute names.

When using the MongoDB driver only maps and keyword lists are used to represent documents.
If you prefer to use structs instead of the maps to give the document a stronger meaning or to emphasize
its importance, you have to create a `defstruct` and fill it from the map manually:

```elixir
defmodule Label do
  defstruct name: "warning", color: "red"
end

iex> label_map = Mongo.find_one(:mongo, "labels", %{})
  %{"name" => "warning", "color" => "red"}
iex> label = %Label{name: label_map["name"], color: label_map["color"]}
```

We have defined a module `Label` as `defstruct`, then we get the first label document
the collection `labels`. The function `find_one` returns a map. We convert the map manually and
get the desired struct. If we want to save a new structure, we have to do the reverse. We convert the struct into a map:

```elixir
iex> label = %Label{}
iex> label_map = %{"name" => label.name, "color" => label.color}
iex> {:ok, _} = Mongo.insert_one(:mongo, "labels", label_map)
```

Alternatively, you can also remove the `__struct__` key from `label`. The MongoDB driver automatically
converts the atom keys into strings (Or use the `Mongo.Encode` protocol)

```elixir
iex>  Map.drop(label, [:__struct__])
%{color: :red, name: "warning"}
```

If you use nested structures, the work becomes a bit more complex. In this case, you have to use the inner structures
convert manually, too. If you take a closer look at the necessary work, two basic functions can be derived:

- `load` Conversion of the map into a struct.
- `dump` Conversion of the struct into a map.

`Mongo.Collection` provides the necessary macros to automate this boilerplate code. The above example can be rewritten as follows:

```elixir
defmodule Label do
    use Mongo.Collection

    document do
      attribute :name, String.t(), default: "warning"
      attribute :color, String.t(), default: :red
    end
end
```

This results in the following module:

```elixir
defmodule Label do

    defstruct [name: "warning", color: "red"]

    @type t() :: %Label{String.t(), String.t()}

    def new()...
    def load(map)...
    def dump(%Label{})...
    def __collection__(:attributes)...
    def __collection__(:types)...
    def __collection__(:collection)...
    def __collection__(:id)...

end
```

You can now create new structs with the default values and use the conversion functions between map and structs:

```elixir
iex(1)> x = Label.new()
%Label{color: :red, name: "warning"}
iex(2)> m = Label.dump(x)
%{color: :red, name: "warning"}
iex(3)> Label.load(m, true)
%Label{color: :red, name: "warning"}
```

The `load/2` function distinguishes between keys of type binarys `load(map, false)` and keys of type atoms `load(map, true)`. The default is `load(map, false)`:

```elixir
iex(1)> m = %{"color" => :red, "name" => "warning"}
iex(2)> Label.load(m)
%Label{color: :red, name: "warning"}
```

If you would now expect atoms as keys, the result of the conversion is not correct in this case:

```elixir
iex(3)> Label.load(m, true)
%Label{color: nil, name: nil}
```

The background is that MongoDB always returns binarys as keys and structs use atoms as keys.
For more information look at the module documentation `Mongo.Collection`.
Of course, using the `Mongo.Collection` is not free. When loading and saving, the maps are converted into structures, which increases CPU usage somewhat. When it comes to speed, it is better to use the maps directly.

## Breaking changes

Prior to version 0.9.2 the dump function returns atoms as key. Since the `dump/1` function is the inverse function of `load/1`,
which uses binary keys as default, the `dump/1` function should return binary keys as well. This increases the consistency and
you can do:

    l = Label.load(doc)
    doc = Label.dump(l)
    assert l == Label.load(doc)

## Using the Repo Module

For convenience, you can also `use` the `Mongo.Repo` module in your application to configure the MongoDB application.

Simply create a new module and include the `use Mongo.Repo` macro:

```elixir
defmodule MyApp.Repo do
  use Mongo.Repo,
    otp_app: :my_app,
    topology: :mongo
end
```

To configure the MongoDB add the configuration to your `config.exs`:

```elixir
config :my_app, MyApp.Repo,
  url: "mongodb://localhost:27017/my-app-dev",
  timeout: 60_000,
  idle_interval: 10_000,
  queue_target: 5_000
```

Finally, we can add the `Mongo.Repo` instance to our application supervision tree:

```elixir
  children = [
    # ...
    MyApp.Repo,
    # ...
  ]
```

In addition, the convenient configuration, the `Mongo.Repo` module will also include query functions to use with your
`Mongo.Collection` modules.

For more information check out the `Mongo.Repo` module documentation and the `Mongo` module documentation.

## Breaking changes

Prior to version 0.9.2 some `Mongo.Repo` functions use the `dump/1` function for the query (and update) parameter. 
This worked only for some query that used only the attributes of the document. In the case of nested documents, 
it didn't work, so it is changed to be more consistent. The `Mongo.Repo` module is very simple without any query 
rewriting like Ecto does. In the case you want to use the `:name` option, you need to specify the query and update 
documents in the `Mongo.Repo` functions following the specification in the MongoDB. Example:

    defmodule MyApp.Session do
        @moduledoc false
        use Mongo.Collection
        
        alias BSON.Binary
        
        collection :s do
            attribute :uuid, Binary.t(), name: :u 
        end
    end

If you use the `Mongo.Repo` module and want to fetch a specific session document, this won't work:

    MyApp.Repo.get_by(MyApp.Session, %{uuid: session_uuid})

because the `get_by/2` function uses the query parameter without any rewriting. You need to change the query:

    MyApp.Repo.get_by(MyApp.Session, %{u: session_uuid})

A rewriting is too complex for now because MongoDB has a lot of options. 

## Logging

You config the logging output by adding in your config file this line

```elixir
config :mongodb_driver, log: true
```

The attribute `log` supports `true`, `false` or a log level like `:info`. The default value is `false`. If you turn
logging on, then you will see log output (command, collection, parameters):

```
[info] CMD find "my-collection" [filter: [name: "Helga"]] db=2.1ms
```

## Telemetry

The driver uses the [:telemetry](https://github.com/beam-telemetry/telemetry) package to emit the execution duration
for each command. The event name is `[:mongodb_driver, :execution]` and the driver uses the following meta data:

```elixir
metadata = %{
    type: :mongodb_driver,
    command: command,
    params: parameters,
    collection: collection,
    options: Keyword.get(opts, :telemetry_options, [])
}

:telemetry.execute([:mongodb_driver, :execution], %{duration: duration}, metadata)
```

In a Phoenix application with installed Phoenix Dashboard the metrics can be used by defining a metric in the Telemetry module:

```elixr
      summary("mongodb_driver.execution.duration",
        tags: [:collection, :command],
        unit: {:microsecond, :millisecond}
      ),
```

Then you see for each collection the execution time for each different command in the Dashboard metric page.

## Network compression

The driver supports two compressors

* zlib, which is supported by Erlang 
* zstd, which is optional and supported by https://github.com/silviucpp/ezstd bindings.

To activate zlib compression:
1. Append `compressors=zlib` to the URL connection string:
```elixir
{:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/db?compressors=zlib")
```

To activate zstd compression:
1. Add `{:ezstd, "~> 1.1"}` to the dependencies of your `mix.exs` file. The driver will provide the related code.
2. Append `compressors=zstd` to the URL connection string:

```elixir
{:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/db?compressors=zstd")
```

The driver uses compression for the following functions:

* `Mongo.aggregate/4`
* `Mongo.find/4`
* `Mongo.insert_one/4`
* `Mongo.insert_many/4`
* `Mongo.update/4`
* `Mongo.update_documents/6`
* `Mongo.find_one_and_update/5`
* `Mongo.find_one_and_replace/5`
* `Mongo.find_one_and_delete/4`
* `Mongo.count/4`
* `Mongo.distinct/5`
* `Mongo.delete_documents/5`
* `Mongo.create/4`

You can disable the compression for a single function by using the option `compression: false`, for example:

```
Mongo.find(top, "tasks", %{}, compression: false) |> Enum.to_list()
```
The compression significantly reduces the amount of data, while increasing the load on the CPU.
This is certainly interesting for environments in which network transmission has to be paid for.

zlib compression requires a greater penalty in terms of speed than zstd compression. 
The zstd compression offers a good compromise between compression rate and speed and 
is undoubtedly supported by all current MongoDB.

The speed also depends on the `batch_size` attribute. A higher speed is achieved for certain batch sizes. 
Simple experiments can be carried out here to determine which size shortens the duration of the queries:

```elixir
:timer.tc(fn -> Mongo.find(top, "tasks", %{}, limit: 30_000, batch_size: 1000) |> Stream.reject(fn _x -> true end) |> Stream.run() end)
```

## Connection Pooling

The driver supports pooling by DBConnection (2.x). By default `mongodb_driver` will start a single
connection, but it also supports pooling with the `:pool_size` option. For 3 connections add the `pool_size: 3` option to `Mongo.start_link` and to all
function calls in `Mongo` using the pool:

```elixir
# Starts an pooled connection
{:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/db-name", pool_size: 3)

# Gets an enumerable stream for the results
top
|> Mongo.find("test-collection", %{})
|> Enum.to_list()
```

If you're using pooling it is recommended to add it to your application supervisor:

```elixir
def start(_type, _args) do

  children = [
    {Mongo, [name: :mongo_db, url: "mongodb://localhost:27017/test", pool_size: 3]}
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

We can use the `:mongo_db` atom instead of a process pid. This allows us to call the `Mongo` functions directly from
every place in the code.

## Replica Sets

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

## Migration

Despite the schema-free approach, migration is still desirable. Migrations are used to maintain the indexes 
and to drop collections that are no longer needed. Capped collections must be migrated. 
The driver provides a workflow similar to Ecto that can be used to create migrations.

First we create a migration script:
```elixir

mix mongo.gen.migration add_indexes

```

In `priv/mongo/migrations` you will find an Elixir script like `20220322173354_add_indexes.exs`:

```elixr
defmodule Mongo.Migrations.AddIndexes do
  def up() do
    indexes = [
      [key: [email: 1], name: "email_index", unique: true]
    ]

    Mongo.create_indexes(:my_db, "my_collection", indexes)
  end

  def down() do
    Mongo.drop_index(:my_db, "my_collection", "email_index")
  end
end

```

After that you can run the migration using a task:

```
mix mongo.migrate

🔒 migrations locked
⚡️ Successfully migrated Elixir.Mongo.Migrations.CreateIndex
🔓 migrations unlocked

```

Or let it run if your application starts:

```elixir
defmodule MyApp.Release do
  @moduledoc """
  Used for executing DB release tasks when run in production without mix
  installed.
  """

  def migrate() do
    Application.load(:my_app)
    Application.ensure_all_started(:ssl)
    Application.ensure_all_started(:mongodb_driver)
    Mongo.start_link(name: :mongo_db, url: "mongodb://localhost:27017/my-database", timeout: 60_000, pool_size: 1, idle_interval: 10_000)

    Mongo.Migration.migrate()
  end
end
```

With the release features of Elixir you can add an overlay script like this:

```shell
#!/bin/sh
cd -P -- "$(dirname -- "$0")"
exec ./my_app eval MyApp.Release.migrate
```

```shell
#!/bin/sh
cd -P -- "$(dirname -- "$0")"
PHX_SERVER=true exec ./my_app start
```

And then you need just to call migrate before you start the server:

```shell
/app/bin/migrate && /app/bin/server
```

Or if you use a Dockerfile:

```dockerfile
ENTRYPOINT /app/bin/migrate && /app/bin/server
```

The migration module tries to *lock* the migration collection to ensure that only one instance is running the migration. 
Unfortunately MongoDB does not support collection locks, so need to use a software lock:

```elixir
Mongo.update_one(topology, 
  "migrations", 
  %{_id: "lock", used: false}, 
  %{"$set": %{used: true}}, 
  upsert: true)
```
You can lock and unlock the migration collection using these functions in case of an error:

1. `Mongo.Migration.lock()` 
2. `Mongo.Migration.unlock()` or `mix mongo.unlock`

If nothing helps, just delete the document with `{_id: "lock"}` from the migration collection.

For more information see:

- `Mongo.Migration`
- `Mix.Tasks.Mongo`
- https://hexdocs.pm/mix/1.14/Mix.Tasks.Release.html

### Configuration:
You need to configure the migration module and specify at least the `:otp_app` and `:topology` values. Here are the
default values:

    config :mongodb_driver,
        migration:
            [
                topology: :mongo,
                collection: "migrations",
                path: "migrations",
                otp_app: :mongodb_driver
            ]

The following  options are available:
* `:collection` - Version numbers of migrations will be saved in a collection named `migrations` by default.
* `:path` - the `priv` directory for migrations. `:path` defaults to "migrations" and migrations should be placed at "priv/mongo/migrations". The pattern to build the path is `:priv/:topology/:path`
* `:otp_app` - the name of the otp_app to resolve the `priv` folder, defaults to `:mongodb_driver`. In most cases you use your application name.
* `:topology` - the topology for running the migrations, `:topology` defaults to `:mongo`

### Supporting multiple topologies:

Each function `lock/1, unlock/1, migrate/1, drop/1` accepts a keyword list (options) to override the default config having 
full control of the migration process. The options are passed through the migration scripts. 

That means you can support multiple topologies, databases and migration collections. Example

    Mongo.start_link(name: :topology_1, url: "mongodb://localhost:27017/mig_test_1", timeout: 60_000, pool_size: 5, idle_interval: 10_000)
    Mongo.start_link(name: :topology_2, url: "mongodb://localhost:27017/mig_test_2", timeout: 60_000, pool_size: 5, idle_interval: 10_000)

    IO.puts("running default migration")
    Mongo.Migration.migrate() ## default values specified in the configs

    IO.puts("running topology_2 migration")
    Mongo.Migration.migrate([topology: :topology_2]) ## override the topology 

Adding the options parameter in the `up/1` and `down/1` function of the migration script is supported as well. It is
possible to pass additional parameters to the migration scripts.

    defmodule Mongo.Migrations.Topology.CreateIndex do
        def up(opts) do 
            IO.inspect(opts)
            ...
        end
        
        def down(opts) do
            IO.inspect(opts)
            ...
        end
    end

The topology is part of the namespace and of the migration path as well. The default value is defined in the configuration.
You can specify the topology in the case of creating a new migration script by appending the name to the script call:
```elixir

mix mongo.gen.migration add_indexes topology_2

```

In `priv/topology_2/migrations` you will find an Elixir script like `20220322173354_add_indexes.exs`:

```elixr
defmodule Mongo.Migrations.Topology2.AddIndexes do
    ...
end

```

By using the `:topology` keyword, you can organise the migration scripts in different sub-folders. The migration path is prefixed with the `priv` folder of the application and the topology name.

If you call

    Mongo.Migration.migrate([topology: :topology_2])

then the migration scripts under `/priv/topology_2/` are used and the options keyword list is passed through
to the `up/1` function if it is implemented. That means you can create migration scripts for multiple topologies
separated in sub folders and module namespaces.

## Auth Mechanisms

For versions of Mongo 3.0 and greater, the auth mechanism defaults to SCRAM.

If connecting to MongoDB Enterprise Edition or MongoDB Atlas, the [PLAIN](https://www.mongodb.com/docs/manual/tutorial/authenticate-nativeldap-activedirectory/)
auth mechanism is supported for LDAP authentication. The GSSAPI auth mechanism used for Kerberos authentication
is not currently supported.

If you'd like to use [MONGODB-X509](https://www.mongodb.com/docs/v6.0/tutorial/configure-x509-client-authentication/)
authentication, you can specify that as a `start_link` option. 

You need roughly three additional configuration steps:

* Deploy with x.509 Authentication
* Add x.509 Certificate subject as a User
* Authenticate with an x.509 Certificate

To get the x.509 authentication working you need to prepare the ssl configuration accordingly:
* you need to set the ssl option: `verify_peer`
* you need to specify the `cacertfile` because Erlang BEAM don't provide any CA certificate store by default
* you maybe need to customize the hostname check to allow wildcard certificates
* you need to specify the `username` from the subject entry of the user certificate

If you use a user certificate from Atlas a working configuration looks like this. First we
use the [castore](https://hex.pm/packages/castore) package as the CA certificate store. After downloading
the user certificate we extract the username subject entry from the PEM file:

```shell
openssl x509 -in <pathToClientPEM> -inform PEM -subject -nameopt RFC2253

> CN=cert-user
```

The configuration looks now:
```elixir
  opts = [
      url: "mongodb+srv://cluster0.xxx.mongodb.net/myFirstDatabase?authSource=%24external&retryWrites=true&w=majority",
      ssl: true,
      username: "CN=cert-user",
      password: "",
      auth_mechanism: :x509,
      ssl_opts: [
        verify: :verify_peer,
        cacertfile: to_charlist(CAStore.file_path()),
        certfile: '/path-to-cert/X509-cert-2227052404946303101.pem',
        customize_hostname_check: [
          match_fun:
            :public_key.pkix_verify_hostname_match_fun(:https)
        ]
      ]]

    Mongo.start_link(opts)
```

Currently, we need to specify *an empty password* to get the x.509 auth module working. This will be changed soon.  

## x509 and using a dedicated MongoDB Atlas server

Using OTP 26 changed the default configuration regarding TLS. You may see issues when 
connecting to a dedicated Atlas Server using OTP 26. You can restrict the allowed versions and force to use TLS 1.2 instead
of TLS 1.3.

```elixir
   ...
    versions: [:"tlsv1.2"],
   ...
```

See also [MongoDB Security](https://www.mongodb.com/docs/atlas/reference/faq/security/) and 
the [Issue 226](https://github.com/zookzook/elixir-mongodb-driver/issues/226) for some background information.

## AWS, TLS and Erlang SSL Ciphers

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

See the example `AWSX509.Example` as well.

## Timeout

The `:timeout` option sets the maximum time that the caller is allowed to hold the connection’s state (to send and to receive data). 
The default value is 15 seconds. The connection pool defines additional timeout values. 
You can use the `:timeout` as a global option to override the default value:

```elixir
# Starts an pooled connection
{:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/db-name", timeout: 60_000)
```

Each single connection uses `60_000` (60 seconds) as the timeout value instead of `15_000`. But you can override the default value by
using the `:timeout` option, when running a single command:

```elixir
Mongo.find(top, "dogs", %{}, timeout: 120_000)
```

Now the driver will use 120 seconds as the timeout for the single query.

## Read Preferences

The `:read_preference` option sets [read preference](https://www.mongodb.com/docs/manual/core/read-preference/) for the query. The read preference is
a simple map, supporting the following keys:

* `:mode`, possible values: `:primary`, `:primary_preferred`, `:secondary`, `:secondary_preferred` and `:nearest`
* `:max_staleness_ms`, the maxStaleness value in milliseconds
* `:tags`, the set of tags, for example: `[dc: "west", usage: "production"]`

The driver selects the server using the read preference. 

```elixr 
prefs = %{
    mode: :secondary,
    max_staleness_ms: 120_000,
    tags: [dc: "west", usage: "production"]
}

Mongo.find_one(top, "dogs", %{name: "Oskar"}, read_preference: prefs)
```

## Change Streams

Change streams are available in replica set and sharded cluster deployments
and tell you about changes of documents in collections. They work like endless
cursors.

The special thing about change streams is that they are resumable: in case of
a resumable error, no exception is propagated to the application, but instead
the cursor is re-scheduled at the last successful location.

The following example will never stop, thus it is a good idea to use a process
for reading from change streams:

```elixir
seeds = ["hostname1.net:27017", "hostname2.net:27017", "hostname3.net:27017"]
{:ok, top} = Mongo.start_link(database: "my-db", seeds: seeds, appname: "getting rich")
stream =  Mongo.watch_collection(top, "accounts", [], fn doc -> IO.puts "New Token #{inspect doc}" end, max_time: 2_000 )
Enum.each(stream, fn doc -> IO.puts inspect doc end)
```

An example with a spawned process that sends messages to the monitor process:

```elixir
def for_ever(top, monitor) do
    stream = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end)
    Enum.each(stream, fn doc -> send(monitor, {:change, doc}) end)
end

spawn(fn -> for_ever(top, self()) end)
```

For more information see `Mongo.watch_collection/5`

## Indexes

To create indexes you can call the function `Mongo.create_indexes/4`:

```elixir
indexes =  [[key: [files_id: 1, n: 1], name: "files_n_index", unique: true]]
Mongo.create_indexes(top, "my_collection", indexes, opts)
```

You specify the `indexes` parameter as a keyword list with all options described in the documentation of the [createIndex](https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes) command.

For more information see:

- `Mongo.create_indexes/4`
- `Mongo.drop_index/4`

## Bulk Writes

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

result = Mongo.BulkWrite.write(top, bulk, w: 1)
```

In the following example we import 1.000.000 integers into the MongoDB using the stream api:

We need to create an insert operation for each number. Then we call the `Mongo.UnorderedBulk.stream`
function to import it. This function returns a stream function that accumulates
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

- `Mongo.UnorderedBulk`
- `Mongo.OrderedBulk`
- `Mongo.BulkWrite`
- `Mongo.BulkOps`

and have a look at the test units as well.

## GridFS

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

- [Mongo.GridFs.Bucket](https://hexdocs.pm/mongodb_driver/Mongo.GridFs.Bucket.html#content)
- [Mongo.GridFs.Download](https://hexdocs.pm/mongodb_driver/Mongo.GridFs.Download.html#content)
- [Mongo.GridFs.Upload](https://hexdocs.pm/mongodb_driver/Mongo.GridFs.Upload.html#content)

## Transactions

Since MongoDB 4.x, transactions for multiple write operations are possible. Transaction uses sessions, which
just contain a transaction number for each transaction. The `Mongo.Session` is responsible for the
details, and you can use a convenient api for transactions:

```elixir

{:ok, ids} = Mongo.transaction(top, fn ->
{:ok, %InsertOneResult{:inserted_id => id1}} = Mongo.insert_one(top, "dogs", %{name: "Greta"})
{:ok, %InsertOneResult{:inserted_id => id2}} = Mongo.insert_one(top, "dogs", %{name: "Waldo"})
{:ok, %InsertOneResult{:inserted_id => id3}} = Mongo.insert_one(top, "dogs", %{name: "Tom"})
{:ok, [id1, id2, id3]}
end, w: 1)

```
The `Mongo.transaction/3` function supports nesting. This allows the functions to be called from each other and all write operations
are still in the same transaction. The session is stored in the process dictionary under the key `:session`. The surrounding
`Mongo.transaction/3` call creates the session and starts the transaction, storing the session in the process dictionary, commits or
aborts the transaction. All other `Mongo.transaction/3` calls just call the function parameter without other actions.

```elixir
def insert_dog(top, name) do
  Mongo.insert_one(top, "dogs", %{name: name})
end

def insert_dogs(top) do
  Mongo.transaction(top, fn ->
    insert_dog(top, "Tom")
    insert_dog(top, "Bell")
    insert_dog(top, "Fass")
    :ok
  end)
end

:ok = Mongo.transaction(top, fn ->
    insert_dog(top, "Greta")
    insert_dogs(top)
end)
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
For more information see `Mongo.Session` and have a look at the test units as well.

### Aborting a transaction

You have some options to abort a transaction. The simplest possibility is to return an `:error`. For nested
function calls, the `Mongo.abort_transaction/1` function call that throws an exception is suitable.
That means, you can just generate a `raise :should_not_happen` exception as well.

## Command Monitoring

You can watch all events that are triggered while the driver sends requests and processes responses. You can use the
`Mongo.EventHandler` as a starting point. It logs the events from the topic `:commands` (by ignoring the `:isMaster` command)
to `Logger.info`:

```elixir
iex> Mongo.EventHandler.start()
iex> {:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/test")
{:ok, #PID<0.226.0>}
 iex> Mongo.find_one(top, "test", %{}) |> Enum.to_list()
[info] Received command: %Mongo.Events.CommandStartedEvent{command: [find: "test", ...
[info] Received command: %Mongo.Events.CommandSucceededEvent{command_name: :find, ...
```

## Testing

Latest MongoDB is used while running the tests. Replica set of three nodes is created and runs all tests, except the socket and ssl test. If you want to
run the test cases against other MongoDB deployments or older versions, you can use the [mtools](https://github.com/rueckstiess/mtools) for deployment and run the test cases locally:

```bash
pyenv global 3.6
pip3 install --upgrade pip
pip3 install 'mtools[all]'
export PATH=to-your-mongodb/bin/:$PATH
ulimit -S -n 2048 ## in case of Mac OS X
mlaunch init --setParameter enableTestCommands=1 --replicaset --name "rs_1"
mongosh --host localhost:27017 --eval 'rs.initiate({_id: "rs_1", members: [{_id: 0, host: "127.0.0.1:27017"}, {_id: 1, host: "127.0.0.1:27018"}, {_id: 2, host: "127.0.0.1:27019"}]})'
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

- For `--sslMode` you can use one of `allowSSL` or `preferSSL`
- You can enable any other options you want when starting `mongod`

## Additional articles
* [Connecting to MongoDB with Elixir](https://zookzook.github.io/2024/08-25.html)
* [Using Network Compression with MongoDB in Elixir](https://zookzook.github.io/2024/09-15.html)

## Copyright and License

Copyright 2015 Eric Meadows-Jönsson and Justin Wood \
Copyright 2019 - present Michael Maier

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
