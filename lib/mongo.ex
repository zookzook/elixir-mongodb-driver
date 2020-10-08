defmodule Mongo do
  @moduledoc """
  The main entry point for doing queries. All functions take a topology to
  run the query on.

  ## Generic options

  All operations take these options.

    * `:timeout` - The maximum time that the caller is allowed the to hold the
      connection’s state (ignored when using a run/transaction connection,
      default: `15_000`)
    * `:pool` - The pooling behaviour module to use, this option is required
      unless the default `DBConnection.Connection` pool is used
    * `:pool_timeout` - The maximum time to wait for a reply when making a
      synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
      connection's state (boolean, default: `true`)
    * `:log` - A function to log information about a call, either
      a 1-arity fun, `{module, function, args}` with `DBConnection.LogEntry.t`
      prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)
    * `:database` - the database to run the operation on
    * `:connect_timeout` - maximum timeout for connect (default: `5_000`)

  ## Read options

  All read operations that returns a cursor take the following options
  for controlling the behaviour of the cursor.

    * `:batch_size` - Number of documents to fetch in each batch
    * `:limit` - Maximum number of documents to fetch with the cursor
    * `:read_preference` - specifies the rules for selecting a server to query

  ## Write options

  All write operations take the following options for controlling the
  write concern.

    * `:w` - The number of servers to replicate to before returning from write
      operators, a 0 value will return immediately, :majority will wait until
      the operation propagates to a majority of members in the replica set
      (Default: 1)
    * `:j` If true, the write operation will only return after it has been
      committed to journal - (Default: false)
    * `:wtimeout` - If the write concern is not satisfied in the specified
      interval, the operation returns an error
  """

  import Keywords
  import Mongo.Utils
  import Mongo.WriteConcern

  use Bitwise
  use Mongo.Messages

  alias Mongo.Query
  alias Mongo.Topology
  alias Mongo.UrlParser
  alias Mongo.Session
  alias Mongo.Events
  alias Mongo.Events.CommandSucceededEvent
  alias Mongo.Events.CommandFailedEvent
  alias Mongo.Error

  @timeout 15000 # 5000

  @dialyzer [no_match: [count_documents!: 4]]

  @type conn :: DbConnection.Conn
  @type collection :: String.t
  @opaque cursor :: Mongo.Cursor.t
  @type result(t) :: :ok | {:ok, t} | {:error, Mongo.Error.t}
  @type result!(t) :: nil | t | no_return

  defmacrop bangify(result) do
    quote do
      case unquote(result) do
        {:ok, value}    -> value
        {:error, error} -> raise error
        :ok             -> nil
      end
    end
  end

  @type initial_type :: :unknown | :single | :replica_set_no_primary | :sharded

  @doc """
  Start and link to a database connection process.

  ### Options
    * `:database` - The database to use (required)
    * `:hostname` - The host to connect to (require)
    * `:port` - The port to connect to your server (default: 27017)
    * `:url` - A mongo connection url. Can be used in place of `:hostname` and `:database` (optional)
    * `:socket_dir` - Connect to MongoDB via UNIX sockets in the given directory.
      The socket name is derived based on the port. This is the preferred method
      for configuring sockets and it takes precedence over the hostname. If you
      are connecting to a socket outside of the MongoDB convection, use
     `:socket` instead.
    * `:socket` - Connect to MongoDB via UNIX sockets in the given path.
      This option takes precedence over `:hostname` and `:socket_dir`.
    * `:database` (optional)
    * `:seeds` - A list of host names in the cluster. Can be used in place of
      `:hostname` (optional)
    * `:username` - The User to connect with (optional)
    * `:password` - The password to connect with (optional)
    * `:auth` - List of additional users to authenticate as a keyword list with
      `:username` and `:password` keys (optional)
    * `:auth_source` - The database to authenticate against
    * `:appname` - The name of the application used the driver for the MongoDB-Handshake
    * `:set_name` - The name of the replica set to connect to (required if
    connecting to a replica set)
    * `:type` - a hint of the topology type. See `t:initial_type/0` for
      valid values (default: `:unknown`)
    * `:idle` - The idle strategy, `:passive` to avoid checkin when idle and
      `:active` to checking when idle (default: `:passive`)
    * `:idle_timeout` - The idle timeout to ping the database (default: `1_000`)
    * `:connect_timeout` - The maximum timeout for the initial connection
      (default: `5_000`)
    * `:backoff_min` - The minimum backoff interval (default: `1_000`)
    * `:backoff_max` - The maximum backoff interval (default: `30_000`)
    * `:backoff_type` - The backoff strategy, `:stop` for no backoff and to
      stop, `:exp` of exponential, `:rand` for random and `:ran_exp` for random
      exponential (default: `:rand_exp`)
    * `:after_connect` - A function to run on connect use `run/3`. Either a
      1-arity fun, `{module, function, args}` with `DBConnection.t`, prepended
      to `args` or `nil` (default: `nil`)
    * `:auth_mechanism` - options for the mongo authentication mechanism,
      currently only supports `:x509` atom as a value
    * `:ssl` - Set to `true` if ssl should be used (default: `false`)
    * `:ssl_opts` - A list of ssl options, see the ssl docs

  ### Error Reasons
    * `:single_topology_multiple_hosts` - A topology of `:single` was set
      but multiple hosts were given
    * `:set_name_bad_topology` - A `:set_name` was given but the topology was
      set to something other than `:replica_set_no_primary` or `:single`
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Mongo.Error.t | atom}
  def start_link(opts) do
    opts
    |> UrlParser.parse_url()
    |> Topology.start_link()
  end

  def child_spec(opts) do
    %{id: Mongo,
      start: {Mongo, :start_link, [opts]}}
  end

  @doc """
  Generates a new `BSON.ObjectId`.
  """
  @spec object_id :: BSON.ObjectId.t
  def object_id do
    Mongo.IdServer.new()
  end

  @doc """
  Converts the DataTime to a MongoDB timestamp.
  """
  @spec timestamp(DateTime.t) :: BSON.Timestamp.t
  def timestamp(datetime) do
    %BSON.Timestamp{value: DateTime.to_unix(datetime), ordinal: 1}
  end

  @doc """
  Converts the binary to UUID

  ## Example
      iex> Mongo.uuid("848e90e9-5750-4e0a-ab73-66ac6b328242")
      {:ok, #BSON.UUID<848e90e9-5750-4e0a-ab73-66ac6b328242>}

      iex> Mongo.uuid("848e90e9-5750-4e0a-ab73-66ac6b328242x")
      {:error, %ArgumentError{message: "invalid UUID string"}}

      iex> Mongo.uuid("848e90e9-5750-4e0a-ab73-66-c6b328242")
      {:error, %ArgumentError{message: "non-alphabet digit found: \"-\" (byte 45)"}}
  """
  @spec uuid(String.t) :: {:ok, BSON.Binary.t} | {:error, %ArgumentError{}}
  def uuid(string) when is_binary(string) and byte_size(string) == 36 do

    try do
      p1 = binary_part(string, 0, 8) |> Base.decode16!(case: :lower)
      p2 = binary_part(string, 9, 4) |> Base.decode16!(case: :lower)
      p3 = binary_part(string, 14, 4) |> Base.decode16!(case: :lower)
      p4 = binary_part(string, 19, 4) |> Base.decode16!(case: :lower)
      p5 = binary_part(string, 24, 12) |> Base.decode16!(case: :lower)

      value = p1 <> p2 <> p3 <> p4 <> p5
      {:ok, %BSON.Binary{binary: value, subtype: :uuid}}
    rescue
       reason -> {:error, reason}
    end
  end
  def uuid(_other) do
    {:error, %ArgumentError{message: "invalid UUID string"}}
  end

  @doc"""
  Similar to `uuid/1` except it will unwrap the error tuple and raise
  in case of errors.

  ## Example

      iex> Mongo.uuid!("848e90e9-5750-4e0a-ab73-66ac6b328242")
      #BSON.UUID<848e90e9-5750-4e0a-ab73-66ac6b328242>

      iex> Mongo.uuid!("848e90e9-5750-4e0a-ab73-66ac6b328242x")
      ** (ArgumentError) invalid UUID string
      (mongodb_driver 0.6.4) lib/mongo.ex:205: Mongo.uuid!/1
  """
  def uuid!(string) do
    with {:ok, result} <- uuid(string) do
      result
    else
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Creates a new UUID.
  """
  @spec uuid(String.t) :: BSON.Binary.t
  def uuid() do
    %BSON.Binary{binary: uuid4(), subtype: :uuid}
  end

  #
  # From https://github.com/zyro/elixir-uuid/blob/master/lib/uuid.ex
  # with modifications:
  #
  # We don't need a string version, so we use the binary directly
  #
  @uuid_v4   4
  @variant10 2

  defp uuid4() do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    <<u0::48, @uuid_v4::4, u1::12, @variant10::2, u2::62>>
  end

  @doc"""
  Creates a change stream cursor on collections.

  `on_resume_token` is function that takes the new resume token, if it changed.

  ## Options

    * `:full_document` -
    * `:max_time` - Specifies a time limit in milliseconds. This option is used on `getMore` commands
    * `:batch_size` - Specifies the number of maximum number of documents to
      return (default: 1)
    * `:resume_after` - Specifies the logical starting point for the new change stream.
    * `:start_at_operation_time` - The change stream will only provide changes that occurred at or after the specified timestamp (since 4.0)
    * `:start_after` - Similar to `resumeAfter`, this option takes a resume token and starts a new change stream
        returning the first notification after the token. This will allow users to watch collections that have been dropped and recreated
        or newly renamed collections without missing any notifications. (since 4.0.7)
  """
  @spec watch_collection(GenServer.server, collection | 1, [BSON.document], fun, Keyword.it) :: cursor
  def watch_collection(topology_pid, coll, pipeline, on_resume_token \\ nil, opts \\ []) do

    stream_opts = %{
                    fullDocument: opts[:full_document] || "default",
                    resumeAfter: opts[:resume_after],
                    startAtOperationTime: opts[:start_at_operation_time],
                    startAfter: opts[:start_after]
                  } |> filter_nils()

    cmd = [
            aggregate: coll,
            pipeline:  [%{"$changeStream" => stream_opts} | pipeline],
            explain: opts[:explain],
            allowDiskUse: opts[:allow_disk_use],
            collation: opts[:collation],
            maxTimeMS: opts[:max_time],
            cursor: filter_nils(%{batchSize: opts[:batch_size]}),
            bypassDocumentValidation: opts[:bypass_document_validation],
            hint: opts[:hint],
            comment: opts[:comment],
            readConcern: opts[:read_concern]
          ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(full_document resume_after start_at_operation_time start_after explain allow_disk_use collation bypass_document_validation hint comment read_concern)a)

    on_resume_token = on_resume_token || (fn _token -> nil end)
    change_stream_cursor(topology_pid, cmd, on_resume_token, opts)

  end

  @doc"""
  Creates a change stream cursor all collections of the database.

  `on_resume_token` is function that takes the new resume token, if it changed.

  ## Options

    * `:full_document` -
    * `:max_time` - Specifies a time limit in milliseconds. This option is used on `getMore` commands
    * `:batch_size` - Specifies the number of maximum number of documents to
      return (default: 1)
    * `:resume_after` - Specifies the logical starting point for the new change stream.
    * `:start_at_operation_time` - The change stream will only provide changes that occurred at or after the specified timestamp (since 4.0)
    * `:start_after` - Similar to `resumeAfter`, this option takes a resume token and starts a new change stream
        returning the first notification after the token. This will allow users to watch collections that have been dropped and recreated
        or newly renamed collections without missing any notifications. (since 4.0.7)
  """
  @spec watch_db(GenServer.server, [BSON.document], fun, Keyword.it) :: cursor
  def watch_db(topology_pid, pipeline, on_resume_token \\ nil, opts \\ []) do
    watch_collection(topology_pid, 1, pipeline, on_resume_token, opts)
  end

  @doc """
  Performs aggregation operation using the aggregation pipeline.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/aggregate/#aggregate)

  """
  @spec aggregate(GenServer.server, collection, [BSON.document], Keyword.t) :: cursor
  def aggregate(topology_pid, coll, pipeline, opts \\ []) do

    cmd = [
            aggregate: coll,
            pipeline: pipeline,
            explain: opts[:explain],
            allowDiskUse: opts[:allow_disk_use],
            collation: opts[:collation],
            maxTimeMS: opts[:max_time],
            cursor: filter_nils(%{batchSize: opts[:batch_size]}),
            bypassDocumentValidation: opts[:bypass_document_validation],
            hint: opts[:hint],
            comment: opts[:comment],
            readConcern: opts[:read_concern]
          ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(explain allow_disk_use collation bypass_document_validation hint comment read_concern)a)

    get_stream(topology_pid, cmd, opts)
  end

  @doc """
  Finds a document and updates it (using atomic modifiers).

  ## Options

    * `:bypass_document_validation` -  Allows the write to opt-out of document
      level validation
    * `:max_time` -  The maximum amount of time to allow the query to run (in MS)
    * `:projection` -  Limits the fields to return for all matching documents.
    * `:return_document` - Returns the replaced or inserted document rather than
       the original. Values are :before or :after. (default is :before)
    * `:sort` - Determines which document the operation modifies if the query
      selects multiple documents.
    * `:upsert` -  Create a document if no document matches the query or updates
      the document.
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
  """
  @spec find_one_and_update(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result(BSON.document) | {:ok, nil}
  def find_one_and_update(topology_pid, coll, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)

    cmd = [
            findAndModify:            coll,
            query:                    filter,
            sort:                     opts[:sort],
            update:                   update,
            new:                      should_return_new(opts[:return_document]),
            fields:                   opts[:projection],
            upsert:                   opts[:upsert],
            bypassDocumentValidation: opts[:bypass_document_validation],
            writeConcern:             write_concern(opts),
            maxTimeMS:                opts[:max_time],
            collation:                opts[:collation]
          ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(bypass_document_validation max_time projection return_document sort upsert collation w j wtimeout)a)

    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts) do
        {:ok, doc["value"]}
    end

  end

  @doc """
  Executes an admin command against the `admin` database using alway the primary. Retryable writes are disabled.

  ## Example

    iex>  cmd = [
      configureFailPoint: "failCommand",
      mode: "alwaysOn",
      data: [errorCode: 6, failCommands: ["commitTransaction"], errorLabels: ["TransientTransactionError"]]
    ]

    iex> {:ok, _doc} = Mongo.admin_command(top, cmd)
  """
  def admin_command(topology_pid, cmd) do
    issue_command(topology_pid, cmd, :write, database: "admin", retryable_writes: false)
  end

  @doc """
  This function is very fundamental.
  """
  def issue_command(topology_pid, cmd, :read, opts) do

    ## check, if retryable reads are enabled
    opts = Mongo.retryable_reads(opts)

    with {:ok, session} <- Session.start_implicit_session(topology_pid, :read, opts),
         result <- exec_command_session(session, cmd, opts),
         :ok <- Session.end_implict_session(topology_pid, session) do
      case result do
        {:error, error} ->
          case Error.should_retry_read(error, cmd, opts) do
            true  -> issue_command(topology_pid, cmd, :read, Keyword.put(opts, :read_counter, 2))
            false -> {:error, error}
          end
        _other        -> result
      end
    else
      {:new_connection, _server} ->
        :timer.sleep(1000)
        issue_command(topology_pid, cmd, :read, opts)
    end
  end
  def issue_command(topology_pid, cmd, :write, opts) do

    ## check, if retryable reads are enabled
    opts = Mongo.retryable_writes(opts, acknowledged?(cmd[:writeConcerns]))

    with {:ok, session} <- Session.start_implicit_session(topology_pid, :write, opts),
         result         <- exec_command_session(session, cmd, opts),
         :ok            <- Session.end_implict_session(topology_pid, session) do
      result
    else
      {:new_connection, _server} ->
        :timer.sleep(1000)
        issue_command(topology_pid, cmd, :write, opts)
    end
  end

  @doc """
  Finds a document and replaces it.

  ## Options

    * `:bypass_document_validation` -  Allows the write to opt-out of document
      level validation
    * `:max_time` -  The maximum amount of time to allow the query to run (in MS)
    * `:projection` -  Limits the fields to return for all matching documents.
    * `:return_document` - Returns the replaced or inserted document rather than
      the original. Values are :before or :after. (default is :before)
    * `:sort` - Determines which document the operation modifies if the query
      selects multiple documents.
    * `:upsert` -  Create a document if no document matches the query or updates
      the document.
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
      higher.
  """
  @spec find_one_and_replace(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result(BSON.document)
  def find_one_and_replace(topology_pid, coll, filter, replacement, opts \\ []) do
    _ = modifier_docs(replacement, :replace)

    write_concern = write_concern(opts)

    cmd = [
            findAndModify:            coll,
            query:                    filter,
            update:                   replacement,
            bypassDocumentValidation: opts[:bypass_document_validation],
            maxTimeMS:                opts[:max_time],
            fields:                   opts[:projection],
            new:                      should_return_new(opts[:return_document]),
            sort:                     opts[:sort],
            upsert:                   opts[:upsert],
            collation:                opts[:collation],
            writeConcern:             write_concern
          ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(bypass_document_validation max_time projection return_document sort upsert collation)a)

    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts), do: {:ok, doc["value"]}
  end

  defp should_return_new(:after), do: true
  defp should_return_new(:before), do: false
  defp should_return_new(_), do: false

  @doc """
  Finds a document and deletes it.

  ## Options

    * `:max_time` -  The maximum amount of time to allow the query to run (in MS)
    * `:projection` -  Limits the fields to return for all matching documents.
    * `:sort` - Determines which document the operation modifies if the query selects multiple documents.
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and higher.
  """
  @spec find_one_and_delete(GenServer.server, collection, BSON.document, Keyword.t) :: result(BSON.document)
  def find_one_and_delete(topology_pid, coll, filter, opts \\ []) do

    write_concern = write_concern(opts)

    cmd = [
            findAndModify: coll,
            query:         filter,
            remove:        true,
            maxTimeMS:     opts[:max_time],
            fields:        opts[:projection],
            sort:          opts[:sort],
            collation:     opts[:collation],
            writeConcern:  write_concern
          ] |> filter_nils()
    opts = Keyword.drop(opts, ~w(max_time projection sort collation)a)

    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts), do: {:ok, doc["value"]}
  end

  @doc false
  @spec count(GenServer.server, collection, BSON.document, Keyword.t) :: result(non_neg_integer)
  def count(topology_pid, coll, filter, opts \\ []) do
    cmd = [
            count:     coll,
            query:     filter,
            limit:     opts[:limit],
            skip:      opts[:skip],
            hint:      opts[:hint],
            collation: opts[:collation]
          ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(limit skip hint collation)a)

    with {:ok, doc} <- issue_command(topology_pid, cmd, :read, opts),
         do: {:ok, trunc(doc["n"])}
  end

  @doc false
  @spec count!(GenServer.server, collection, BSON.document, Keyword.t) :: result!(non_neg_integer)
  def count!(topology_pid, coll, filter, opts \\ []) do
    bangify(count(topology_pid, coll, filter, opts))
  end

  @doc """
  Returns the count of documents that would match a find/4 query.

  ## Options
    * `:limit` - Maximum number of documents to fetch with the cursor
    * `:skip` - Number of documents to skip before returning the first
  """
  @spec count_documents(GenServer.server, collection, BSON.document, Keyword.t) :: result(non_neg_integer)
  def count_documents(topology_pid, coll, filter, opts \\ []) do
    pipeline = [
      "$match": filter,
      "$skip": opts[:skip],
      "$limit": opts[:limit],
      "$group": %{"_id" => nil, "n" => %{"$sum" => 1}}
    ] |> filter_nils() |> Enum.map(&List.wrap/1)

    documents =
      topology_pid
      |> Mongo.aggregate(coll, pipeline, opts)
      |> Enum.to_list

    case documents do
      [%{"n" => count}] -> {:ok, count}
      []                -> {:ok, 0}
      _                 -> :error
    end
  end

  @doc """
  Similar to `count_documents/4` but unwraps the result and raises on error.
  """
  @spec count_documents!(GenServer.server, collection, BSON.document, Keyword.t) :: result!(non_neg_integer)
  def count_documents!(topology_pid, coll, filter, opts \\ []) do
    bangify(count_documents(topology_pid, coll, filter, opts))
  end

  @doc """
  Estimate the number of documents in a collection using collection metadata.
  """
  @spec estimated_document_count(GenServer.server, collection, Keyword.t) :: result(non_neg_integer)
  def estimated_document_count(topology_pid, coll, opts) do
    opts = Keyword.drop(opts, [:skip, :limit, :hint, :collation])
    count(topology_pid, coll, %{}, opts)
  end

  @doc """
  Similar to `estimated_document_count/3` but unwraps the result and raises on
  error.
  """
  @spec estimated_document_count!(GenServer.server, collection, Keyword.t) :: result!(non_neg_integer)
  def estimated_document_count!(topology_pid, coll, opts) do
    bangify(estimated_document_count(topology_pid, coll, opts))
  end

  @doc """
  Finds the distinct values for a specified field across a collection.

  ## Options

    * `:max_time` - Specifies a time limit in milliseconds
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
  """
  @spec distinct(GenServer.server, collection, String.t | atom, BSON.document, Keyword.t) :: result([BSON.t])
  def distinct(topology_pid, coll, field, filter, opts \\ []) do
    cmd = [
            distinct:   coll,
            key:       field,
            query:     filter,
            collation: opts[:collation],
            maxTimeMS: opts[:max_time]
          ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(max_time)a)

    with {:ok, doc} <- issue_command(topology_pid, cmd, :read, opts), do: {:ok, doc["values"]}
  end

  @doc """
  Similar to `distinct/5` but unwraps the result and raises on error.
  """
  @spec distinct!(GenServer.server, collection, String.t | atom, BSON.document, Keyword.t) :: result!([BSON.t])
  def distinct!(topology_pid, coll, field, filter, opts \\ []) do
    bangify(distinct(topology_pid, coll, field, filter, opts))
  end

  @doc """
  Selects documents in a collection and returns a cursor for the selected
  documents.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  Use the underscore style, for example to set the option `singleBatch` use `single_batch`. Another example:

       Mongo.find(top, "jobs", %{}, batch_size: 2)

  """
  @spec find(GenServer.server, collection, BSON.document, Keyword.t) :: cursor
  def find(topology_pid, coll, filter, opts \\ []) do

    filter = case normalize_doc(filter) do
      []    -> nil
      other -> other
    end

    cmd = [find: coll,
           filter: filter,
           limit: opts[:limit],
           hint: opts[:hint],
           singleBatch: opts[:single_batch],
           readConcern: opts[:read_concern],
           max: opts[:max],
           min: opts[:min],
           collation: opts[:collation],
           returnKey: opts[:return_key],
           showRecordId: opts[:show_record_id],
           tailable: opts[:tailable],
           oplogReplay: opts[:oplog_replay],
           noCursorTimeout: opts[:no_cursor_timeout],
           awaitData: opts[:await_data],
           batchSize: opts[:batch_size],
           projection: opts[:projection],
           comment: opts[:comment],
           maxTimeMS: opts[:max_time],
           skip: opts[:skip],
           sort: opts[:sort]
          ]

    cmd = filter_nils(cmd)

    drop = ~w(limit hint single_batch read_concern max min collation return_key show_record_id tailable no_cursor_timeout await_data batch_size projection comment max_time skip sort)a
    opts = Keyword.drop(opts, drop)
    try do
      get_stream(topology_pid, cmd, opts)
    rescue
      error -> {:error, error}
    end

  end

  @doc """
  Selects a single document in a collection and returns either a document
  or nil.

  If multiple documents satisfy the query, this method returns the first document
  according to the natural order which reflects the order of documents on the disk.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  Use the underscore style, for example to set the option `readConcern` use `read_concern`. Another example:

       Mongo.find_one(top, "jobs", %{}, read_concern: %{level: "local"})
  """
  @spec find_one(GenServer.server, collection, BSON.document, Keyword.t) :: BSON.document | nil
  def find_one(topology_pid, coll, filter, opts \\ []) do
    opts = opts
           |> Keyword.put(:limit, 1)
           |> Keyword.put(:batch_size, 1)

    try do
      case find(topology_pid, coll, filter, opts) do
        {:error, error} -> {:error, error}
        other           -> Enum.at(other, 0)
      end
    rescue
      error -> {:error, error}
    end

  end

  @doc """
  Issue a database command. If the command has parameters use a keyword
  list for the document because the "command key" has to be the first
  in the document.
  """
  @spec command(GenServer.server, BSON.document, Keyword.t) :: result(BSON.document)
  def command(topology_pid, cmd, opts \\ []) do
    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts) do
      {:ok, doc}
    end
  end

  @doc false
  @spec exec_command_session(GenServer.server, BSON.document, Keyword.t) :: {:ok, BSON.document | nil} | {:error, Mongo.Error.t}
  def exec_command_session(session, cmd, opts) do
    with {:ok, conn, new_cmd}      <- Session.bind_session(session, cmd),
         {:ok, _cmd, {doc, event}} <- DBConnection.execute(conn, %Query{action: :command}, [new_cmd], defaults(opts)),
         doc                       <- Session.update_session(session, doc, opts),
         {:ok, doc}                <- check_for_error(doc, event) do
      {:ok, doc}
    else
      {:error, error} ->
      ## todo update Topology
        case Error.should_retry_write(error, cmd, opts) do
          true  ->
          with :ok <- Session.select_server(session, opts) do
           exec_command_session(session, cmd, Keyword.put(opts, :write_counter, 2))
          end
          false -> {:error, error}
        end
    end

  end

  @doc false
  @spec exec_command(GenServer.server, BSON.document, Keyword.t) :: {:ok, BSON.document | nil} | {:error, Mongo.Error.t}
  def exec_command(conn, cmd, opts) do
    with {:ok, _cmd, {doc, event}} <- DBConnection.execute(conn, %Query{action: :command}, [cmd], defaults(opts)),
         {:ok, doc} <- check_for_error(doc, event) do
      {:ok, doc}
    end

  end

  defp check_for_error(%{"ok" => ok} = response, {event, duration}) when ok == 1 do
    Events.notify(%CommandSucceededEvent{
      reply: response,
      duration: duration,
      command_name: event.command_name,
      request_id: event.request_id,
      operation_id: event.operation_id,
      connection_id: event.connection_id
    }, :commands)
    {:ok, response}
  end
  defp check_for_error(doc, {event, duration}) do

    error = Mongo.Error.exception(doc)

    Events.notify(%CommandFailedEvent{
      failure: error,
      duration: duration,
      command_name: event.command_name,
      request_id: event.request_id,
      operation_id: event.operation_id,
      connection_id: event.connection_id
    }, :commands)

    {:error, error}
  end

  @doc """
  Returns the wire version of the database
  ## Example

      {:ok, top} = Mongo.start_link(...)
      Mongo.wire_version(top)

      {:ok, 8}
  """
  @spec wire_version(GenServer.server) :: {:ok, integer} | {:error, Mongo.Error.t}
  def wire_version(topology_pid) do
    with {:ok, wire_version} <- Topology.wire_version(topology_pid) do
      {:ok, wire_version}
    end
  end

  @doc """
  Returns the limits of the database.

  ## Example

      {:ok, top} = Mongo.start_link(...)
      Mongo.limits(top)

      {:ok, %{
         compression: nil,
         logical_session_timeout: 30,
         max_bson_object_size: 16777216,
         max_message_size_bytes: 48000000,
         max_wire_version: 8,
         max_write_batch_size: 100000,
         read_only: false
      }}
  """
  @spec limits(GenServer.server) :: {:ok, BSON.document} | {:error, Mongo.Error.t}
  def limits(topology_pid) do
    with {:ok, limits} <- Topology.limits(topology_pid) do
      {:ok, limits}
    end
  end

  @doc """
  Similar to `command/3` but unwraps the result and raises on error.
  """
  @spec command!(GenServer.server, BSON.document, Keyword.t) :: result!(BSON.document)
  def command!(topology_pid, cmd, opts \\ []) do
    bangify(issue_command(topology_pid, cmd, :write, opts))
  end

  @doc """
  Sends a ping command to the server.
  """
  @spec ping(GenServer.server) :: result(BSON.document)
  def ping(topology_pid) do
    issue_command(topology_pid, [ping: 1], :read, [batch_size: 1])
  end

  @doc """
  Explicitly creates a collection or view.
  """
  @spec create(GenServer.server, collection, Keyword.t) :: :ok | {:error, Mongo.Error.t}
  def create(topology_pid, coll, opts \\ []) do

    cmd = [
            create:              coll,
            capped:              opts[:capped],
            autoIndexId:         opts[:auto_index_id],
            size:                opts[:size],
            max:                 opts[:max],
            storageEngine:       opts[:storage_engine],
            validator:           opts[:validator],
            validationLevel:     opts[:validation_level],
            validationAction:    opts[:validation_action],
            indexOptionDefaults: opts[:index_option_defaults],
            viewOn:              opts[:view_on],
            pipeline:            opts[:pipeline],
            collation:           opts[:collation],
            writeConcern:        write_concern(opts),
          ] |> filter_nils()

    with {:ok, _doc} <- issue_command(topology_pid, cmd, :write, opts) do
      :ok
    end

  end

  @doc """
  Insert a single document into the collection.

  If the document is missing the `_id` field or it is `nil`, an ObjectId
  will be generated, inserted into the document, and returned in the result struct.

  ## Examples

      Mongo.insert_one(pid, "users", %{first_name: "John", last_name: "Smith"})

      {:ok, session} = Session.start_session(pid)
      Session.start_transaction(session)
      Mongo.insert_one(pid, "users", %{first_name: "John", last_name: "Smith"}, session: session)
      Session.commit_transaction(session)
      Session.end_session(pid)

  """
  @spec insert_one(GenServer.server, collection, BSON.document, Keyword.t) :: result(Mongo.InsertOneResult.t)
  def insert_one(topology_pid, coll, doc, opts \\ []) do
    assert_single_doc!(doc)
    {[id], [doc]} = assign_ids([doc])

    write_concern = write_concern(opts)
    cmd = [
            insert: coll,
            documents: [doc],
            ordered: Keyword.get(opts, :ordered),
            writeConcern: write_concern,
            bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
          ] |> filter_nils()

    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts) do
      case doc do
        %{"writeErrors" => _} -> {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}
        _ ->
          case acknowledged?(write_concern) do
            false -> {:ok, %Mongo.InsertOneResult{acknowledged: false}}
            true  -> {:ok, %Mongo.InsertOneResult{inserted_id: id}}
          end
      end
    end
  end

  @doc """
  Similar to `insert_one/4` but unwraps the result and raises on error.
  """
  @spec insert_one!(GenServer.server, collection, BSON.document, Keyword.t) :: result!(Mongo.InsertOneResult.t)
  def insert_one!(topology_pid, coll, doc, opts \\ []) do
    bangify(insert_one(topology_pid, coll, doc, opts))
  end

  @doc """
  Insert multiple documents into the collection.

  If any of the documents is missing the `_id` field or it is `nil`, an ObjectId will be generated, and insertd into the document.
  Ids of all documents will be returned in the result struct.

  ## Options

  For more information about options see [Options](https://docs.mongodb.com/manual/reference/command/insert/)

  ## Examples

      Mongo.insert_many(pid, "users", [%{first_name: "John", last_name: "Smith"}, %{first_name: "Jane", last_name: "Doe"}])
  """
  @spec insert_many(GenServer.server, collection, [BSON.document], Keyword.t) :: result(Mongo.InsertManyResult.t)
  def insert_many(topology_pid, coll, docs, opts \\ []) do
    assert_many_docs!(docs)
    {ids, docs} = assign_ids(docs)

    write_concern = write_concern(opts)

    cmd = [
            insert: coll,
            documents: docs,
            ordered: Keyword.get(opts, :ordered),
            writeConcern: write_concern,
            bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
          ] |> filter_nils()

    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts) do
      case doc do
        %{"writeErrors" => _} ->  {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}
        _ ->
          case acknowledged?(write_concern)  do
            false -> {:ok, %Mongo.InsertManyResult{acknowledged: false}}
            true  -> {:ok, %Mongo.InsertManyResult{inserted_ids: ids}}
          end
      end
    end
  end

  @doc """
  Similar to `insert_many/4` but unwraps the result and raises on error.
  """
  @spec insert_many!(GenServer.server, collection, [BSON.document], Keyword.t) :: result!(Mongo.InsertManyResult.t)
  def insert_many!(topology_pid, coll, docs, opts \\ []) do
    bangify(insert_many(topology_pid, coll, docs, opts))
  end

  @doc """
  Remove a document matching the filter from the collection.
  """
  @spec delete_one(GenServer.server, collection, BSON.document, Keyword.t) :: result(Mongo.DeleteResult.t)
  def delete_one(topology_pid, coll, filter, opts \\ []) do
    delete_documents(topology_pid, coll, filter, 1, opts)
  end

  @doc """
  Similar to `delete_one/4` but unwraps the result and raises on error.
  """
  @spec delete_one!(GenServer.server, collection, BSON.document, Keyword.t) :: result!(Mongo.DeleteResult.t)
  def delete_one!(topology_pid, coll, filter, opts \\ []) do
    bangify(delete_one(topology_pid, coll, filter, opts))
  end

  @doc """
  Remove all documents matching the filter from the collection.
  """
  @spec delete_many(GenServer.server, collection, BSON.document, Keyword.t) :: result(Mongo.DeleteResult.t)
  def delete_many(topology_pid, coll, filter, opts \\ []) do
    delete_documents(topology_pid, coll, filter, 0, opts)
  end

  ##
  # This is the implementation of the delete command for
  # delete_one and delete_many
  #
  defp delete_documents(topology_pid, coll, filter, limit, opts)  do

    # see https://docs.mongodb.com/manual/reference/command/delete/#dbcmd.delete
    write_concern = write_concern(opts)

    filter = %{
               q: filter,
               limit: limit,
               collation: Keyword.get(opts, :collation)
             } |> filter_nils()

    cmd = [
            delete: coll,
            deletes: [filter],
            ordered: Keyword.get(opts, :ordered),
            writeConcern: write_concern
          ] |> filter_nils()

    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts) do
      case doc do
        %{"writeErrors" => _} -> {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}
        %{ "ok" => _ok, "n" => n } ->
          case acknowledged?(write_concern) do
            false -> {:ok, %Mongo.DeleteResult{acknowledged: false}}
            true  -> {:ok, %Mongo.DeleteResult{deleted_count: n}}
          end
        _ -> {:ok, nil}
      end
    end
  end

  @doc """
  Similar to `delete_many/4` but unwraps the result and raises on error.
  """
  @spec delete_many!(GenServer.server, collection, BSON.document, Keyword.t) :: result!(Mongo.DeleteResult.t)
  def delete_many!(topology_pid, coll, filter, opts \\ []) do
    bangify(delete_many(topology_pid, coll, filter, opts))
  end

  @doc """
  Replace a single document matching the filter with the new document.

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec replace_one(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result(Mongo.UpdateResult.t)
  def replace_one(topology_pid, coll, filter, replacement, opts \\ []) do
    _ = modifier_docs(replacement, :replace)
    update_documents(topology_pid, coll, filter, replacement, false, opts)
  end

  @doc """
  Similar to `replace_one/5` but unwraps the result and raises on error.
  """
  @spec replace_one!(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result!(Mongo.UpdateResult.t)
  def replace_one!(topology_pid, coll, filter, replacement, opts \\ []) do
    bangify(replace_one(topology_pid, coll, filter, replacement, opts))
  end

  @doc """
  Update a single document matching the filter.

  Uses MongoDB update operators to specify the updates. For more information
  please refer to the
  [MongoDB documentation](http://docs.mongodb.org/manual/reference/operator/update/)

  Example:

      Mongo.update_one(MongoPool,
        "my_test_collection",
        %{"filter_field": "filter_value"},
        %{"$set": %{"modified_field": "new_value"}})

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec update_one(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result(Mongo.UpdateResult.t)
  def update_one(topology_pid, coll, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)
    update_documents(topology_pid, coll, filter, update, false, opts)
  end

  @doc """
  Similar to `update_one/5` but unwraps the result and raises on error.
  """
  @spec update_one!(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result!(Mongo.UpdateResult.t)
  def update_one!(topology_pid, coll, filter, update, opts \\ []) do
    bangify(update_one(topology_pid, coll, filter, update, opts))
  end

  @doc """
  Update all documents matching the filter.

  Uses MongoDB update operators to specify the updates. For more information and all options
  please refer to the [MongoDB documentation](https://docs.mongodb.com/manual/reference/command/update/#dbcmd.update)

  """
  @spec update_many(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result(Mongo.UpdateResult.t)
  def update_many(topology_pid, coll, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)
    update_documents(topology_pid, coll, filter, update, true, opts)
  end

  ##
  # Calls the update command:
  #
  # see https://docs.mongodb.com/manual/reference/command/update/#update-command-output
  #
  defp update_documents(topology_pid, coll, filter, update, multi, opts) do

    write_concern = write_concern(opts)

    update = %{
               q: filter,
               u: update,
               upsert: Keyword.get(opts, :upsert),
               multi: multi,
               collation: Keyword.get(opts, :collation),
               arrayFilters: Keyword.get(opts, :array_filters)
             } |> filter_nils()

    cmd = [
            update: coll,
            updates: [update],
            ordered: Keyword.get(opts, :ordered),
            writeConcern: write_concern,
            bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
          ] |> filter_nils()


    with {:ok, doc} <- issue_command(topology_pid, cmd, :write, opts) do

      case doc do

        %{"writeErrors" => _} -> {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}

        %{"n" => n, "nModified" => n_modified, "upserted" => upserted} ->
          case acknowledged?(write_concern)  do
            false -> {:ok, %Mongo.UpdateResult{acknowledged: false}}
            true  -> {:ok, %Mongo.UpdateResult{matched_count: n, modified_count: n_modified, upserted_ids: filter_upsert_ids(upserted)}}
          end

        %{"n" => n, "nModified" => n_modified} ->
          case acknowledged?(write_concern)  do
            false -> {:ok, %Mongo.UpdateResult{acknowledged: false}}
            true  -> {:ok, %Mongo.UpdateResult{matched_count: n, modified_count: n_modified}}
          end

        _ -> {:ok, nil}

      end
    end
  end

  defp filter_upsert_ids(nil), do: []
  defp filter_upsert_ids(upserted), do: Enum.map(upserted, fn doc -> doc["_id"] end)

  @doc """
  Similar to `update_many/5` but unwraps the result and raises on error.
  """
  @spec update_many!(GenServer.server, collection, BSON.document, BSON.document, Keyword.t) :: result!(Mongo.UpdateResult.t)
  def update_many!(topology_pid, coll, filter, update, opts \\ []) do
    bangify(update_many(topology_pid, coll, filter, update, opts))
  end

  @doc """
  Returns a cursor to enumerate all indexes
  """
  @spec list_indexes(GenServer.server, String.t, Keyword.t) :: cursor
  def list_indexes(topology_pid, coll, opts \\ []) do
    cmd = [listIndexes: coll]
    get_stream(topology_pid, cmd, opts)
  end

  @doc """
  Convenient function that returns a cursor with the names of the indexes.
  """
  @spec list_index_names(GenServer.server, String.t, Keyword.t) :: %Stream{}
  def list_index_names(topology_pid, coll, opts \\ []) do
    list_indexes(topology_pid, coll, opts)
    |> Stream.map(fn %{"name" => name } -> name end)
  end

  @doc """
  Convenient function that drops the index `name` in the collection `coll`.
  """
  @spec drop_index(GenServer.server, String.t, String.t, Keyword.t) :: :ok | {:error, Mongo.Error.t}
  def drop_index(topology_pid, coll, name, opts \\ []) do
    cmd = [dropIndexes: coll, index: name]
    with {:ok, _} <- Mongo.issue_command(topology_pid, cmd, :write, opts) do
      :ok
    end
  end

  @doc """
  Convenient function to creates new indexes in the collection `coll`. The `indexes` parameter
  is a keyword list with all options for creating indexes in the MongoDB.
  See [options](https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes) about the
  details of each parameter.
  """
  @spec create_indexes(GenServer.server, String.t, Keyword.t, Keyword.t) :: :ok | {:error, Mongo.Error.t}
  def create_indexes(topology_pid, coll, indexes, opts \\ []) do
    cmd = [createIndexes: coll, indexes: indexes]
    with {:ok, _} <- Mongo.issue_command(topology_pid, cmd, :write, opts) do
      :ok
    end
  end

  @doc """
  Convenient function that drops the collection `coll`.
  """
  @spec drop_collection(GenServer.server, String.t, Keyword.t) :: :ok | {:error, Mongo.Error.t}
  def drop_collection(topology_pid, coll, opts \\ []) do
    with {:ok, _} <- Mongo.issue_command(topology_pid, [drop: coll], :write, opts) do
      :ok
    end
  end

  @doc """
  Convenient function that drops the database `name`.
  """
  @spec drop_database(GenServer.server, String.t) :: :ok | {:error, Mongo.Error.t}
  def drop_database(topology_pid, name \\ nil)
  def drop_database(topology_pid, nil) do
    with {:ok, _} <- Mongo.issue_command(topology_pid, [dropDatabase: 1], :write, []) do
      :ok
    end
  end
  def drop_database(topology_pid, name) do
    with {:ok, _} <- Mongo.issue_command(topology_pid, [dropDatabase: 1], :write, [database: name]) do
      :ok
    end
  end

  @doc """
  Getting Collection Names
  """
  @spec show_collections(GenServer.server, Keyword.t) :: cursor
  def show_collections(topology_pid, opts \\ []) do

    ##
    # from the specs
    # https://github.com/mongodb/specifications/blob/f4bb783627e7ed5c4095c5554d35287956ef8970/source/enumerate-collections.rst#post-mongodb-280-rc3-versions
    #
    cmd = [listCollections: 1]
    get_stream(topology_pid, cmd, opts)
    |> Stream.filter(fn
      %{"type" => name} -> name == "collection"
      _                 -> true
    end)
    |> Stream.map(fn coll -> coll["name"] end)
  end

  @doc """
  In case of retryable reads are enabled, the keyword `:read_counter` is added with the value of 1.

  In other cases like

  * `:retryable_reads` is false or nil
  * `:session` is nil
  * `:read_counter` is nil

  the `opts` is unchanged

  ## Example

    iex> Mongo.retryable_reads([retryable_reads: true])
    [retryable_reads: true, read_counter: 1]

  """
  def retryable_reads(opts) do
    case opts[:read_counter] do
      nil -> case opts[:retryable_reads] == true && opts[:session] == nil do
              true -> opts ++ [read_counter: 1]
              false -> opts
             end
      _other -> opts
    end
  end

  @doc """
  In case of retryable writes are enabled, the keyword `:write_counter` is added with the value of 1.

  In other cases like

  * `:retryable_writes` is false or nil
  * `:session` is nil
  * `:write_counter` is nil

  the `opts` is unchanged

  ## Example

    iex> Mongo.retryable_writes([retryable_writes: true], true)
    [retryable_writes: true, write_counter: 1]

  """
  def retryable_writes(opts, true) do
    case opts[:write_counter] do
      nil -> case Keyword.get(opts, :retryable_writes, true) == true && opts[:session] == nil do
               true  -> opts ++ [write_counter: 1]
               false -> opts
             end
      _other -> opts
    end
  end
  def retryable_writes(opts, false) do
    Keyword.put(opts, :retryable_writes, false)
  end

  defp get_stream(topology_pid, cmd, opts) do
    Mongo.Stream.new(topology_pid, cmd, opts)
  end

  defp change_stream_cursor(topology_pid, cmd, fun, opts) do
    Mongo.ChangeStream.new(topology_pid,  cmd, fun, opts)
  end

  ##
  # Checks the validity of the document structure. that means either you use binaries or atoms as a key, but not in combination of both.
  #
  # todo support for structs
  defp normalize_doc(doc) do

    #doc = case Map.has_key?(doc, :__struct__) do
    #  true  -> Map.to_list(doc)
    #  false -> doc
    #end

    Enum.reduce(doc, {:unknown, []}, fn
      {key, _value}, {:binary, _acc} when is_atom(key)   -> invalid_doc(doc)
      {key, _value}, {:atom, _acc}   when is_binary(key) -> invalid_doc(doc)
      {key, value}, {_, acc}         when is_atom(key)   -> {:atom, [{key, value}|acc]}
      {key, value}, {_, acc}         when is_binary(key) -> {:binary, [{key, value}|acc]}
    end)
    |> elem(1)
    |> Enum.reverse
  end

  defp invalid_doc(doc), do: raise ArgumentError, "invalid document containing atom and string keys: #{inspect doc}"

  defp assert_single_doc!(doc) when is_map(doc), do: :ok
  defp assert_single_doc!([]), do: :ok
  defp assert_single_doc!([{_, _} | _]), do: :ok
  defp assert_single_doc!(other), do: raise ArgumentError, "expected single document, got: #{inspect other}"

  defp assert_many_docs!([first | _]) when not is_tuple(first), do: :ok
  defp assert_many_docs!(other), do: raise ArgumentError, "expected list of documents, got: #{inspect other}"

  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end

end
