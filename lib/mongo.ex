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

  use Bitwise
  use Mongo.Messages
  alias Mongo.Query
  alias Mongo.ReadPreference
  alias Mongo.TopologyDescription
  alias Mongo.Topology
  alias Mongo.UrlParser

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

  def child_spec(opts, child_opts \\ []) do
    Supervisor.Spec.worker(Mongo, [opts], child_opts)
  end

  @doc """
  Generates a new `BSON.ObjectId`.
  """
  @spec object_id :: BSON.ObjectId.t
  def object_id do
    Mongo.IdServer.new
  end

  @doc """
  Converts the DataTime to a MongoDB timestamp.
  """
  @spec timestamp(DateTime.t) :: BSON.Timestamp.t
  def timestamp(datetime) do
    %BSON.Timestamp{value: DateTime.to_unix(datetime), ordinal: 1}
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
  @spec watch_collection(GenServer.server, collection, [BSON.document], fun, Keyword.it) :: cursor
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

    cursor(topology_pid, cmd, opts)
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
      update:                   update,
      bypassDocumentValidation: opts[:bypass_document_validation],
      maxTimeMS:                opts[:max_time],
      fields:                   opts[:projection],
      new:                      should_return_new(opts[:return_document]),
      sort:                     opts[:sort],
      upsert:                   opts[:upsert],
      collation:                opts[:collation],
    ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(bypass_document_validation max_time projection return_document sort upsert collation)a)

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- exec_command(conn, cmd, opts) do
        {:ok, doc["value"]}
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
    ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(bypass_document_validation max_time projection return_document sort upsert collation)a)

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- exec_command(conn, cmd, opts), do: {:ok, doc["value"]}
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
    cmd = [
      findAndModify: coll,
      query:         filter,
      remove:        true,
      maxTimeMS:     opts[:max_time],
      fields:        opts[:projection],
      sort:          opts[:sort],
      collation:     opts[:collation],
    ] |> filter_nils()
    opts = Keyword.drop(opts, ~w(max_time projection sort collation)a)

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- exec_command(conn, cmd, opts), do: {:ok, doc["value"]}
  end

  @doc false
  @spec count(GenServer.server, collection, BSON.document, Keyword.t) :: result(non_neg_integer)
  def count(topology_pid, coll, filter, opts \\ []) do
    cmd = [
      count: coll,
      query: filter,
      limit: opts[:limit],
      skip: opts[:skip],
      hint: opts[:hint],
      collation: opts[:collation]
    ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(limit skip hint collation)a)

    # Mongo 2.4 and 2.6 returns a float
    with {:ok, doc} <- command(topology_pid, cmd, opts),
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
      []                -> {:error, Mongo.Error.exception(message: "nothing returned")}
      _                 -> :ok # fixes {:error, :too_many_documents_returned}
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
      distinct: coll,
      key: field,
      query: filter,
      collation: opts[:collation],
      maxTimeMS: opts[:max_time]
    ] |> filter_nils()

    opts = Keyword.drop(opts, ~w(max_time)a)

    with {:ok, conn, slave_ok, _} <- select_server(topology_pid, :read, opts),
         opts = Keyword.put(opts, :slave_ok, slave_ok),
         {:ok, doc} <- exec_command(conn, cmd, opts),
         do: {:ok, doc["values"]}
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
           tailable: opts[:tailable],
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
    cursor(topology_pid, cmd, opts)
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
  @spec find_one(GenServer.server, collection, BSON.document, Keyword.t) ::
    BSON.document | nil
  def find_one(conn, coll, filter, opts \\ []) do
    opts = opts
           |> Keyword.delete(:sort)
           |> Keyword.put(:limit, 1)
           |> Keyword.put(:batch_size, 1)

    conn
    |> find(coll, filter, opts)
    |> Enum.at(0)
  end

  @doc """
  Issue a database command. If the command has parameters use a keyword
  list for the document because the "command key" has to be the first
  in the document.
  """
  @spec command(GenServer.server, BSON.document, Keyword.t) :: result(BSON.document)
  def command(topology_pid, cmd, opts \\ []) do
    rp = ReadPreference.defaults(%{mode: :primary})
    rp_opts = [read_preference: Keyword.get(opts, :read_preference, rp)]
    with {:ok, conn, slave_ok, _} <- select_server(topology_pid, :read, rp_opts),
         opts = Keyword.put(opts, :slave_ok, slave_ok),
         do: exec_command(conn, cmd, opts)
  end

  @doc false
  ## refactor: exec_command
  @spec exec_command(pid, BSON.document, Keyword.t) :: {:ok, BSON.document | nil} | {:error, Mongo.Error.t}
  def exec_command(conn, cmd, opts) do
    action = %Query{action: :command}

    with {:ok, _cmd, doc} <- DBConnection.execute(conn, action, [cmd], defaults(opts)),
         {:ok, doc} <- check_for_error(doc) do
      {:ok, doc}
    end
  end

  defp check_for_error(%{"ok" => ok} = response) when ok == 1, do: {:ok, response}
  defp check_for_error(%{"code" => code, "errmsg" => msg}), do: {:error, Mongo.Error.exception(message: msg, code: code)}

  @doc """
  Returns the current wire version.
  """
  @spec wire_version(pid) :: {:ok, integer} | {:error, Mongo.Error.t}
  def wire_version(conn) do
    cmd = %Query{action: :wire_version}
    with {:ok, _cmd, version} <- DBConnection.execute(conn, cmd, %{}, defaults([])) do
      {:ok, version}
    end
  end

  @doc """
  Returns the limits of the database.
  """
  @spec limits(pid) :: {:ok, BSON.document} | {:error, Mongo.Error.t}
  def limits(conn) do
    cmd = %Query{action: :limits}
    with {:ok, _cmd, limits} <- DBConnection.execute(conn, cmd, %{}, defaults([])) do
      {:ok, limits}
    end
  end

  @doc """
  Similar to `command/3` but unwraps the result and raises on error.
  """
  @spec command!(GenServer.server, BSON.document, Keyword.t) :: result!(BSON.document)
  def command!(topology_pid, cmd, opts \\ []) do
    bangify(command(topology_pid, cmd, opts))
  end

  @doc """
  Sends a ping command to the server.
  """
  @spec ping(GenServer.server) :: result(BSON.document)
  def ping(topology_pid) do
    command(topology_pid, [ping: 1], [batch_size: 1])
  end

  @doc """
  Insert a single document into the collection.

  If the document is missing the `_id` field or it is `nil`, an ObjectId
  will be generated, inserted into the document, and returned in the result struct.

  ## Examples

      Mongo.insert_one(pid, "users", %{first_name: "John", last_name: "Smith"})
  """
  @spec insert_one(GenServer.server, collection, BSON.document, Keyword.t) :: result(Mongo.InsertOneResult.t)
  def insert_one(topology_pid, coll, doc, opts \\ []) do
    assert_single_doc!(doc)
    {[id], [doc]} = assign_ids([doc])

    write_concern = %{
      w: Keyword.get(opts, :w),
      j: Keyword.get(opts, :j),
      wtimeout: Keyword.get(opts, :wtimeout)
    } |> filter_nils()

    cmd = [
      insert: coll,
      documents: [doc],
      ordered: Keyword.get(opts, :ordered),
      writeConcern: write_concern,
      bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
    ] |> filter_nils()

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- exec_command(conn, cmd, opts) do
      case doc do
        %{"writeErrors" => _} -> {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}
        _ ->
          case Map.get(write_concern, :w) do
            0 -> {:ok, %Mongo.InsertOneResult{acknowledged: false}}
            _ -> {:ok, %Mongo.InsertOneResult{inserted_id: id}}
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

    write_concern = %{
      w: Keyword.get(opts, :w),
      j: Keyword.get(opts, :j),
      wtimeout: Keyword.get(opts, :wtimeout)
    } |> filter_nils()

    cmd = [
      insert: coll,
      documents: docs,
      ordered: Keyword.get(opts, :ordered),
      writeConcern: write_concern,
      bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
    ] |> filter_nils()

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- exec_command(conn, cmd, opts) do
      case doc do
        %{"writeErrors" => _} ->  {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}
        _ ->
          case Map.get(write_concern, :w) do
            0 -> {:ok, %Mongo.InsertManyResult{acknowledged: false}}
            _ -> {:ok, %Mongo.InsertManyResult{inserted_ids: ids}}
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
    write_concern = %{
                      w: Keyword.get(opts, :w),
                      j: Keyword.get(opts, :j),
                      wtimeout: Keyword.get(opts, :wtimeout)
                    } |> filter_nils()

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

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- exec_command(conn, cmd, opts) do
      case doc do
        %{"writeErrors" => _} -> {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}
        %{ "ok" => _ok, "n" => n } ->
          case Map.get(write_concern, :w) do
            0 -> {:ok, %Mongo.DeleteResult{acknowledged: false}}
            _ -> {:ok, %Mongo.DeleteResult{deleted_count: n}}
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

    write_concern = %{
                      w: Keyword.get(opts, :w),
                      j: Keyword.get(opts, :j),
                      wtimeout: Keyword.get(opts, :wtimeout)
                    } |> filter_nils()

    update = %{
               q: filter,
               u: update,
               upsert: Keyword.get(opts, :upsert),
               multi: multi,
               collation: Keyword.get(opts, :collation),
               arrayFilters: Keyword.get(opts, :filters)
             } |> filter_nils()

    cmd = [
              update: coll,
              updates: [update],
              ordered: Keyword.get(opts, :ordered),
              writeConcern: write_concern,
              bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
            ] |> filter_nils()

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc}        <- exec_command(conn, cmd, opts) do

      case doc do

        %{"writeErrors" => _} -> {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}

        %{"n" => n, "nModified" => n_modified, "upserted" => upserted} ->
          case Map.get(write_concern, :w) do
            0 -> {:ok, %Mongo.UpdateResult{acknowledged: false}}
            _ -> {:ok, %Mongo.UpdateResult{matched_count: n, modified_count: n_modified, upserted_ids: filter_upsert_ids(upserted)}}
          end

        %{"n" => n, "nModified" => n_modified} ->
          case Map.get(write_concern, :w) do
            0 -> {:ok, %Mongo.UpdateResult{acknowledged: false}}
            _ -> {:ok, %Mongo.UpdateResult{matched_count: n, modified_count: n_modified}}
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
    cursor(topology_pid, cmd, opts)
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
  Getting Collection Names
  """
  @spec show_collections(GenServer.server, Keyword.t) :: cursor
  def show_collections(topology_pid, opts \\ []) do

    ##
    # from the specs
    # https://github.com/mongodb/specifications/blob/f4bb783627e7ed5c4095c5554d35287956ef8970/source/enumerate-collections.rst#post-mongodb-280-rc3-versions
    #
    cmd = [listCollections: 1]
    cursor(topology_pid, cmd, opts)
    |> Stream.filter(fn
      %{"type" => name} -> name == "collection"
      _                 -> true
    end)
    |> Stream.map(fn coll -> coll["name"] end)
  end

  @doc"""
    Determines the appropriate connection depending on the type (:read, :write). The result is
    a tuple with the connection, slave_ok flag and mongos flag. Possibly you have to set slave_ok == true in
    the options for the following request because you are requesting a secondary server.
  """
    def select_server(topology_pid, type, opts \\ []) do
    with {:ok, servers, slave_ok, mongos?} <- select_servers(topology_pid, type, opts) do
      if Enum.empty? servers do
        {:ok, nil, slave_ok, mongos?}
      else
        with {:ok, connection} <- servers |> Enum.take_random(1) |> Enum.at(0)
                                          |> get_connection(topology_pid) do
          {:ok, connection, slave_ok, mongos?}
        end
      end
    end
  end

  defp select_servers(topology_pid, type, opts), do: select_servers(topology_pid, type, opts, System.monotonic_time)
  @sel_timeout 30000
  # NOTE: Should think about the handling completely in the Topology GenServer
  #       in order to make the entire operation atomic instead of querying
  #       and then potentially having an outdated topology when waiting for the
  #       connection.
  defp select_servers(topology_pid, type, opts, start_time) do
    topology = Topology.topology(topology_pid)
    with {:ok, servers, slave_ok, mongos?} <- TopologyDescription.select_servers(topology, type, opts) do
      case Enum.empty?(servers) do
        true ->
          case Topology.wait_for_connection(topology_pid, @sel_timeout, start_time) do
            {:ok, _servers} -> select_servers(topology_pid, type, opts, start_time)
            {:error, :selection_timeout} = error -> error
          end
        false -> {:ok, servers, slave_ok, mongos?}
      end
    end
  end

  defp get_connection(nil, _pid), do: {:ok, nil}
  defp get_connection(server, pid) do
    with {:ok, connection} <- Topology.connection_for_address(pid, server) do
      {:ok, connection}
    end
  end

  defp modifier_docs([{key, _}|_], type), do: key |> key_to_string |> modifier_key(type)
  defp modifier_docs(map, _type) when is_map(map) and map_size(map) == 0,  do: :ok
  defp modifier_docs(map, type) when is_map(map), do: Enum.at(map, 0) |> elem(0) |> key_to_string |> modifier_key(type)
  defp modifier_docs(list, type) when is_list(list),  do: Enum.map(list, &modifier_docs(&1, type))

  defp modifier_key(<<?$, _::binary>> = other, :replace),  do: raise(ArgumentError, "replace does not allow atomic modifiers, got: #{other}")
  defp modifier_key(<<?$, _::binary>>, :update),  do: :ok
  defp modifier_key(<<_, _::binary>> = other, :update),  do: raise(ArgumentError, "update only allows atomic modifiers, got: #{other}")
  defp modifier_key(_, _),  do: :ok

  defp key_to_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_to_string(key) when is_binary(key), do: key

  defp cursor(topology_pid, cmd, opts) do
    %Mongo.Cursor{topology_pid: topology_pid, cmd: cmd, on_resume_token: nil, opts: opts}
  end

  defp change_stream_cursor(topology_pid, cmd, fun, opts) do
    %Mongo.Cursor{topology_pid: topology_pid, cmd: cmd, on_resume_token: fun, opts: opts}
  end

  defp filter_nils(keyword) when is_list(keyword) do
    Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
  end

  defp filter_nils(map) when is_map(map) do
    Enum.reject(map, fn {_key, value} -> is_nil(value) end)
    |> Enum.into(%{})
  end

  ##
  # Checks the validity of the document structure. that means either you use binaries or atoms as a key, but not in combination of both.
  #
  #
  defp normalize_doc(doc) do
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

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
    |> Enum.unzip
  end

  defp assign_id(%{_id: id} = map) when id != nil,  do: {id, map}
  defp assign_id(%{"_id" => id} = map) when id != nil, do: {id, map}
  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, id} | _] when id != nil -> {id, keyword}
      [] -> add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map |> Map.to_list |> add_id
  end

  ##
  # Inserts an ID to the document. A distinction is made as to whether binaries or atoms are used as keys.
  #
  defp add_id(doc) do
    id = Mongo.IdServer.new
    {id, add_id(doc, id)}
  end
  defp add_id([{key, _}|_] = list, id) when is_atom(key), do: [{:_id, id}|list]
  defp add_id([{key, _}|_] = list, id) when is_binary(key), do: [{"_id", id}|list]
  defp add_id([], id), do: [{"_id", id}]

end
