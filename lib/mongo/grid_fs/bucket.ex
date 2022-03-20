defmodule Mongo.GridFs.Bucket do
  @moduledoc """

  This is the MongoDB Bucket struct, which specifies the underlying collections to support GridFS. There are always
  two kind of collections used for storing files by GridFS. The `fs.files` collection contains the meta information about
  the file and the `fs.chunks` collection contains the data of the file chunked into blocks of bytes. The names of the
  collection and the size of the block is defined by the following options:

  The bucket has some configuration options:
    * `:chunk_size` - The chunk size in bytes. Defaults to `255*1024`
    * `:name` - The bucket name. Defaults to `fs`

  The bucket checks whether the indexes already exist before attempting to create them. The names of the
  created indexes are "filename_1_uploadDate_1" and "files_id_1_n_1"

  """

  alias BSON.ObjectId
  alias Mongo.GridFs.Bucket

  ##
  # constants used in this module
  #
  @files_index_name "filename_1_uploadDate_1"
  @chunks_index_name "files_id_1_n_1"
  @defaults [name: "fs", chunk_size: 255 * 1024]

  @type t :: %__MODULE__{
          name: String.t(),
          chunk_size: non_neg_integer,
          topology_pid: GenServer.server()
        }

  defstruct name: "fs", chunk_size: 255 * 1024, topology_pid: nil, opts: []

  @doc """
  Creates a new Bucket with a existing connection using the default values. It just contains the
  name of the collections (fs) and the chunk size (255KB).

  The bucket checks the index for both collections as well. In case of multiple
  upload or downloads just create only one bucket and reuse it.

  """
  @spec new(GenServer.server(), Keyword.t()) :: Bucket.t()
  def new(topology_pid, options \\ []) do
    Keyword.merge(@defaults, options)
    |> Enum.reduce(%Bucket{topology_pid: topology_pid, opts: options}, fn {k, v}, bucket -> Map.put(bucket, k, v) end)
    |> check_indexes()
  end

  @doc """
  In case of using transaction you need to add the session to the bucket. This functions adds the session pid to
  the bucket.
  """
  def add_session(%Bucket{opts: opts} = bucket, session_opts) do
    %Bucket{bucket | opts: opts ++ session_opts}
  end

  @doc """
  Returns the collection name for the files collection, default is fs.files.
  """
  @spec files_collection_name(Bucket.t()) :: String.t()
  def files_collection_name(%Bucket{name: fs}), do: fs <> ".files"

  @doc """
  Returns the collection name for the chunks collection, default is fs.chunks.
  """
  @spec chunks_collection_name(Bucket.t()) :: String.t()
  def chunks_collection_name(%Bucket{name: fs}), do: fs <> ".chunks"

  @doc """
  Renames the stored file with the specified file_id.
  """
  @spec rename(Bucket.t(), BSON.ObjectId.t(), String.t()) :: Mongo.result(BSON.document())
  def rename(%Bucket{topology_pid: topology_pid, opts: opts} = bucket, file_id, new_filename) do
    query = %{_id: file_id}
    update = %{"$set" => %{filename: new_filename}}
    collection = files_collection_name(bucket)
    {:ok, _doc} = Mongo.find_one_and_update(topology_pid, collection, query, update, opts)
  end

  @doc """
  Given a `id`, delete this stored file’s files collection document and
  associated chunks from a GridFS bucket.
  """
  @spec delete(Bucket.t(), String.t()) :: {:ok, Mongo.DeleteResult.t()}
  def delete(%Bucket{} = bucket, file_id) when is_binary(file_id) do
    delete(bucket, ObjectId.decode!(file_id))
  end

  @spec delete(Bucket.t(), BSON.ObjectId.t()) :: {:ok, Mongo.DeleteResult.t()}
  def delete(%Bucket{topology_pid: topology_pid, opts: opts} = bucket, %BSON.ObjectId{} = oid) do
    # first delete files document
    collection = files_collection_name(bucket)
    {:ok, %Mongo.DeleteResult{deleted_count: _}} = Mongo.delete_one(topology_pid, collection, %{_id: oid}, opts)

    # then delete all chunk documents
    collection = chunks_collection_name(bucket)
    {:ok, %Mongo.DeleteResult{deleted_count: _}} = Mongo.delete_many(topology_pid, collection, %{files_id: oid}, opts)
  end

  @doc """
  Drops the files and chunks collections associated with
  this bucket.
  """
  @spec drop(Bucket.t()) :: :ok | {:error, Mongo.Error.t()}
  def drop(%Bucket{topology_pid: topology_pid, opts: opts} = bucket) do
    with :ok <- Mongo.drop_collection(topology_pid, files_collection_name(bucket), opts) do
      Mongo.drop_collection(topology_pid, chunks_collection_name(bucket), opts)
    end
  end

  @doc """
  Returns a cursor from the fs.files collection.
  """
  @spec find(Bucket.t(), BSON.document(), Keyword.t()) :: Mongo.cursor()
  def find(%Bucket{topology_pid: topology_pid} = bucket, filter, opts \\ []) do
    Mongo.find(topology_pid, files_collection_name(bucket), filter, opts)
  end

  @doc """
  Finds one file document by `file_id` specified either as a string or `BSON.ObjectId`.
  """
  def find_one(bucket, file_id)

  @spec find_one(Bucket.t(), String.t()) :: BSON.document() | nil
  def find_one(%Bucket{} = bucket, file_id) when is_binary(file_id) do
    find_one(bucket, ObjectId.decode!(file_id))
  end

  @spec find_one(Bucket.t(), BSON.ObjectId.t()) :: BSON.document() | nil
  def find_one(%Bucket{topology_pid: topology_pid, opts: opts} = bucket, %BSON.ObjectId{} = oid) do
    Mongo.find_one(topology_pid, files_collection_name(bucket), %{"_id" => oid}, opts)
  end

  ##
  # checks and creates indexes for the *.files and *.chunks collections
  #
  defp check_indexes(bucket) do
    case files_collection_empty?(bucket) do
      true ->
        create_indexes(bucket)

      false ->
        with :ok <- check_and_create_files_index(bucket),
             :ok <- check_and_chunks_files_index(bucket) do
          bucket
        end
    end
  end

  ##
  # create the collection and indexes for files and chunks
  #
  defp create_indexes(%Bucket{topology_pid: topology_pid, opts: opts} = bucket) do
    with :ok <- Mongo.create(topology_pid, files_collection_name(bucket), opts),
         :ok <- Mongo.create(topology_pid, chunks_collection_name(bucket), opts),
         :ok <- create_files_index(bucket),
         :ok <- create_chunks_index(bucket) do
      bucket
    end
  end

  def check_and_create_files_index(bucket) do
    case check_files_index(bucket) do
      false -> create_files_index(bucket)
      true -> :ok
    end
  end

  def check_and_chunks_files_index(bucket) do
    case check_chunks_index(bucket) do
      false -> create_chunks_index(bucket)
      true -> :ok
    end
  end

  ##
  # from the specs:
  #
  # To determine whether the files collection is empty drivers SHOULD execute the equivalent of the following shell command:
  #
  # db.fs.files.findOne({}, { _id : 1 })
  #
  defp files_collection_empty?(%Bucket{topology_pid: topology_pid} = bucket) do
    coll_name = files_collection_name(bucket)

    topology_pid
    |> Mongo.show_collections()
    |> Enum.find(fn name -> name == coll_name end)
    |> is_nil()
  end

  ##
  # Checks the indexes for the fs.files collection
  #
  defp check_files_index(%Bucket{topology_pid: topology_pid, opts: opts} = bucket) do
    index_member?(topology_pid, files_collection_name(bucket), @files_index_name, opts)
  end

  ##
  # Checks the indexes for the fs.chunks collection
  #
  defp check_chunks_index(%Bucket{topology_pid: topology_pid, opts: opts} = bucket) do
    index_member?(topology_pid, chunks_collection_name(bucket), @chunks_index_name, opts)
  end

  # returns true if the collection contains a index with the given name
  def index_member?(topology_pid, coll, index, opts) do
    topology_pid
    |> Mongo.list_indexes(coll, opts)
    |> Enum.map(fn
      %{"name" => name} -> name
      _ -> ""
    end)
    |> Enum.member?(index)
  end

  ##
  # Creates the indexes for the fs.chunks collection
  #
  defp create_chunks_index(%Bucket{topology_pid: topology_pid, opts: opts} = bucket) do
    Mongo.create_indexes(topology_pid, chunks_collection_name(bucket), [[key: [files_id: 1, n: 1], name: @chunks_index_name, unique: true]], opts)
  end

  ##
  # Creates the indexes for the fs.files collection
  #
  defp create_files_index(%Bucket{topology_pid: topology_pid, opts: opts} = bucket) do
    Mongo.create_indexes(topology_pid, files_collection_name(bucket), [[key: [filename: 1, uploadDate: 1], name: @files_index_name]], opts)
  end
end
