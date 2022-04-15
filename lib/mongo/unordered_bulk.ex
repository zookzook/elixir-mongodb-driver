defmodule Mongo.UnorderedBulk do
  @moduledoc """

  An **unordered** bulk is filled in the memory with the bulk operations. These are divided into three lists (inserts, updates, deletes)
  added. If the unordered bulk is written to the database, the groups are written in the following order:

  1. inserts
  2. updates
  3. deletes

  The order within the group is undefined.

  ## Example

  ```
  alias Mongo.UnorderedBulk
  alias Mongo.BulkWrite

  bulk = "bulk"
  |> UnorderedBulk.new()
  |> UnorderedBulk.insert_one(%{name: "Greta"})
  |> UnorderedBulk.insert_one(%{name: "Tom"})
  |> UnorderedBulk.insert_one(%{name: "Waldo"})
  |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
  |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
  |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
  |> UnorderedBulk.delete_one(%{kind: "dog"})
  |> UnorderedBulk.delete_one(%{kind: "dog"})
  |> UnorderedBulk.delete_one(%{kind: "dog"})

  result = BulkWrite.write(:mongo, bulk, w: 1)
  ```

  To reduce the memory usage the unordered bulk can be used with streams.

  ## Example

  ```
  1..1_000_000
  |> Stream.map(fn i -> BulkOps.get_insert_one(%{number: i}) end)
  |> UnorderedBulk.write(:mongo, "bulk", 1_000)
  |> Stream.run()
  ```

  This example first generates the bulk operation by calling `Mongo.BulkOps.get_insert_one\\1`. The operation is used as a parameter in the `Mongo.UnorderedBulk.write\\3` function.
  The unordered bulk was created with a buffer of 1000 operations. After 1000 operations, the
  unordered bulk is written to the database. Depending on the selected size you can control the speed and memory consumption. The higher the
  value, the faster the processing and the greater the memory consumption.
  """

  alias Mongo.UnorderedBulk
  alias Mongo.BulkWrite

  import Mongo.BulkOps

  @type t :: %__MODULE__{
          coll: String.t(),
          inserts: [Mongo.BulkOps.bulk_op()],
          updates: [Mongo.BulkOps.bulk_op()],
          deletes: [Mongo.BulkOps.bulk_op()]
        }

  defstruct coll: nil, inserts: [], updates: [], deletes: []

  @doc """
  Creates an empty unordered bulk for a collection.

  Example:

  ```
  Mongo.UnorderedBulk.new("bulk")
  %Mongo.UnorderedBulk{coll: "bulk", deletes: [], inserts: [], updates: []}
  ```
  """
  @spec new(String.t()) :: UnorderedBulk.t()
  def new(coll) do
    %UnorderedBulk{coll: coll}
  end

  @doc """
  Returns true, if the bulk is empty, that means it contains no inserts, updates or deletes operations
  """
  def empty?(%UnorderedBulk{inserts: [], updates: [], deletes: []}) do
    true
  end

  def empty?(_other) do
    false
  end

  @doc """
  Adds the two unordered bulks together.
  """
  def add(%UnorderedBulk{coll: coll_a} = a, %UnorderedBulk{coll: coll_b} = b) when coll_a == coll_b do
    %UnorderedBulk{coll: coll_a, inserts: a.inserts ++ b.inserts, updates: a.updates ++ b.updates, deletes: a.deletes ++ b.deletes}
  end

  @doc """
  Appends a bulk operation to the unordered bulk. One of the field (inserts, updates or deletes)
  will be updated.
  """
  @spec push(Mongo.BulkOps.bulk_op(), UnorderedBulk.t()) :: UnorderedBulk.t()
  def push({:insert, doc}, %UnorderedBulk{inserts: rest} = bulk) do
    %UnorderedBulk{bulk | inserts: [doc | rest]}
  end

  def push({:update, doc}, %UnorderedBulk{updates: rest} = bulk) do
    %UnorderedBulk{bulk | updates: [doc | rest]}
  end

  def push({:delete, doc}, %UnorderedBulk{deletes: rest} = bulk) do
    %UnorderedBulk{bulk | deletes: [doc | rest]}
  end

  @doc """
  Appends an insert operation.

  Example:

  ```
  Mongo.UnorderedBulk.insert_one(bulk, %{name: "Waldo"})
  %Mongo.UnorderedBulk{
  coll: "bulk",
  deletes: [],
  inserts: [%{name: "Waldo"}],
  updates: []
  }
  ```
  """
  @spec insert_one(UnorderedBulk.t(), BSON.document()) :: UnorderedBulk.t()
  def insert_one(%UnorderedBulk{} = bulk, doc) do
    get_insert_one(doc) |> push(bulk)
  end

  @doc """
  Appends a delete operation with `:limit = 1`.

  Example:

  ```
  Mongo.UnorderedBulk.delete_one(bulk, %{name: "Waldo"})
  %Mongo.UnorderedBulk{
  coll: "bulk",
  deletes: [{%{name: "Waldo"}, [limit: 1]}],
  inserts: [],
  updates: []
  }
  ```
  """
  @spec delete_one(UnorderedBulk.t(), BSON.document()) :: UnorderedBulk.t()
  def delete_one(%UnorderedBulk{} = bulk, doc) do
    get_delete_one(doc) |> push(bulk)
  end

  @doc """
  Appends a delete operation with `:limit = 0`.

    Example:

  ```
  Mongo.UnorderedBulk.delete_many(bulk, %{name: "Waldo"})
  %Mongo.UnorderedBulk{
  coll: "bulk",
  deletes: [{%{name: "Waldo"}, [limit: 0]}],
  inserts: [],
  updates: []
  }
  ```
  """
  @spec delete_many(UnorderedBulk.t(), BSON.document()) :: UnorderedBulk.t()
  def delete_many(%UnorderedBulk{} = bulk, doc) do
    get_delete_many(doc) |> push(bulk)
  end

  @doc """
  Appends a replace operation with `:multi = false`.

    Example:

  ```
  Mongo.UnorderedBulk.replace_one(bulk, %{name: "Waldo"}, %{name: "Greta", kind: "dog"})
  %Mongo.UnorderedBulk{
  coll: "bulk",
  deletes: [],
  inserts: [],
  updates: [{%{name: "Waldo"}, %{kind: "dog", name: "Greta"}, [multi: false]}]
  }
  ```
  """
  @spec replace_one(UnorderedBulk.t(), BSON.document(), BSON.document(), Keyword.t()) :: UnorderedBulk.t()
  def replace_one(%UnorderedBulk{} = bulk, filter, replacement, opts \\ []) do
    get_replace_one(filter, replacement, opts) |> push(bulk)
  end

  @doc """
  Appends a update operation with `:multi = false`.

    Example:

  ```
  Mongo.UnorderedBulk.update_one(bulk, %{name: "Waldo"}, %{"$set": %{name: "Greta", kind: "dog"}})
  %Mongo.UnorderedBulk{
  coll: "bulk",
  deletes: [],
  inserts: [],
  updates: [
    {%{name: "Waldo"}, %{"$set": %{kind: "dog", name: "Greta"}}, [multi: false]}
  ]
  }
  ```
  """
  @spec update_one(UnorderedBulk.t(), BSON.document(), BSON.document(), Keyword.t()) :: UnorderedBulk.t()
  def update_one(%UnorderedBulk{} = bulk, filter, update, opts \\ []) do
    get_update_one(filter, update, opts) |> push(bulk)
  end

  @doc """
  Appends a update operation with `:multi = true`.

    Example:

  ```
  Mongo.UnorderedBulk.update_many(bulk, %{name: "Waldo"}, %{"$set": %{name: "Greta", kind: "dog"}})
  %Mongo.UnorderedBulk{
  coll: "bulk",
  deletes: [],
  inserts: [],
  updates: [
    {%{name: "Waldo"}, %{"$set": %{kind: "dog", name: "Greta"}}, [multi: true]}
  ]
  }
  ```
  """
  @spec update_many(UnorderedBulk.t(), BSON.document(), BSON.document(), Keyword.t()) :: UnorderedBulk.t()
  def update_many(%UnorderedBulk{} = bulk, filter, update, opts \\ []) do
    get_update_many(filter, update, opts) |> push(bulk)
  end

  @doc """
  Returns a stream chunk function that can be used with streams. The `limit` specifies the number
  of operation hold in the memory while processing the stream inputs.

  The inputs of the stream should be `Mongo.BulkOps.bulk_op`. See `Mongo.BulkOps`
  """
  @spec write(Enumerable.t(), GenServer.server(), String.t(), non_neg_integer, Keyword.t()) :: Enumerable.t()
  def write(enum, top, coll, limit \\ 1000, opts \\ [])

  def write(enum, top, coll, limit, opts) when limit > 1 do
    Stream.chunk_while(
      enum,
      {new(coll), limit - 1},
      fn
        op, {bulk, 0} -> {:cont, BulkWrite.write(top, push(op, bulk), opts), {new(coll), limit - 1}}
        op, {bulk, l} -> {:cont, {push(op, bulk), l - 1}}
      end,
      fn
        {bulk, _} ->
          case empty?(bulk) do
            true ->
              {:cont, bulk}

            false ->
              {:cont, BulkWrite.write(top, bulk, opts), {new(coll), limit - 1}}
          end
      end
    )
  end

  def write(_enum, _top, _coll, limit, _opts) when limit < 1 do
    raise(ArgumentError, "limit must be greater then 1, got: #{limit}")
  end
end
