defmodule Mongo.OrderedBulk do
  @moduledoc """
  An **ordered** bulk is filled in the memory with the bulk operations. If the ordered bulk is written to the database, the order
  is preserved. Only same types of operation are grouped and only if they have been inserted one after the other.


  ## Example

  ```
  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite

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

  BulkWrite.write(:mongo, bulk, w: 1)
  ```

  This example would not work by using an unordered bulk, because the `OrderedBulk.update_many` would executed too early.

  To reduce the memory usage the ordered bulk can be used with streams as well.

  ## Example

  ```
   alias Mongo.OrderedBulk

   1..1000
   |> Stream.map(fn
     1    -> BulkOps.get_insert_one(%{count: 1})
     1000 -> BulkOps.get_delete_one(%{count: 999})
     i    -> BulkOps.get_update_one(%{count: i - 1}, %{"$set": %{count: i}})
   end)
   |> OrderedBulk.write(:mongo, "bulk", 25)
   |> Stream.run()
  ```

  Of course, this example is a bit silly. A sequence of update operations is created that only work in the correct order.

  """

  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite

  import Mongo.BulkOps

  @type t :: %__MODULE__{
          coll: String.t(),
          ops: [Mongo.BulkOps.bulk_op()]
        }

  defstruct coll: nil, ops: []

  @doc """
  Creates an empty ordered bulk for a collection.

  Example:

  ```
  Mongo.orderedBulk.new("bulk")
  %Mongo.OrderedBulk{coll: "bulk", ops: []}
  ```
  """
  @spec new(String.t()) :: OrderedBulk.t()
  def new(coll) do
    %OrderedBulk{coll: coll}
  end

  @doc """
  Returns true, if the bulk is empty, that means it contains no inserts, updates or deletes operations
  """
  def empty?(%OrderedBulk{ops: []}) do
    true
  end

  def empty?(_other) do
    false
  end

  @doc """
  Appends a bulk operation to the ordered bulk.
  """
  @spec push(Mongo.BulkOps.bulk_op(), OrderedBulk.t()) :: OrderedBulk.t()
  def push(op, %OrderedBulk{ops: rest} = bulk) do
    %OrderedBulk{bulk | ops: [op | rest]}
  end

  @doc """
  Appends an insert operation.

  Example:

  ```
  Mongo.OrderedBulk.insert_one(bulk, %{name: "Waldo"})
  %Mongo.OrderedBulk{coll: "bulk", ops: [insert: %{name: "Waldo"}]}
  ```
  """
  @spec insert_one(OrderedBulk.t(), BSON.document()) :: OrderedBulk.t()
  def insert_one(%OrderedBulk{} = bulk, doc) do
    get_insert_one(doc) |> push(bulk)
  end

  @doc """
  Appends a delete operation with `:limit = 1`.

  Example:

  ```
  Mongo.OrderedBulk.delete_one(bulk, %{name: "Waldo"})
  %Mongo.OrderedBulk{coll: "bulk", ops: [delete: {%{name: "Waldo"}, [limit: 1]}]}
  ```
  """
  @spec delete_one(OrderedBulk.t(), BSON.document()) :: OrderedBulk.t()
  def delete_one(%OrderedBulk{} = bulk, doc) do
    get_delete_one(doc) |> push(bulk)
  end

  @doc """
  Appends a delete operation with `:limit = 0`.

    Example:

  ```
  Mongo.OrderedBulk.delete_many(bulk, %{name: "Waldo"})
  %Mongo.OrderedBulk{coll: "bulk", ops: [delete: {%{name: "Waldo"}, [limit: 0]}]}
  ```
  """
  @spec delete_many(OrderedBulk.t(), BSON.document()) :: OrderedBulk.t()
  def delete_many(%OrderedBulk{} = bulk, doc) do
    get_delete_many(doc) |> push(bulk)
  end

  @doc """
  Appends a replace operation with `:multi = false`.

    Example:

  ```
  Mongo.OrderedBulk.replace_one(bulk, %{name: "Waldo"}, %{name: "Greta", kind: "dog"})
  %Mongo.OrderedBulk{
  coll: "bulk",
  ops: [
    update: {%{name: "Waldo"}, %{kind: "dog", name: "Greta"}, [multi: false]}
  ]
  }
  ```
  """
  @spec replace_one(OrderedBulk.t(), BSON.document(), BSON.document(), Keyword.t()) :: OrderedBulk.t()
  def replace_one(%OrderedBulk{} = bulk, filter, replacement, opts \\ []) do
    get_replace_one(filter, replacement, opts) |> push(bulk)
  end

  @doc """
  Appends a update operation with `:multi = false`.

    Example:

  ```
  Mongo.OrderedBulk.update_one(bulk, %{name: "Waldo"}, %{"$set": %{name: "Greta", kind: "dog"}})
  %Mongo.OrderedBulk{
  coll: "bulk",
  ops: [
    update: {%{name: "Waldo"}, %{"$set": %{kind: "dog", name: "Greta"}},
     [multi: false]}
  ]
  }
  ```
  """
  @spec update_one(OrderedBulk.t(), BSON.document(), BSON.document(), Keyword.t()) :: OrderedBulk.t()
  def update_one(%OrderedBulk{} = bulk, filter, update, opts \\ []) do
    get_update_one(filter, update, opts) |> push(bulk)
  end

  @doc """
  Appends a update operation with `:multi = true`.

    Example:

  ```
  Mongo.OrderedBulk.update_many(bulk, %{name: "Waldo"}, %{"$set": %{name: "Greta", kind: "dog"}})
  %Mongo.OrderedBulk{
  coll: "bulk",
  ops: [
    update: {%{name: "Waldo"}, %{"$set": %{kind: "dog", name: "Greta"}},
     [multi: true]}
  ]
  }
  ```
  """
  @spec update_many(OrderedBulk.t(), BSON.document(), BSON.document(), Keyword.t()) :: OrderedBulk.t()
  def update_many(%OrderedBulk{} = bulk, filter, update, opts \\ []) do
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
