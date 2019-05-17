defmodule Mongo.UnorderedBulk do
  @moduledoc """

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  alias Mongo.UnorderedBulk
  alias Mongo.BulkWrite

  import Mongo.BulkOps
  import Mongo.Utils

  @type t :: %__MODULE__{
               coll: String.t,
               inserts: [BulkOps.bulk_op],
               updates: [BulkOps.bulk_op],
               deletes: [BulkOps.bulk_op]
             }

  defstruct coll: nil, inserts: [], updates: [], deletes: []

  def new(coll) do
    %UnorderedBulk{coll: coll}
  end

  def push({:insert, doc}, %UnorderedBulk{inserts: rest} = bulk) do
    %UnorderedBulk{bulk | inserts: [doc | rest] }
  end
  def push({:update, doc}, %UnorderedBulk{updates: rest} = bulk) do
    %UnorderedBulk{bulk | updates: [doc | rest] }
  end
  def push({:delete, doc}, %UnorderedBulk{deletes: rest} = bulk) do
    %UnorderedBulk{bulk | deletes: [doc | rest] }
  end

  def insert_one(%UnorderedBulk{} = bulk, doc) do
    get_insert_one(doc) |> push(bulk)
  end

  def delete_one(%UnorderedBulk{} = bulk, doc, opts \\ []) do
    get_delete_one(doc, opts) |> push(bulk)
  end

  def delete_many(%UnorderedBulk{} = bulk, doc, opts \\ []) do
    get_delete_many(doc, opts) |> push(bulk)
  end

  def replace_one(%UnorderedBulk{} = bulk, filter, replacement, opts \\ []) do
    get_replace_one(filter, replacement, opts) |> push(bulk)
  end

  def update_one(%UnorderedBulk{} = bulk, filter, update, opts \\ []) do
    get_update_one(filter, update, opts) |> push(bulk)
  end

  def update_many(%UnorderedBulk{} = bulk, filter, update, opts \\ []) do
    get_update_many(filter, update, opts) |> push(bulk)
  end

  def write(enum, top, coll, limit \\ 1000, opts \\ []) when limit > 1 do
    Stream.chunk_while(enum,
      {new(coll), limit - 1},
      fn
        op, {bulk, 0} -> {:cont, BulkWrite.write(top, push(op, bulk), opts), {new(coll), limit - 1}}
        op, {bulk, l} -> {:cont, {push(op, bulk), l - 1}}
      end,
      fn
        {bulk, 0} -> {:cont, bulk}
        {bulk, _} -> {:cont, BulkWrite.write(top, bulk, opts), {new(coll), limit - 1}}
    end)
    # todo reduce to one
  end

  def test(top) do

    bulk = "bulk"
    |> new()
    |> insert_one(%{name: "Greta"})
    |> insert_one(%{name: "Tom"})
    |> insert_one(%{name: "Waldo"})
    |> update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
    |> update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
    |> update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
    |> delete_one(%{kind: "dog"})
    |> delete_one(%{kind: "dog"})
    |> delete_one(%{kind: "dog"})

    result = BulkWrite.write(top, bulk, w: 1)

    IO.puts inspect result
  end

  def test3(top) do

    bulk = "bulk"
           |> new()
           |> insert_one(%{name: "Greta"})
           |> insert_one(%{name: "Tom"})
           |> insert_one(%{name: "Waldo"})
           |> replace_one(%{name: "Waldo"}, %{name: "Waldo", kind: "dog"})
           |> replace_one(%{name: "Greta"}, %{name: "Greta", kind: "dog"})
           |> replace_one(%{name: "Tom"}, %{name: "Tom", kind: "dog"})
           |> delete_many(%{kind: "dog"})

    result = BulkWrite.write(top, bulk, w: 1)

    IO.puts inspect result
  end


  def test5(top) do

    bulk = "bulk"
           |> new()
           |> insert_one(%{name: "Greta"})
           |> insert_one(%{name: "Tom"})
           |> insert_one(%{name: "Waldo"})
           |> update_many(%{name: %{"$exists": true}}, %{"$set": %{kind: "dog"}})
           |> delete_many(%{kind: "dog"})

    result = BulkWrite.write(top, bulk, w: 1)

    IO.puts inspect result
  end


end