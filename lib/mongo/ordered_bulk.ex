defmodule Mongo.OrderedBulk do
  @moduledoc """

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite

  import Mongo.BulkOps

  @type t :: %__MODULE__{
               coll: String.t,
               ops: [BulkOps.bulk_op]
             }

  defstruct coll: nil, ops: []

  def new(coll) do
    %OrderedBulk{coll: coll}
  end

  def push(op, %OrderedBulk{ops: rest} = bulk) do
    %OrderedBulk{bulk | ops: [op | rest] }
  end

  def insert_one(%OrderedBulk{} = bulk, doc) do
    get_insert_one(doc) |> push(bulk)
  end

  def delete_one(%OrderedBulk{} = bulk, doc) do
    get_delete_one(doc) |> push(bulk)
  end

  def delete_many(%OrderedBulk{} = bulk, doc) do
    get_delete_many(doc) |> push(bulk)
  end

  def replace_one(%OrderedBulk{} = bulk, filter, replacement, opts \\ []) do
    get_replace_one(filter, replacement, opts) |> push(bulk)
  end

  def update_one(%OrderedBulk{} = bulk, filter, update, opts \\ []) do
    get_update_one(filter, update, opts) |> push(bulk)
  end

  def update_many(%OrderedBulk{} = bulk, filter, update, opts \\ []) do
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

  def test() do

    seeds = ["127.0.0.1:27001", "127.0.0.1:27002", "127.0.0.1:27003"]
    {:ok, top} = Mongo.start_link(database: "me", seeds: seeds, show_sensitive_data_on_connection_error: true)

    bulk = "bulk"
           |> new()
           |> insert_one(%{name: "Greta"})
           |> insert_one(%{name: "Tom"})
           |> insert_one(%{name: "Waldo"})
           |> update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
           |> update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
           |> update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
           |> update_many(%{kind: "dog"}, %{"$set": %{kind: "cat"}})
           |> delete_one(%{kind: "cat"})
           |> delete_one(%{kind: "cat"})
           |> delete_one(%{kind: "cat"})

    IO.puts inspect bulk

    result = Mongo.BulkWrite.write(top, bulk, w: 1)

    IO.puts inspect result
  end



end