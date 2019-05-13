defmodule Mongo.UnorderedBulk do
  @moduledoc """

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  alias Mongo.UnorderedBulk

  defstruct coll: nil, inserts: [], updates: [], deletes: []

  def new(coll) do
    %UnorderedBulk{coll: coll}
  end

  def insert_one(%UnorderedBulk{inserts: rest} = bulk, doc) do
    %UnorderedBulk{bulk | inserts: [doc | rest] }
  end

  def delete_one(%UnorderedBulk{deletes: rest} = bulk, doc, collaction \\ nil) do
    %UnorderedBulk{bulk | deletes: [{doc, collaction, 1} | rest] }
  end

  def delete_many(%UnorderedBulk{deletes: rest} = bulk, doc, collaction \\ nil) do
    %UnorderedBulk{bulk | deletes: [{doc, collaction, 0} | rest] }
  end

  def update_one(%UnorderedBulk{updates: rest} = bulk, filter, update, opts \\ []) do
    %UnorderedBulk{bulk | updates: [{filter, update, opts} | rest] }
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

    result = Mongo.BulkWrite.bulk_write(top, bulk)

    IO.puts inspect result
  end
end