defmodule Mongo.OrderedBulk do
  @moduledoc """

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  alias Mongo.OrderedBulk

  defstruct coll: nil, ops: []

  def new(coll) do
    %OrderedBulk{coll: coll}
  end

  def insert_one(%OrderedBulk{ops: rest} = bulk, doc) do
    %OrderedBulk{bulk | ops: [{:insert, doc} | rest] }
  end

  def delete_one(%OrderedBulk{ops: rest} = bulk, doc, opts \\ []) do
    %OrderedBulk{bulk | ops: [{:delete, {doc, Keyword.put(opts, :limit, 1)}} | rest] }
  end

  def delete_many(%OrderedBulk{ops: rest} = bulk, doc, opts \\ []) do
    %OrderedBulk{bulk | ops: [{:delete, {doc, Keyword.put(opts, :limit, 0)}} | rest] }
  end

  def update_one(%OrderedBulk{ops: rest} = bulk, filter, update, opts \\ []) do
    ## _ = modifier_docs(update, :update)
    %OrderedBulk{bulk | ops: [{:update, {filter, update, Keyword.put(opts, :multi, false)}} | rest] }
  end

  def update_many(%OrderedBulk{ops: rest} = bulk, filter, update, opts \\ []) do
    ## _ = modifier_docs(update, :update)
    %OrderedBulk{bulk | ops: [{:update, {filter, update, Keyword.put(opts, :multi, true)}} | rest] }
  end

  def replace_one(%OrderedBulk{ops: rest} = bulk, filter, replacement, opts \\ []) do
    ## _ = modifier_docs(replacement, :replace)
    %OrderedBulk{bulk | ops: [{:update, {filter, replacement, Keyword.put(opts, :multi, false)}} | rest] }
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

    result = Mongo.BulkWrite.bulk_write(top, bulk, w: 1)

    IO.puts inspect result
  end



end