defmodule Mongo.UnorderedBulk do
  @moduledoc """

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  alias Mongo.UnorderedBulk
  import Mongo.BulkUtils

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
    _ = modifier_docs(replacement, :replace)
    get_replace_one(filter, replacement, opts) |> push(bulk)
  end

  def update_one(%UnorderedBulk{} = bulk, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)
    get_update_one(filter, update, opts) |> push(bulk)
  end

  def update_many(%UnorderedBulk{updates: rest} = bulk, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)
    get_update_many(filter, update, opts) |> push(bulk)
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

  def stream(enum, top, coll, limit \\ 1000, opts \\ []) when limit > 1 do
    Stream.chunk_while(enum,
      {new(coll), limit - 1},
      fn
        op, {bulk, 0} -> {:cont, Mongo.BulkWrite.bulk_write(top, push(op, bulk), opts), {new(coll), limit - 1}}
        op, {bulk, l} -> {:cont, {push(op, bulk), l - 1}}
      end,
      fn
        {bulk, 0} -> {:cont, bulk}
        {bulk, _} -> {:cont, Mongo.BulkWrite.bulk_write(top, bulk, opts), {new(coll), limit - 1}}
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

    result = Mongo.BulkWrite.bulk_write(top, bulk, w: 1)

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

    result = Mongo.BulkWrite.bulk_write(top, bulk, w: 1)

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

    result = Mongo.BulkWrite.bulk_write(top, bulk, w: 1)

    IO.puts inspect result
  end


end