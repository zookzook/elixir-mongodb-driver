defmodule Mongo.BulkOps do
  @moduledoc """

  This module defines bulk operation for insert, update and delete. A bulk operation is a tuple of two elements

  1. an atom, which specify the type `:insert`, `:update` and `:delete`
  2. a document or another tuple which contains all parameters of the operation.

  You use these function in streams:

  ## Example

  ```
  alias Mongo.UnorderedBulk
  alias Mongo.BulkOps

  Filestream!("large.csv")
  |> Stream.map(&String.trim(&1))
  |> Stream.map(&String.split(&1,","))
  |> Stream.map(fn [firstname | [lastname | _]] -> %{firstname: firstname, lastname: lastname} end)
  |> Stream.map(fn doc -> BulkOps.get_insert_one(doc) end)
  |> UnorderedBulk.write(:mongo, "bulk", 1_000)
  |> Stream.run()
  ```

  """

  @type bulk_op ::
          {atom, BSON.document()}
          | {atom, {BSON.document(), Keyword.t()}}
          | {atom, {BSON.document(), BSON.document(), Keyword.t()}}

  import Mongo.Utils

  @doc """
  Returns an `insert_one` operation tuple for appending to a bulk. Used to perform stream bulk writes.

    Example
  ```
  Mongo.BulkOps.get_insert_one(%{name: "Waldo"})

  {:insert, %{name: "Waldo"}}
  ```
  """
  @spec get_insert_one(BSON.document()) :: bulk_op
  def get_insert_one(doc), do: {:insert, doc}

  @doc """
  Returns an `delete_one` operation tuple for appending to a bulk. Used to perform stream bulk writes.

    Example

  ```
  Mongo.BulkOps.get_delete_one(%{name: "Waldo"})

  {:delete, {%{name: "Waldo"}, [limit: 1]}}
  ```
  """
  @spec get_delete_one(BSON.document()) :: bulk_op
  def get_delete_one(doc), do: {:delete, {doc, [limit: 1]}}

  @doc """
  Returns an `delete_many` operation for appending to a bulk. Used to perform stream bulk writes.

    Example

  ```
  Mongo.BulkOps.get_delete_many(%{name: "Waldo"})

  {:delete, {%{name: "Waldo"}, [limit: 0]}}
  ```
  """
  @spec get_delete_many(BSON.document()) :: bulk_op
  def get_delete_many(doc), do: {:delete, {doc, [limit: 0]}}

  @doc """
  Returns an `update_one` operation for appending to a bulk. Used to perform stream bulk writes.

      Example

  ```
  Mongo.BulkOps.get_update_one(%{name: "Waldo"}, %{"$set" : %{name: "Greta", kind: "dog"}})

  {:update,
    {%{name: "Waldo"}, %{"$set": %{kind: "dog", name: "Greta"}}, [multi: false]}}
  ```
  """
  @spec get_update_one(BSON.document(), BSON.document(), Keyword.t()) :: bulk_op
  def get_update_one(filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)
    {:update, {filter, update, Keyword.put(opts, :multi, false)}}
  end

  @doc """
  Returns an `update_many` operation for appending to a bulk. Used to perform stream bulk writes.

    Example

  ```
  Mongo.BulkOps.get_update_many(%{name: "Waldo"}, %{"$set" : %{name: "Greta", kind: "dog"}})

  {:update,
    {%{name: "Waldo"}, %{"$set": %{kind: "dog", name: "Greta"}}, [multi: true]}}
  ```
  """
  @spec get_update_many(BSON.document(), BSON.document(), Keyword.t()) :: bulk_op
  def get_update_many(filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)
    {:update, {filter, update, Keyword.put(opts, :multi, true)}}
  end

  @doc """
  Returns an `replace_one` operation for appending to a bulk. Used to perform stream bulk writes.

    Example

  ```
  Mongo.BulkOps.get_replace_one(%{name: "Waldo"}, %{name: "Greta", kind: "dog"})

  {:update, {%{name: "Waldo"}, %{kind: "dog", name: "Greta"}, [multi: false]}}
  ```
  """
  @spec get_replace_one(BSON.document(), BSON.document(), Keyword.t()) :: bulk_op
  def get_replace_one(filter, replacement, opts \\ []) do
    _ = modifier_docs(replacement, :replace)
    {:update, {filter, replacement, Keyword.put(opts, :multi, false)}}
  end
end
