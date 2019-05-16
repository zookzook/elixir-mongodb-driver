defmodule Mongo.BulkUtils do

  def get_insert_one(doc), do: {:insert, doc}

  def get_delete_one(doc, opts \\ []), do: {:delete, {doc, Keyword.put(opts, :limit, 1)}}
  def get_delete_many(doc, opts \\ []), do: {:delete, {doc, Keyword.put(opts, :limit, 0)}}

  def get_update_one(filter, update, opts \\ []) do
    ## _ = modifier_docs(update, :update)
    {:update, {filter, update, Keyword.put(opts, :multi, false)}}
  end

  def get_update_many(filter, update, opts \\ []) do
    ## _ = modifier_docs(update, :update)
    {:update, {filter, update, Keyword.put(opts, :multi, true)}}
  end

  def get_replace_one(filter, replacement, opts \\ []) do
    ## _ = modifier_docs(replacement, :replace)
    {:update, {filter, replacement, Keyword.put(opts, :multi, false)}}
  end

end