defmodule Mongo.BulkWrite do
  @moduledoc """

  todo

  Ist immer für eine Collections

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  import Mongo.Utils
  alias Mongo.UnorderedBulk
  alias Mongo.OrderedBulk

  @doc """
  Unordered bulk write operations:
  Executes first insert commands, then updates commands and after that all delete commands are executed. If a group (inserts, updates or deletes) exceeds the limit
  maxWriteBatchSize it will be split into chunks. Everything is done in memory, so this use case is limited by memory. A better approach seems to use streaming bulk writes.
  """
  def bulk_write(topology_pid, %UnorderedBulk{} = bulk, opts) do

    write_concern = write_concern(opts)
    with {:ok, conn, _, _} <- Mongo.select_server(topology_pid, :write, opts) do
      one_bulk_write(conn, bulk, write_concern, opts)
    end
  end

  @doc """
  Schreibt den OrderedBulk in die Datenbank. Es erfolgt eine kleine Optimierung. Folgen von gleichen Operationen
  werden zusammengefasst und als ein Befehl gesendet.
  """
  def bulk_write(topology_pid, %OrderedBulk{coll: coll, ops: ops} = bulk, opts) do

    write_concern = write_concern(opts)
    with {:ok, conn, _, _} <- Mongo.select_server(topology_pid, :write, opts) do
      get_op_sequence(coll, ops)
      |> Enum.map(fn {cmd, docs} -> one_bulk_write_operation(conn, cmd, coll, docs, write_concern, opts) end)
      |> Enum.each(fn {cmd, count} -> IO.puts "#{cmd} : #{count}" end)
    end
  end

  ##
  # returns the current write concerns from `opts`
  #
  defp write_concern(opts) do
    %{
      w: Keyword.get(opts, :w),
      j: Keyword.get(opts, :j),
      wtimeout: Keyword.get(opts, :wtimeout)
    } |> filter_nils()
  end

  @doc"""
  Executues one unordered bulk write. The execution order of operation groups is

  * inserts
  * updates
  * deletes

  The function returns a keyword list with the results of each operation group:
  For the details see https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#results
  """
  def one_bulk_write(conn, %UnorderedBulk{coll: coll, inserts: inserts, updates: updates, deletes: deletes} = bulk, write_concern, opts) do

    with {_, inserts} <- one_bulk_write_operation(conn, :insert, coll, inserts, write_concern, opts),
         {_, updates} <- one_bulk_write_operation(conn, :update, coll, updates, write_concern, opts),
         {_, deletes} <- one_bulk_write_operation(conn, :delete, coll, deletes, write_concern, opts) do
      [
        acknowledged: acknowledged(write_concern),
        insertedCount: inserts,
        matchedCount: updates,
        deletedCount: deletes,
        upsertedCount: 0,
        upsertedIds: [],
        insertedIds: [],
      ]
    end
  end

  ###
  # Executes the command `cmd` and collects the result.
  #
  def one_bulk_write_operation(conn, cmd, coll, docs, write_concern, opts) do
    with result <- conn |> run_commands(get_cmds(cmd, coll, docs, write_concern, opts), opts) |> collect(cmd) do
      {cmd, result}
    end
  end

  ##
  # Converts the list of operations into insert/update/delete commands
  #
  defp get_cmds(:insert, coll, docs, write_concern, opts), do: get_insert_cmds(coll, docs, write_concern, opts)
  defp get_cmds(:update, coll, docs, write_concern, opts), do: get_update_cmds(coll, docs, write_concern, opts)
  defp get_cmds(:delete, coll, docs, write_concern, opts), do: get_delete_cmds(coll, docs, write_concern, opts)

  defp acknowledged(%{w: w}) when w > 0, do: true
  defp acknowledged(%{}), do: false

  ###
  # Converts the list of operations into list of lists with same operations.
  #
  # [inserts, inserts, updates] -> [[inserts, inserts],[updates]]
  #
  defp get_op_sequence(coll, ops) do
    get_op_sequence(coll, ops, [])
  end
  defp get_op_sequence(coll, [], acc), do: acc
  defp get_op_sequence(coll, ops, acc) do
    [{kind, _doc} | _rest] = ops
    {docs, rest} = find_max_sequence(kind, ops)
    get_op_sequence(coll, rest, [{kind, docs} | acc])
  end

  ###
  # Splits the sequence of operations into two parts
  # 1) sequence of operations of kind `kind`
  # 2) rest of operations
  #
  defp find_max_sequence(kind, rest) do
    find_max_sequence(kind, rest, [])
  end
  defp find_max_sequence(_kind, [], acc) do
    {acc, []}
  end
  defp find_max_sequence(kind, [{other, desc} | rest], acc) when kind == other do
    find_max_sequence(kind, rest, [desc | acc])
  end
  defp find_max_sequence(_kind, rest, acc) do
    {acc, rest}
  end

#  {
#"acknowledged" : true,
#"deletedCount" : 1,
#"insertedCount" : 2,
#              "matchedCount" : 2,
#"upsertedCount" : 0,
#"insertedIds" : {
# "0" : 4,
#"1" : 5
#},
#"upsertedIds" : {
#
# }
# }

  def collect(docs, :insert) do
    docs
    |> Enum.map(fn
      {:ok, %{"n" => n}} -> n
      {:ok, _other}      -> 0
    end)
    |> Enum.reduce(0, fn x, acc -> x + acc end)
  end

  def collect(docs, :update) do
    docs
    |> Enum.map(fn
      {:ok, %{"n" => n}} -> n
      {:ok, _other}      -> 0
    end)
    |> Enum.reduce(0, fn x, acc -> x + acc end)
  end

  def collect(docs, :delete) do
    docs
    |> Enum.map(fn
      {:ok, %{"n" => n}} -> n
      {:ok, _other}      -> 0
    end)
    |> Enum.reduce(0, fn x, acc -> x + acc end)
  end

  defp run_commands(conn, cmds, opts) do

    IO.puts "Running cmds #{inspect cmds}"

    cmds
    |> Enum.map(fn cmd -> Mongo.direct_command(conn, cmd, opts) end)
    |> Enum.map(fn {:ok, doc} -> {:ok, doc} end)
  end

  def get_insert_cmds(coll, docs, write_concern, _opts) do

    max_batch_size = 10 ## only for test maxWriteBatchSize

    {_ids, docs} = assign_ids(docs)

    docs
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn inserts -> get_insert_cmd(coll, inserts, write_concern) end)

  end

  defp get_insert_cmd(coll, inserts, write_concern) do
    [insert: coll,
     documents: inserts,
     writeConcern: write_concern] |> filter_nils()
  end

  defp get_delete_cmds(coll, docs, write_concern, opts) do

    max_batch_size = 10 ## only for test maxWriteBatchSize
    docs
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn deletes -> get_delete_cmd(coll, deletes, write_concern, opts) end)

  end

  defp get_delete_cmd(coll, deletes, write_concern, opts ) do
    [delete: coll,
     deletes: Enum.map(deletes, fn delete -> get_delete_doc(delete) end),
     ordered: Keyword.get(opts, :ordered),
     writeConcern: write_concern] |> filter_nils()
  end
  defp get_delete_doc({filter, opts}) do
    [q: filter,
     limit: Keyword.get(opts, :limit),
     collation: Keyword.get(opts, :collaction)] |> filter_nils()
  end

  defp get_update_cmds(coll, docs, write_concern, opts) do

    max_batch_size = 10 ## only for test maxWriteBatchSize
    docs
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn updates -> get_update_cmd(coll, updates, write_concern, opts) end)

  end

  defp get_update_cmd(coll, updates, write_concern, opts) do
    [ update: coll,
      updates: Enum.map(updates, fn update -> get_update_doc(update) end),
      ordered: Keyword.get(opts, :ordered),
      writeConcern: write_concern,
      bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
    ] |> filter_nils()
  end

  defp get_update_doc({filter, update, update_opts}) do
    [ q: filter,
      u: update,
      upsert: Keyword.get(update_opts, :upsert),
      multi: Keyword.get(update_opts, :multi) || false,
      collation: Keyword.get(update_opts, :collation),
      arrayFilters: Keyword.get(update_opts, :filters)
    ] |> filter_nils()
  end
  defp get_update_doc(_other) do
    []
  end

end
