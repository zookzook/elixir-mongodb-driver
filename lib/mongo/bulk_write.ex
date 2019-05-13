defmodule Mongo.BulkWrite do
  @moduledoc """

  todo

  Ist immer für eine Collections

   The maxWriteBatchSize limit of a database, which indicates the maximum number of write operations permitted in a write batch, raises from 1,000 to 100,000.

  """

  import Mongo.Utils
  alias Mongo.UnorderedBulk

  @doc """
  Unordered bulk write operations:
  Executes first insert commands, then all update commands and after that all delete commands are executed. If a group (inserts, updates or deletes) exceeds the limit
  maxWriteBatchSize it will be split into chunks. Everything is done in memory, so this use case is limited by memory. A better approach seems to use streaming bulk writes.
  """
  def bulk_write(topology_pid, %UnorderedBulk{} = bulk, opts \\ []) do

    write_concern = %{
                      w: Keyword.get(opts, :w),
                      j: Keyword.get(opts, :j),
                      wtimeout: Keyword.get(opts, :wtimeout)
                    } |> filter_nils()

    with {:ok, conn, _, _} <- Mongo.select_server(topology_pid, :write, opts),
         inserts <- conn |> run_commands(get_insert_cmds(bulk, write_concern), opts) |> collect(:inserts),
         updates <- conn |> run_commands(get_update_cmds(bulk, write_concern, opts), opts) |> collect(:updates),
         deletes <- conn |> run_commands(get_delete_cmds(bulk, write_concern, opts), opts) |> collect(:deletes) do
      inserts ++ updates ++ deletes
    end
  end

  def collect(doc, :inserts) do

  end

  def collect(doc, :updates) do

  end

  def collect(doc, :deletes) do

  end

  defp run_commands(conn, cmds, opts) do

    IO.puts "Running cmsd #{inspect cmds}"

    cmds
    |> Enum.map(fn cmd -> Mongo.direct_command(conn, cmd, opts) end)
    |> Enum.map(fn {:ok, doc} -> {:ok, doc} end)
  end

  def get_insert_cmds(%UnorderedBulk{coll: coll, inserts: all_inserts}, write_concern) do

    max_batch_size = 10 ## only for test maxWriteBatchSize

    {_ids, all_inserts} = assign_ids(all_inserts)

    all_inserts
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn inserts -> get_insert_cmd(coll, inserts, write_concern) end)

  end

  defp get_insert_cmd(coll, inserts, write_concern) do
    filter_nils([insert: coll, documents: inserts, writeConcern: write_concern])
  end

  defp get_delete_cmds(%UnorderedBulk{coll: coll, deletes: all_deletes}, write_concern, opts) do

    max_batch_size = 10 ## only for test maxWriteBatchSize
    all_deletes
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn deletes -> get_delete_cmd(coll, deletes, write_concern, opts) end)

  end

  defp get_delete_cmd(coll, deletes, write_concern, opts ) do
    filter_nils([delete: coll,
                 deletes: Enum.map(deletes, fn delete -> get_delete_doc(delete) end),
                 ordered: Keyword.get(opts, :ordered),
                 writeConcern: write_concern])
  end
  defp get_delete_doc({filter, collaction, limit}) do
    %{q: filter, limit: limit, collation: collaction} |> filter_nils()
  end

  defp get_update_cmds(%UnorderedBulk{coll: coll, updates: all_updates}, write_concern, opts) do

    max_batch_size = 10 ## only for test maxWriteBatchSize
    all_updates
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
