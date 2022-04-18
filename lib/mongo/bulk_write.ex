defmodule Mongo.BulkWrite do
  @moduledoc """

  The driver supports the so-called bulk writes ([Specification](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#basic)):

  The motivation for bulk writes lies in the possibility of optimizing to group the same operations.  The driver supports

  * unordered and ordered bulk writes
  * in-memory and stream bulk writes

  ## Unordered bulk writes

  Unordered bulk writes have the highest optimization factor. Here all operations can be divided into
  three groups (inserts, updates and deletes).
  The order of execution within a group does not matter. However, the groups are executed in the
  order: inserts, updates and deletes. The following example creates three records, changes them, and then
  deletes all records. After execution, the collection is unchanged. It's valid, because of the execution order:

  1. inserts
  2. updates
  3. deletes

  ## Example:

  ```
  alias Mongo.BulkWrite
  alias Mongo.UnorderedBulk

  bulk = "bulk"
      |> UnorderedBulk.new()
      |> UnorderedBulk.insert_one(%{name: "Greta"})
      |> UnorderedBulk.insert_one(%{name: "Tom"})
      |> UnorderedBulk.insert_one(%{name: "Waldo"})
      |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.delete_one(%{kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})

  result = BulkWrite.write(:mongo, bulk, w: 1)
  ```

  ## Ordered bulk writes

  Sometimes the order of execution is important for successive operations to yield a correct result.
  In this case, one uses ordered bulk writes. The following example would not work with unordered bulk writes
  because the order within the update operations is undefined. The `update_many()` will only work, if it is
  executed after the `update_one()` functions.

  ```
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

  result = BulkWrite.write(:mongo, bulk, w: 1)
  ```

  ## Stream bulk writes

  The examples shown initially filled the bulk with a few operations and then the bulk is written to the database.
  This is all done in memory. For larger amounts of operations or imports of very long files, the main memory would
  be unnecessarily burdened. It could come to some resource problems.

  For such cases you could use streams. Unordered and ordered bulk writes can also be combined with Streams.
  You set the maximum size of the bulk. Once the number of bulk operations has been reached,
  it will be sent to the database. While streaming you can limit the memory consumption regarding the current task.

  In the following example we import 1.000.000 integers into the MongoDB using the stream api:

  We need to create an insert operation (`BulkOps.get_insert_one()`) for each number. Then we call the `UnorderedBulk.stream`
  function to import it. This function returns a stream function which accumulate
  all inserts operations until the limit `1000` is reached. In this case the operation group is written to
  MongoDB.

  ## Example

  ```
  1..1_000_000
    |> Stream.map(fn i -> BulkOps.get_insert_one(%{number: i}) end)
    |> UnorderedBulk.write(:mongo, "bulk", 1_000)
    |> Stream.run()
  ```

  ## Benchmark

  The following benchmark compares multiple `Mongo.insert_one()` calls with a stream using unordered bulk writes.
  Both tests inserts documents into a replica set with `w: 1`.

  ```
  Benchee.run(
      %{
        "inserts" => fn input ->
         input
         |> Enum.map(fn i -> %{number: i} end)
         |> Enum.each(fn doc -> Mongo.insert_one!(top, "bulk_insert", doc) end)
        end,
        "streams" => fn input ->
                        input
                        |> Stream.map(fn i -> get_insert_one(%{number: i}) end)
                        |> Mongo.UnorderedBulk.write(top, "bulk", 1_0000)
                        |> Stream.run()
        end,
      },
      inputs: %{
        "Small" => Enum.to_list(1..10_000),
        "Medium" => Enum.to_list(1..100_000),
        "Bigger" => Enum.to_list(1..1_000_000)
      }
    )
  ```

  Result:

  ```
  ##### With input Bigger #####
  Name              ips        average  deviation         median         99th %
  streams        0.0885      0.188 min     ±0.00%      0.188 min      0.188 min
  inserts       0.00777       2.14 min     ±0.00%       2.14 min       2.14 min

  Comparison:
  streams        0.0885
  inserts       0.00777 - 11.39x slower +1.96 min

  ##### With input Medium #####
  Name              ips        average  deviation         median         99th %
  streams          1.00         1.00 s     ±8.98%         0.99 s         1.12 s
  inserts        0.0764        13.09 s     ±0.00%        13.09 s        13.09 s

  Comparison:
  streams          1.00
  inserts        0.0764 - 13.12x slower +12.10 s

  ##### With input Small #####
  Name              ips        average  deviation         median         99th %
  streams          8.26        0.121 s    ±30.46%        0.112 s         0.23 s
  inserts          0.75         1.34 s     ±7.15%         1.29 s         1.48 s

  Comparison:
  streams          8.26
  inserts          0.75 - 11.07x slower +1.22 s
  ```

  The result is, that using bulk writes is much faster (about 15x faster at all).

  """

  import Keywords
  import Mongo.Utils
  import Mongo.WriteConcern
  import Mongo.Session, only: [in_write_session: 3]

  alias Mongo.UnorderedBulk
  alias Mongo.OrderedBulk
  alias Mongo.BulkWriteResult

  @doc """
  Executes unordered and ordered bulk writes.

  ## Unordered bulk writes
  The operation are grouped (inserts, updates, deletes). The order of execution is:

  1. inserts
  2. updates
  3. deletes

  The execution order within the group is not preserved.

  ## Ordered bulk writes
  Sequences of the same operations are grouped and sent as one command. The order is preserved.

  If a group (inserts, updates or deletes) exceeds the limit `maxWriteBatchSize` it will be split into chunks.
  Everything is done in memory, so this use case is limited by memory. A better approach seems to use streaming bulk writes.
  """
  @spec write(GenServer.server(), UnorderedBulk.t() | OrderedBulk.t(), Keyword.t()) :: Mongo.BulkWriteResult.t()
  def write(topology_pid, bulk, opts \\ [])

  def write(topology_pid, %UnorderedBulk{} = bulk, opts) do
    in_write_session(topology_pid, &one_bulk_write(&1, topology_pid, bulk, &2), opts)
  end

  def write(topology_pid, %OrderedBulk{} = bulk, opts) do
    in_write_session(topology_pid, &write_ordered_bulk(&1, topology_pid, bulk, &2), opts)
  end

  defp write_ordered_bulk(session, topology_pid, %OrderedBulk{coll: coll, ops: ops}, opts) do
    write_concern = write_concern(opts)

    empty = %BulkWriteResult{acknowledged: acknowledged?(write_concern)}

    with {:ok, limits} <- Mongo.limits(topology_pid) do
      max_batch_size = limits.max_write_batch_size

      ops
      |> get_op_sequence()
      |> Enum.reduce_while(empty, fn {cmd, docs}, acc ->
        temp_result = one_bulk_write_operation(session, cmd, coll, docs, max_batch_size, opts)

        case temp_result do
          %{errors: []} ->
            {:cont, BulkWriteResult.add(acc, temp_result)}

          _other ->
            {:halt, BulkWriteResult.add(acc, temp_result)}
        end
      end)
    end
  end

  ##
  # Executes one unordered bulk write. The execution order of operation groups is
  #
  # * inserts
  # * updates
  # * deletes
  #
  # The function returns a keyword list with the results of each operation group:
  # For the details see https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#results
  #
  defp one_bulk_write(session, topology_pid, %UnorderedBulk{coll: coll, inserts: inserts, updates: updates, deletes: deletes}, opts) do
    with {:ok, limits} <- Mongo.limits(topology_pid) do
      max_batch_size = limits.max_write_batch_size

      results =
        case one_bulk_write_operation(session, :insert, coll, inserts, max_batch_size, opts) do
          %{errors: []} = insert_result ->
            case one_bulk_write_operation(session, :update, coll, updates, max_batch_size, opts) do
              %{errors: []} = update_result ->
                delete_result = one_bulk_write_operation(session, :delete, coll, deletes, max_batch_size, opts)
                [insert_result, update_result, delete_result]

              update_result ->
                [insert_result, update_result]
            end

          insert_result ->
            [insert_result]
        end

      BulkWriteResult.reduce(results, %BulkWriteResult{acknowledged: acknowledged?(opts)})
    end
  end

  ###
  # Executes the command `cmd` and collects the result.
  #
  defp one_bulk_write_operation(session, cmd, coll, docs, max_batch_size, opts) do
    session
    |> run_commands(get_cmds(cmd, coll, docs, max_batch_size, opts), opts)
    |> collect(cmd)
  end

  ##
  # Converts the list of operations into insert/update/delete commands
  #
  defp get_cmds(:insert, coll, docs, max_batch_size, opts), do: get_insert_cmds(coll, docs, max_batch_size, opts)
  defp get_cmds(:update, coll, docs, max_batch_size, opts), do: get_update_cmds(coll, docs, max_batch_size, opts)
  defp get_cmds(:delete, coll, docs, max_batch_size, opts), do: get_delete_cmds(coll, docs, max_batch_size, opts)

  ###
  # Converts the list of operations into list of lists with same operations.
  #
  # [inserts, inserts, updates] -> [[inserts, inserts],[updates]]
  #
  defp get_op_sequence(ops) do
    get_op_sequence(ops, [])
  end

  defp get_op_sequence([], acc), do: acc

  defp get_op_sequence(ops, acc) do
    [{kind, _doc} | _rest] = ops
    {docs, rest} = find_max_sequence(kind, ops)
    get_op_sequence(rest, [{kind, docs} | acc])
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

  ##
  # collects the returns values for each operation
  #
  # the update operation is more complex than insert or delete operation
  #
  defp collect({docs, ids}, :insert) do
    docs
    |> Enum.map(fn
      {:ok, %{"n" => n} = doc} -> BulkWriteResult.insert_result(n, ids, doc["writeErrors"] || [])
      {:ok, _other} -> BulkWriteResult.empty()
      {:error, reason} -> BulkWriteResult.error(reason)
    end)
    |> BulkWriteResult.reduce()
  end

  defp collect(docs, :update) do
    docs
    |> Enum.map(fn
      {:ok, %{"n" => n, "nModified" => modified, "upserted" => ids} = doc} ->
        l = length(ids)
        BulkWriteResult.update_result(n - l, modified, l, filter_upsert_ids(ids), doc["writeErrors"] || [])

      {:ok, %{"n" => matched, "nModified" => modified} = doc} ->
        BulkWriteResult.update_result(matched, modified, 0, [], doc["writeErrors"] || [])

      {:ok, _other} ->
        BulkWriteResult.empty()

      {:error, reason} ->
        BulkWriteResult.error(reason)
    end)
    |> BulkWriteResult.reduce()
  end

  defp collect(docs, :delete) do
    docs
    |> Enum.map(fn
      {:ok, %{"n" => n} = doc} -> BulkWriteResult.delete_result(n, doc["writeErrors"] || [])
      {:ok, _other} -> BulkWriteResult.empty()
      {:error, reason} -> BulkWriteResult.error(reason)
    end)
    |> BulkWriteResult.reduce()
  end

  defp filter_upsert_ids([_ | _] = upserted), do: Enum.map(upserted, fn doc -> doc["_id"] end)
  defp filter_upsert_ids(_), do: []

  defp run_commands(session, {cmds, ids}, opts) do
    {Enum.map(cmds, fn cmd -> Mongo.exec_command_session(session, cmd, opts) end), ids}
  end

  defp run_commands(session, cmds, opts) do
    Enum.map(cmds, fn cmd -> Mongo.exec_command_session(session, cmd, opts) end)
  end

  defp get_insert_cmds(coll, docs, max_batch_size, opts) do
    {ids, docs} = assign_ids(docs)

    cmds =
      docs
      |> Enum.chunk_every(max_batch_size)
      |> Enum.map(fn inserts -> get_insert_cmd(coll, inserts, opts) end)

    {cmds, ids}
  end

  defp get_insert_cmd(coll, inserts, opts) do
    [insert: coll, documents: inserts, writeConcern: write_concern(opts)] |> filter_nils()
  end

  defp get_delete_cmds(coll, docs, max_batch_size, opts) do
    docs
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn deletes -> get_delete_cmd(coll, deletes, opts) end)
  end

  defp get_delete_cmd(coll, deletes, opts) do
    [delete: coll, deletes: Enum.map(deletes, fn delete -> get_delete_doc(delete) end), ordered: Keyword.get(opts, :ordered), writeConcern: write_concern(opts)] |> filter_nils()
  end

  defp get_delete_doc({filter, opts}) do
    [q: filter, limit: Keyword.get(opts, :limit), collation: Keyword.get(opts, :collation)] |> filter_nils()
  end

  defp get_update_cmds(coll, docs, max_batch_size, opts) do
    docs
    |> Enum.chunk_every(max_batch_size)
    |> Enum.map(fn updates -> get_update_cmd(coll, updates, opts) end)
  end

  defp get_update_cmd(coll, updates, opts) do
    [update: coll, updates: Enum.map(updates, fn update -> get_update_doc(update) end), ordered: Keyword.get(opts, :ordered), writeConcern: write_concern(opts), bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)]
    |> filter_nils()
  end

  defp get_update_doc({filter, update, update_opts}) do
    [q: filter, u: update, upsert: Keyword.get(update_opts, :upsert), multi: Keyword.get(update_opts, :multi) || false, collation: Keyword.get(update_opts, :collation), arrayFilters: Keyword.get(update_opts, :array_filters)] |> filter_nils()
  end
end
