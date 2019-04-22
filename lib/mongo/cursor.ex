defmodule Mongo.Cursor do
  @moduledoc false

  import Record, only: [defrecordp: 2]

  @type t :: %__MODULE__{
    conn: Mongo.conn,
    coll: Mongo.collection,
    query: BSON.document,
    opts: Keyword.t
  }

  defstruct [:conn, :coll, :query, :opts]

  defimpl Enumerable do

    defrecordp :state, [:conn, :cursor, :coll, :buffer]

    def reduce(%{conn: conn, coll: coll, query: query, opts: opts}, acc, reduce_fun) do
      start_fun = start_fun(conn, coll, query, opts)
      next_fun  = next_fun(opts)
      after_fun = after_fun(opts)

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp start_fun(conn, coll, query, opts) do
      opts = Keyword.put(opts, :batch_size, -1)

      fn ->
        case Mongo.direct_command(conn, query, opts) do
          {:ok, %{"ok" => ok,
                  "cursor" => %{
                    "id" => cursor,
                    "ns" => coll,
                    "firstBatch" => docs}}} when ok == 1 -> state(conn: conn, cursor: cursor, coll: coll, buffer: docs)
          {:error, error}                                -> raise error
        end
      end
    end

    defp next_fun(opts) do
      fn
        state(buffer: [], cursor: 0) = state ->  {:halt, state}

        state(buffer: [], conn: conn, cursor: cursor, coll: coll) = state ->
          case get_more(conn, only_coll(coll), cursor, opts) do
            {:ok, %{cursor_id: cursor, docs: []}}   -> {:halt, state(state, cursor: cursor)}
            {:ok, %{cursor_id: cursor, docs: docs}} -> {docs, state(state, cursor: cursor)}
            {:error, error}                         -> raise error
          end

        state(buffer: buffer) = state -> {buffer, state(state, buffer: [])}
      end
    end

    @doc """
      Calls the GetCore-Command
      See https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
    """
    def get_more(conn, coll, cursor, opts) do

      query = [
        {"getMore", cursor},
        {"collection", coll},
        {"batchSize", opts[:batch_size]},
        {"maxTimeMS", opts[:max_time]}
      ]

      query = filter_nils(query)

      with {:ok, %{"cursor" => %{ "id" => cursor_id, "nextBatch" => docs}, "ok" => ok}} when ok == 1 <- Mongo.direct_command(conn, query, opts) do
        {:ok, %{cursor_id: cursor_id, docs: docs}}
      end

    end

    @doc """
      Calls the KillCursors-Command
      See https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
    """
    def kill_cursors(conn, coll, cursor_ids, opts) do

      query = [
        {"killCursors", coll},
        {"cursors", cursor_ids}
      ]

      query = filter_nils(query)

      with {:ok, %{"cursorsAlive" => [],
                   "cursorsNotFound" => [],
                   "cursorsUnknown" => [],
                   "ok" => ok}} when ok == 1 <- Mongo.direct_command(conn, query, opts) do
        :ok
      end
    end

    defp filter_nils(keyword) when is_list(keyword) do
      Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
    end

    defp after_fun(opts) do
      fn
        state(cursor: 0)                              -> :ok
        state(cursor: cursor, coll: coll, conn: conn) -> kill_cursors(conn, only_coll(coll), [cursor], opts)
      end
    end

    defp only_coll(coll) do
      [_db, coll] = String.split(coll, ".", parts: 2)
      coll
    end

    # we cannot determinstically slice, so tell Enumerable to
    # fall back on brute force
    def slice(_cursor), do: { :error, __MODULE__ }
    def count(_stream), do: {:error, __MODULE__}
    def member?(_stream, _term), do: {:error, __MODULE__}
  end
end
