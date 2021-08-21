defmodule Mongo.Stream do

  alias Mongo.Session
  alias Mongo.Error

  import Record, only: [defrecordp: 2]

  defstruct [:topology_pid, :session, :cursor, :coll, :docs, :cmd, :opts]

  alias Mongo.Session

  def new(topology_pid, cmd, opts) do

    ## check, if retryable reads are enabled
    opts = Mongo.retryable_reads(opts)

    with {:ok, session} <- Session.start_implicit_session(topology_pid, :read, opts),
         {:ok,
           %{"ok" => ok,
             "cursor" => %{
               "id" => cursor_id,
               "ns" => coll,
               "firstBatch" => docs}}} when ok == 1 <- Mongo.exec_command_session(session, cmd, opts) do

      %Mongo.Stream{topology_pid: topology_pid, session: session, cursor: cursor_id, coll: coll, docs: docs, cmd: cmd, opts: opts}
    else
      {:error, error} ->
        case Error.should_retry_read(error, cmd, opts) do
          true -> new(topology_pid, cmd, Keyword.put(opts, :read_counter, 2))
          false -> {:error, error}
        end
      other -> {:error, Mongo.Error.exception("Unknow result #{inspect other} while calling Session.start_implicit_session/3")}
    end
  end

  defimpl Enumerable do

    defrecordp :state, [:topology_pid, :session, :cursor, :coll, :cmd, :docs]

    def reduce(%Mongo.Stream{topology_pid: topology_pid, session: session, cursor: cursor_id, coll: coll, docs: docs, cmd: cmd, opts: opts}, acc, reduce_fun) do

      start_fun = fn -> state(topology_pid: topology_pid, session: session, cursor: cursor_id, coll: coll, cmd: cmd, docs: docs) end
      next_fun  = next_fun(opts)
      after_fun = after_fun(opts)

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp next_fun(opts) do
      fn
        state(docs: [], cursor: 0) = state ->  {:halt, state}

        # this is a regular cursor
        state(docs: [], topology_pid: topology_pid, session: session, cursor: cursor, coll: coll, cmd: cmd) = state ->
          case get_more(topology_pid, session, only_coll(coll), cursor, nil, opts) do
            {:ok, %{cursor_id: cursor_id, docs: []}}   -> {if(cmd[:tailable], do: [], else: :halt), state(state, cursor: cursor_id)}
            {:ok, %{cursor_id: cursor_id, docs: docs}} -> {docs, state(state, cursor: cursor_id)}
            {:error, error}                            -> raise error
          end

        state(docs: docs) = state -> {docs, state(state, docs: [])}
        {:error, error}           -> raise error  ## In case of an error, we should raise the error
      end
    end

    @doc """
      Calls the GetCore-Command
      See https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
    """
    def get_more(_topology_pid, session, coll, cursor, nil, opts) do

      cmd = [
              getMore: %BSON.LongNumber{value: cursor},
              collection: coll,
              batchSize: opts[:batch_size],
              maxTimeMS: opts[:max_time]
            ] |> filter_nils()

      with {:ok, %{"cursor" => %{ "id" => cursor_id, "nextBatch" => docs}, "ok" => ok}} when ok == 1 <- Mongo.exec_command_session(session, cmd, opts) do
        {:ok, %{cursor_id: cursor_id, docs: docs}}
      end

    end


    @doc"""
      Calls the KillCursors-Command
      See https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
    """
    def kill_cursors(session, coll, cursor_ids, opts) do

      cmd = [
              killCursors: coll,
              cursors: cursor_ids |> Enum.map(fn id -> %BSON.LongNumber{value: id} end)
            ] |> filter_nils()

      with {:ok, %{"cursorsAlive" => [],
        "cursorsNotFound" => [],
        "cursorsUnknown" => [],
        "ok" => ok}} when ok == 1 <- Mongo.exec_command_session(session, cmd, opts) do
        :ok
      end
    end

    defp filter_nils(keyword) when is_list(keyword) do
      Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
    end

    defp after_fun(opts) do
      fn
        state(topology_pid: topology_pid, session: session, cursor: 0) -> Session.end_implict_session(topology_pid, session)
        state(topology_pid: topology_pid, session: session, cursor: cursor, coll: coll) ->
          with :ok <- kill_cursors(session, only_coll(coll), [cursor], opts) do
            Session.end_implict_session(topology_pid, session)
          end
        error -> error
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
