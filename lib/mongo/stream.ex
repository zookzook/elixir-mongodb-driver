defmodule Mongo.Stream do
  @moduledoc false

  alias Mongo.Session
  alias Mongo.Error

  defstruct [
    :topology_pid,
    :session,
    :cursor,
    :coll,
    :docs,
    :cmd,
    :opts
  ]

  def new(topology_pid, cmd, opts) do
    ## check, if retryable reads are enabled
    opts = Mongo.retryable_reads(opts)

    with {type, session} <- checkout_session(topology_pid, opts) do
      case Mongo.exec_command_session(session, cmd, opts) do
        {:ok, %{"ok" => ok, "cursor" => cursor}} when ok == 1 ->
          %Mongo.Stream{topology_pid: topology_pid, session: {type, session}, cursor: cursor["id"], coll: cursor["ns"], docs: cursor["firstBatch"], cmd: cmd, opts: Keyword.put(opts, :session, session)}

        {:error, error} ->
          checkin_session(type, session, topology_pid)

          case Error.should_retry_read(error, cmd, opts) do
            true -> new(topology_pid, cmd, Keyword.put(opts, :read_counter, 2))
            false -> {:error, error}
          end
      end
    end
  end

  defp checkout_session(topology_pid, opts) do
    case Mongo.get_session(opts) do
      nil ->
        with {:ok, session} <- Session.start_session(topology_pid, :read, opts) do
          {:own, session}
        end

      session ->
        {:borrowed, session}
    end
  end

  def checkin_session(:own, session, topology_pid) do
    Session.end_session(topology_pid, session)
  end

  def checkin_session(:borrowed, _session, _topology_pid) do
  end

  defimpl Enumerable do
    def reduce(%Mongo.Stream{} = stream, acc, reduce_fun) do
      start_fun = fn -> stream end
      Stream.resource(start_fun, next_fun(), after_fun()).(acc, reduce_fun)
    end

    def slice(_cursor), do: {:error, __MODULE__}
    def count(_stream), do: {:error, __MODULE__}
    def member?(_stream, _term), do: {:error, __MODULE__}

    defp next_fun() do
      fn
        %Mongo.Stream{docs: [], cursor: 0} = stream -> {:halt, stream}
        %Mongo.Stream{docs: []} = stream -> get_more(stream)
        %Mongo.Stream{docs: docs} = stream -> {docs, %{stream | docs: []}}
        {:error, error} -> raise error
      end
    end

    defp after_fun() do
      fn
        %Mongo.Stream{topology_pid: topology_pid, session: {type, session}, cursor: 0} ->
          Mongo.Stream.checkin_session(type, session, topology_pid)

        %Mongo.Stream{topology_pid: topology_pid, session: {type, session}} = stream ->
          with :ok <- kill_cursors(stream) do
            Mongo.Stream.checkin_session(type, session, topology_pid)
          end

        error ->
          error
      end
    end

    defp get_more(%{session: {_type, session}} = stream) do
      cmd =
        [
          getMore: %BSON.LongNumber{value: stream.cursor},
          collection: only_coll(stream.coll),
          batchSize: stream.opts[:batch_size],
          maxTimeMS: stream.opts[:max_time]
        ]
        |> filter_nils()

      case Mongo.exec_command_session(session, cmd, stream.opts) do
        {:ok, %{"cursor" => %{"id" => cursor_id, "nextBatch" => []}, "ok" => ok}} when ok == 1 ->
          {cmd_tailable?(stream), %{stream | cursor: cursor_id}}

        {:ok, %{"cursor" => %{"id" => cursor_id, "nextBatch" => docs}, "ok" => ok}} when ok == 1 ->
          {docs, %{stream | cursor: cursor_id}}

        error ->
          raise error
      end
    end

    defp cmd_tailable?(%{cmd: cmd}) do
      case cmd[:tailable] == true do
        true -> []
        false -> :halt
      end
    end

    defp cmd_tailable?(_other) do
      :halt
    end

    defp kill_cursors(%{session: {_type, session}} = stream) do
      cmd = [
        killCursors: only_coll(stream.coll),
        cursors: [%BSON.LongNumber{value: stream.cursor}]
      ]

      with {:ok, %{"cursorsAlive" => [], "cursorsNotFound" => [], "cursorsUnknown" => [], "ok" => ok}} when ok == 1 <- Mongo.exec_command_session(session, cmd, stream.opts) do
        :ok
      end
    end

    defp only_coll(coll) do
      [_db, coll] = String.split(coll, ".", parts: 2)
      coll
    end

    defp filter_nils(keyword) when is_list(keyword) do
      Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
    end
  end
end
