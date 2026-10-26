defmodule Mongo.Stream do
  @moduledoc """
    This module provides a stream implementation that automatically checks out a session when the stream is started and an Enumerable
    protocol that checks it back in when the stream has been consumed.
  """

  require Logger

  alias Mongo.Session
  alias Mongo.Error

  defstruct [
    :topology_pid,
    :session,
    :cursor,
    :coll,
    :docs,
    :cmd,
    :opts,
    # {getMore command, duration} of the batch in `docs` while it is decoded, see get_more/1.
    :batch_log
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
        %Mongo.Stream{docs: %BSON.LazyDocuments{} = docs} = stream -> take(docs, stream)
        %Mongo.Stream{docs: {:error, _error} = error} -> raise error
        %Mongo.Stream{docs: docs} = stream -> {docs, %{stream | docs: []}}
        {:error, error} -> raise error
      end
    end

    defp after_fun() do
      fn
        %Mongo.Stream{topology_pid: topology_pid, session: {type, session}, cursor: 0} = stream ->
          log_batch(stream)
          Mongo.Stream.checkin_session(type, session, topology_pid)

        %Mongo.Stream{topology_pid: topology_pid, session: {type, session}} = stream ->
          log_batch(stream)

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

      # getMore batches stay undecoded (BSON.LazyDocuments) until they are consumed. Such a
      # getMore is logged once its batch is decoded, with the decoding time added to its
      # duration, as when it was decoded on arrival.
      case Mongo.exec_get_more(session, cmd, stream.opts) do
        {:ok, %{"cursor" => %{"id" => cursor_id, "nextBatch" => []}, "ok" => ok}, _duration} when ok == 1 ->
          {cmd_tailable?(stream), %{stream | cursor: cursor_id}}

        {:ok, %{"cursor" => %{"id" => cursor_id, "nextBatch" => %BSON.LazyDocuments{} = docs}, "ok" => ok}, duration} when ok == 1 ->
          {[], %{stream | cursor: cursor_id, docs: docs, batch_log: {cmd, duration}}}

        {:ok, %{"cursor" => %{"id" => cursor_id, "nextBatch" => docs}, "ok" => ok}, _duration} when ok == 1 ->
          {docs, %{stream | cursor: cursor_id}}

        error ->
          raise error
      end
    end

    # Decoding a whole batch (up to 16 MB) at once keeps every document alive on the heap, and
    # the garbage collector then spends most of the time copying them. It is decoded a chunk
    # at a time instead.
    @chunk_size 100

    defp take(docs, %{batch_log: {cmd, duration}} = stream) do
      {decoding, {docs, rest}} = :timer.tc(fn -> BSON.LazyDocuments.take(docs, @chunk_size) end)
      stream = %{stream | docs: rest, batch_log: {cmd, duration + decoding}}

      case rest do
        [] -> {docs, log_batch(stream)}
        _rest -> {docs, stream}
      end
    rescue
      # Fails like a getMore whose reply cannot be decoded (see Mongo.MongoDBConnection.Utils),
      # which is not logged.
      error ->
        Logger.error("Unable to send request to database because of #{inspect(error)}")
        {[], %{stream | docs: {:error, error}, batch_log: nil}}
    end

    defp log_batch(%{batch_log: {cmd, duration}} = stream) do
      Mongo.log_command(cmd, duration, stream.opts)
      %{stream | batch_log: nil}
    end

    defp log_batch(stream), do: stream

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
