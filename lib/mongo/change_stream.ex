defmodule Mongo.ChangeStream do

  alias Mongo.Session
  alias Mongo.Error

  import Record, only: [defrecordp: 2]

  defstruct [:topology_pid, :session, :doc, :cmd, :on_resume_token, :opts]

  def new(topology_pid, cmd, on_resume_token_fun, opts) do

    ## check, if retryable reads are enabled
    opts = Mongo.retryable_reads(opts)

    with {:ok, session} <- Session.start_implicit_session(topology_pid, :read, opts),
         {:ok, %{"ok" => ok} = doc} when ok == 1 <- Mongo.exec_command_session(session, cmd, opts) do
      %Mongo.ChangeStream{
        topology_pid: topology_pid,
        session: session,
        doc: doc,
        on_resume_token: on_resume_token_fun,
        cmd: cmd,
        opts: opts
      }
    else
      {:error, error} ->
        case Error.should_retry_read(error, cmd, opts) do
          true -> new(topology_pid, cmd, on_resume_token_fun, Keyword.put(opts, :read_counter, 2))
          false -> {:error, error}
        end
    end
  end

  defimpl Enumerable do

    defrecordp :change_stream, [:resume_token, :op_time, :cmd, :on_resume_token]
    defrecordp :state, [:topology_pid, :session, :cursor, :coll, :change_stream, :docs]

    def reduce(change_stream, acc, reduce_fun) do

      start_fun = fn ->
        with {:ok, state} <- aggregate(change_stream.topology_pid, change_stream.session, change_stream.doc, change_stream.cmd, change_stream.on_resume_token) do
          state
          end
      end
      next_fun  = next_fun(change_stream.opts)
      after_fun = after_fun(change_stream.opts)

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp next_fun(opts) do
      fn
        state(docs: [], cursor: 0) = state ->  {:halt, state}

        state(docs: [], topology_pid: topology_pid, session: session, cursor: cursor, change_stream: change_stream, coll: coll) = state ->
          case get_more(topology_pid, session, only_coll(coll), cursor, change_stream, opts) do
            {:ok, %{cursor_id: cursor_id,
              docs: docs,
              change_stream: change_stream}} -> {docs, state(state, cursor: cursor_id, change_stream: change_stream)}
            {:resume, state(docs: docs) = state}   -> {docs, state(state, docs: [])}
            {:error, error}                        -> raise error
          end

        state(docs: docs) = state -> {docs, state(state, docs: [])}
        {:error, error}           -> raise error  ## In case of an error, we should raise the error
      end
    end

    def aggregate(topology_pid, cmd, fun, opts) do

      with {:ok, session} <- Session.start_implicit_session(topology_pid, :read, opts),
           {:ok, %{"ok" => ok} = doc} when ok == 1 <- Mongo.exec_command_session(session, cmd, opts)  do

        aggregate(topology_pid, session, doc, cmd, fun)
      else
        {:error, error} ->
          case Error.should_retry_read(error, cmd, opts) do
            true -> aggregate(topology_pid, cmd, fun, Keyword.put(opts, :read_counter, 2))
            false -> {:error, error}
          end
      end
    end

    def aggregate(topology_pid, session, doc, cmd, fun) do

      with %{"operationTime" => op_time,
             "cursor" => %{
               "id" => cursor_id,
               "ns" => coll,
               "firstBatch" => docs} = response} <- doc,
           {:ok, wire_version} <- Mongo.wire_version(topology_pid) do

        [%{"$changeStream" => stream_opts} | _pipeline] = Keyword.get(cmd, :pipeline) # extract the change stream options

        # The ChangeStream MUST save the operationTime from the initial aggregate response when the following critera are met:
        #
        # None of startAtOperationTime, resumeAfter, startAfter were specified in the ChangeStreamOptions.
        # The max wire version is >= 7.
        # The initial aggregate response had no results.
        # The initial aggregate response did not include a postBatchResumeToken.

        has_values = stream_opts["startAtOperationTime"] || stream_opts["startAfter"] || stream_opts["resumeAfter"]
        op_time    = update_operation_time(op_time, has_values, docs, response["postBatchResumeToken"], wire_version)

        # When the ChangeStream is started:
        # If startAfter is set, cache it.
        # Else if resumeAfter is set, cache it.
        # Else, resumeToken remains unset.
        resume_token = stream_opts["startAfter"] || stream_opts["resumeAfter"]
        resume_token = update_resume_token(resume_token, response["postBatchResumeToken"], List.last(docs))

        fun.(resume_token)

        change_stream = change_stream(resume_token: resume_token, op_time: op_time, cmd: cmd, on_resume_token: fun)

        {:ok, state(topology_pid: topology_pid, session: session, cursor: cursor_id, coll: coll, change_stream: change_stream, docs: docs)}
      end
    end



    @doc """
      Calls the GetCore-Command
      See https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
    """
    def get_more(topology_pid, session, coll, cursor_id,
          change_stream(resume_token: resume_token, op_time: op_time, cmd: aggregate_cmd,
            on_resume_token: fun) = change_stream, opts) do

      get_more = [
                   getMore: %BSON.LongNumber{value: cursor_id},
                   collection: coll,
                   batchSize: opts[:batch_size],
                   maxTimeMS: opts[:max_time]
                 ] |> filter_nils()

      with {:ok, %{"operationTime" => op_time,
                   "cursor" => %{"id" => new_cursor_id, "nextBatch" => docs} = cursor,
                   "ok" => ok}} when ok == 1 <- Mongo.exec_command_session(session, get_more, opts) do

        old_token = change_stream(change_stream, :resume_token)
        change_stream = update_change_stream(change_stream, cursor["postBatchResumeToken"], op_time, List.last(docs))
        new_token = change_stream(change_stream, :resume_token)

        case token_changes(old_token, new_token) do
          true  -> fun.(new_token)
          false -> nil
        end

        {:ok, %{cursor_id: new_cursor_id, docs: docs, change_stream: change_stream}}

      else
        {:error, %Mongo.Error{resumable: false} = not_resumable} -> {:error, not_resumable}
        {:error, _error} ->

          with {:ok, wire_version} <- Mongo.wire_version(topology_pid) do

            [%{"$changeStream" => stream_opts} | pipeline] = Keyword.get(aggregate_cmd, :pipeline) # extract the change stream options

            stream_opts   = update_stream_options(stream_opts, resume_token, op_time, wire_version)
            aggregate_cmd = Keyword.update!(aggregate_cmd, :pipeline, fn _ -> [%{"$changeStream" => stream_opts} | pipeline] end)

            # kill the cursor
            kill_cursors(session, coll, [cursor_id], opts)

            # Start aggregation again...
            with {:ok, state} <- aggregate(topology_pid, aggregate_cmd, fun, opts) do
              {:resume, state}
            end
          end

      end
    end

    defp token_changes(nil, nil), do: false
    defp token_changes(nil, _new_token), do: true
    defp token_changes(_old_token, nil), do: true
    defp token_changes(old_token, new_token), do: not(Map.equal?(old_token, new_token))

    ##
    # we are updating the resume token by matching different cases
    #
    defp update_change_stream(change_stream, nil, nil, nil) do
      change_stream
    end
    defp update_change_stream(change_stream, nil, op_time, nil) do
      change_stream(change_stream, op_time: op_time)
    end
    defp update_change_stream(change_stream, nil, op_time, doc) do
      change_stream(change_stream, op_time: op_time, resume_token: doc["_id"])
    end
    defp update_change_stream(change_stream, post_batch_resume_token, op_time, doc) do
      resume_token = post_batch_resume_token || doc["_id"]
      change_stream(change_stream, op_time: op_time, resume_token: resume_token)
    end

    ##
    # see https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#updating-the-cached-resume-token
    #
    defp update_resume_token(token, nil, nil) do
      token
    end
    defp update_resume_token(_token, postBatchResumeToken, nil) do
      postBatchResumeToken
    end
    defp update_resume_token(_token, nil, last) do
      last["_id"]
    end
    defp update_resume_token(_token, postBatchResumeToken, _last) do
      postBatchResumeToken
    end

    # The ChangeStream MUST save the operationTime from the initial aggregate response when the following critera are met:
    #
    # None of startAtOperationTime, resumeAfter, startAfter were specified in the ChangeStreamOptions.
    # The max wire version is >= 7.
    # The initial aggregate response had no results.
    # The initial aggregate response did not include a postBatchResumeToken.
    defp update_operation_time(op_time, nil, [], nil, wire_version) when wire_version >= 7 do
      op_time
    end
    defp update_operation_time(_op_time, _opts, _doc, _postBatchResumeToken, _wire_version) do
      nil
    end

    @doc"""
    This is the Resume Process described here: https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resume-process
    """
    def update_stream_options(stream_opts, nil, nil, _wire_version) do
      Map.drop(stream_opts, ~w(resumeAfter startAfter startAtOperationTime)a)
    end
    def update_stream_options(stream_opts, nil, _op_time, wire_version) when wire_version < 7 do
      Map.drop(stream_opts, ~w(resumeAfter startAfter startAtOperationTime)a)
    end
    def update_stream_options(stream_opts, nil, op_time, _wire_version) do
      stream_opts
      |> Map.drop(~w(resumeAfter startAfter)a)
      |> Map.put(:startAtOperationTime, op_time)
    end
    def update_stream_options(stream_opts, resume_token, _op_time, _wire_version) do
      stream_opts
      |> Map.drop(~w(startAfter startAtOperationTime)a)
      |> Map.put(:resumeAfter, resume_token)
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