defmodule Mongo.Error do
  alias Mongo.Events

  defexception [:message, :code, :host, :fail_command, :error_labels, :resumable, :retryable_reads, :retryable_writes, :not_writable_primary_or_recovering]

  @exceeded_time_limit 262
  @failed_to_satisfy_read_preference 133
  @host_not_found 7
  @host_unreachable 6
  @interrupted_at_shutdown 11_600
  @interrupted_due_to_repl_state_change 11_602
  @network_timeout 89
  @not_primary_no_secondary_ok 13_435
  @not_primary_or_secondary 13_436
  @not_writable_primary 10_107
  @primary_stepped_down 189
  @retry_change_stream 234
  @shutdown_in_progress 91
  @socket_exception 9001
  @stale_config 13_388
  @stale_epoch 150
  @stale_shard_version 63

  @retryable_writes [
    @exceeded_time_limit,
    @host_not_found,
    @host_unreachable,
    @interrupted_at_shutdown,
    @interrupted_due_to_repl_state_change,
    @network_timeout,
    @not_primary_no_secondary_ok,
    @not_primary_or_secondary,
    @not_writable_primary,
    @primary_stepped_down,
    @shutdown_in_progress,
    @socket_exception
  ]

  @retryable_reads [
    @host_not_found,
    @host_unreachable,
    @interrupted_due_to_repl_state_change,
    @network_timeout,
    @not_primary_no_secondary_ok,
    @not_primary_or_secondary,
    @not_writable_primary,
    @primary_stepped_down,
    @socket_exception,
    @interrupted_at_shutdown
  ]

  @resumable [
    @exceeded_time_limit,
    @interrupted_due_to_repl_state_change,
    @stale_epoch,
    @failed_to_satisfy_read_preference,
    @host_not_found,
    @host_unreachable,
    @interrupted_at_shutdown,
    @interrupted_at_shutdown,
    @network_timeout,
    @not_primary_no_secondary_ok,
    @not_primary_or_secondary,
    @not_writable_primary,
    @primary_stepped_down,
    @retry_change_stream,
    @shutdown_in_progress,
    @socket_exception,
    @stale_config,
    @stale_shard_version
  ]

  # https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-writable-primary-and-node-is-recovering
  @not_writable_primary_or_recovering [
    @interrupted_at_shutdown,
    @interrupted_due_to_repl_state_change,
    @not_primary_no_secondary_ok,
    @not_primary_or_secondary,
    @not_writable_primary,
    @primary_stepped_down,
    @shutdown_in_progress
  ]

  @type t :: %__MODULE__{
          message: String.t(),
          code: number,
          host: String.t(),
          error_labels: [String.t()] | nil,
          fail_command: boolean,
          resumable: boolean,
          retryable_reads: boolean,
          retryable_writes: boolean,
          not_writable_primary_or_recovering: boolean
        }

  def message(e) do
    code = if e.code, do: " #{e.code}"
    "#{e.message}#{code}"
  end

  def exception(tag: :tcp, action: action, reason: reason, host: host) do
    formatted_reason = :inet.format_error(reason)
    %Mongo.Error{message: "#{host} tcp #{action}: #{formatted_reason} - #{inspect(reason)}", host: host, resumable: true}
  end

  def exception(tag: :ssl, action: action, reason: reason, host: host) do
    formatted_reason = :ssl.format_error(reason)
    %Mongo.Error{message: "#{host} ssl #{action}: #{formatted_reason} - #{inspect(reason)}", host: host, resumable: false}
  end

  def exception(%{"code" => code, "errmsg" => msg} = doc) do
    error_labels = doc["errorLabels"] || []
    resumable = Enum.any?(@resumable, &(&1 == code)) || Enum.any?(error_labels, &(&1 == "ResumableChangeStreamError"))
    retryable_reads = Enum.any?(@retryable_reads, &(&1 == code)) || Enum.any?(error_labels, &(&1 == "RetryableReadError"))
    retryable_writes = Enum.any?(@retryable_writes, &(&1 == code)) || Enum.any?(error_labels, &(&1 == "RetryableWriteError"))
    not_writable_primary_or_recovering = Enum.any?(@not_writable_primary_or_recovering, &(&1 == code))

    %Mongo.Error{
      message: msg,
      code: code,
      fail_command: String.contains?(msg, "failCommand") || String.contains?(msg, "failpoint"),
      error_labels: error_labels,
      resumable: resumable,
      retryable_reads: retryable_reads,
      retryable_writes: retryable_writes,
      not_writable_primary_or_recovering: not_writable_primary_or_recovering
    }
  end

  def exception(message: message, code: code) do
    %Mongo.Error{message: message, code: code, resumable: Enum.any?(@resumable, &(&1 == code))}
  end

  def exception(message) do
    %Mongo.Error{message: message, resumable: false}
  end

  @doc """
  Return true if the error is retryable for read operations.
  """
  def should_retry_read(%Mongo.Error{retryable_reads: true}, cmd, opts) do
    [{command_name, _} | _] = cmd

    result = command_name != :getMore and opts[:read_counter] == 1

    if result do
      Events.notify(%Mongo.Events.RetryReadEvent{command_name: command_name, command: cmd}, :commands)
    end

    result
  end

  def should_retry_read(_error, _cmd, _opts) do
    false
  end

  @doc """
  Return true if the error is retryable for writes operations.
  """
  def should_retry_write(%Mongo.Error{retryable_writes: true}, cmd, opts) do
    [{command_name, _} | _] = cmd

    result = opts[:write_counter] == 1

    if result do
      Events.notify(%Mongo.Events.RetryWriteEvent{command_name: command_name, command: cmd}, :commands)
    end

    result
  end

  def should_retry_write(_error, _cmd, _opts) do
    false
  end

  def has_label(%Mongo.Error{error_labels: labels}, label) when is_list(labels) do
    Enum.any?(labels, fn l -> l == label end)
  end

  def has_label(_other, _label) do
    false
  end

  def not_writable_primary?(%Mongo.Error{code: code}) do
    code == @not_writable_primary
  end

  def not_primary_no_secondary_ok?(%Mongo.Error{code: code}) do
    code == @not_primary_no_secondary_ok
  end

  def not_primary_or_secondary?(%Mongo.Error{code: code}) do
    code == @not_primary_or_secondary
  end

  @doc """
  Return true if the error == not writable primary or in recovering mode.
  """
  def not_writable_primary_or_recovering?(%Mongo.Error{not_writable_primary_or_recovering: result}, opts) do
    ## no explicit session, no retry counter but not_writable_primary_or_recovering
    Keyword.get(opts, :session, nil) == nil && Keyword.get(opts, :retry_counter, nil) == nil && result
  end

  # catch all function
  def not_writable_primary_or_recovering?(_other, _opts) do
    false
  end

  @doc """
  Returns true if the error is issued by the failCommand
  """
  def fail_command?(%Mongo.Error{fail_command: fail_command}) do
    fail_command
  end
end

defmodule Mongo.WriteError do
  defexception [:n, :ok, :write_errors]

  def message(e) do
    "n: #{e.n}, ok: #{e.ok}, write_errors: #{inspect(e.write_errors)}"
  end
end
