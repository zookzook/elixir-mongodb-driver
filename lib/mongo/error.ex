defmodule Mongo.Error do

  alias Mongo.Events

  defexception [:message, :code, :host, :error_labels, :resumable, :retryable_reads, :retryable_writes]

  @host_unreachable                     6
  @host_not_found                       7
  @network_timeout                      89
  @shutdown_in_progress                 91
  @primary_stepped_down                 189
  @exceeded_time_limit                  262
  @socket_exception                     9001
  @not_master                           10107
  @interrupted_at_shutdown              11600
  @interrupted_due_to_repl_state_change 11602
  @not_master_no_slaveok                13435
  @not_master_or_secondary              13436
  @stale_shard_version                  63
  @stale_epoch                          150
  @stale_config                         13388
  @retry_change_stream                  234
  @failed_to_satisfy_read_preference    133

  @retryable_writes [@interrupted_at_shutdown, @interrupted_due_to_repl_state_change, @not_master, @not_master_no_slaveok,
                     @not_master_or_secondary, @primary_stepped_down, @shutdown_in_progress, @host_not_found,
                     @host_unreachable, @network_timeout, @socket_exception, @exceeded_time_limit ]

  @retryable_reads [@interrupted_at_shutdown, @interrupted_due_to_repl_state_change, @not_master,
                    @not_master_no_slaveok, @not_master_or_secondary, @primary_stepped_down,
                    @host_not_found, @host_unreachable , @network_timeout, @socket_exception]

  @resumable [@host_unreachable, @host_not_found, @network_timeout, @shutdown_in_progress, @primary_stepped_down,
              @exceeded_time_limit, @socket_exception, @not_master, @interrupted_at_shutdown, @interrupted_at_shutdown,
              @interrupted_due_to_repl_state_change, @not_master_no_slaveok, @not_master_or_secondary, @stale_shard_version,
              @stale_epoch, @stale_config, @retry_change_stream, @failed_to_satisfy_read_preference]

  @type t :: %__MODULE__{
    message: String.t,
    code: number,
    host: String.t,
    error_labels: [String.t] | nil,
    resumable: boolean,
    retryable_reads: boolean,
    retryable_writes: boolean
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
    errorLabels     = doc["errorLabels"] || []
    resumable       = Enum.any?(@resumable, &(&1 == code)) || Enum.any?(errorLabels, &(&1 == "ResumableChangeStreamError"))
    retryable_reads = Enum.any?(@retryable_reads, &(&1 == code)) || Enum.any?(errorLabels, &(&1 == "RetryableReadError"))
    retryable_writes = Enum.any?(@retryable_writes, &(&1 == code)) || Enum.any?(errorLabels, &(&1 == "RetryableWriteError"))
    %Mongo.Error{message: msg, code: code, error_labels: errorLabels, resumable: resumable, retryable_reads: retryable_reads, retryable_writes: retryable_writes}
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
    [{command_name,_}|_] = cmd

    result = (command_name != :getMore and opts[:read_counter] == 1)
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
    [{command_name,_}|_] = cmd

    result = opts[:write_counter] == 1
    if result do
      Events.notify(%Mongo.Events.RetryWriteEvent{command_name: command_name, command: cmd}, :commands)
    end

    result
  end
  def should_retry_write(_error, _cmd, _opts) do
    false
  end

  def has_label(%Mongo.Error{error_labels: labels}, label) when is_list(labels)do
    Enum.any?(labels, fn l -> l == label end)
  end
  def has_label(_other, _label) do
    false
  end
end

defmodule Mongo.WriteError do
  defexception [:n, :ok, :write_errors]

  def message(e) do
    "n: #{e.n}, ok: #{e.ok}, write_errors: #{inspect e.write_errors}"
  end
end
