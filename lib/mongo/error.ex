defmodule Mongo.Error do
  defexception [:message, :code, :host, :error_labels, :resumable]

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

  @resumable [@host_unreachable, @host_not_found, @network_timeout, @shutdown_in_progress, @primary_stepped_down,
              @exceeded_time_limit, @socket_exception, @not_master, @interrupted_at_shutdown, @interrupted_at_shutdown,
              @interrupted_due_to_repl_state_change, @not_master_no_slaveok, @not_master_or_secondary, @stale_shard_version,
              @stale_epoch, @stale_config, @retry_change_stream, @failed_to_satisfy_read_preference]

  @type t :: %__MODULE__{
    message: String.t,
    code: number,
    host: String.t,
    error_labels: [String.t] | nil,
    resumable: boolean
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
    errorLabels = doc["errorLabels"] || []
    resumable   = Enum.any?(@resumable, &(&1 == code)) || Enum.any?(errorLabels, &(&1 == "ResumableChangeStreamError"))
    %Mongo.Error{message: msg, code: code, error_labels: errorLabels, resumable: resumable}
  end
  def exception(message: message, code: code) do
    %Mongo.Error{message: message, code: code, resumable: Enum.any?(@resumable, &(&1 == code))}
  end

  def exception(message) do
    %Mongo.Error{message: message, resumable: false}
  end
end

defmodule Mongo.WriteError do
  defexception [:n, :ok, :write_errors]

  def message(e) do
    "n: #{e.n}, ok: #{e.ok}, write_errors: #{inspect e.write_errors}"
  end
end
