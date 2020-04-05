defmodule Mongo.Error do
  defexception [:message, :code, :host, :error_labels]

  @type t :: %__MODULE__{
    message: String.t,
    code: number,
    host: String.t,
    error_labels: [String.t] | nil
  }

  def message(e) do
    code = if e.code, do: " #{e.code}"
    "#{e.message}#{code}"
  end

  def exception(tag: :tcp, action: action, reason: reason, host: host) do
    formatted_reason = :inet.format_error(reason)
    %Mongo.Error{message: "#{host} tcp #{action}: #{formatted_reason} - #{inspect(reason)}", host: host}
  end

  def exception(tag: :ssl, action: action, reason: reason, host: host) do
    formatted_reason = :ssl.format_error(reason)
    %Mongo.Error{message: "#{host} ssl #{action}: #{formatted_reason} - #{inspect(reason)}", host: host}
  end

  def exception(%{"code" => code, "errmsg" => msg} = doc) do
    %Mongo.Error{message: msg, code: code, error_labels: doc["errorLabels"]}
  end
  def exception(message: message, code: code) do
    %Mongo.Error{message: message, code: code}
  end

  def exception(message) do
    %Mongo.Error{message: message}
  end
end

defmodule Mongo.WriteError do
  defexception [:n, :ok, :write_errors]

  def message(e) do
    "n: #{e.n}, ok: #{e.ok}, write_errors: #{inspect e.write_errors}"
  end
end
