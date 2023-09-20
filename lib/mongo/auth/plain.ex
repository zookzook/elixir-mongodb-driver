defmodule Mongo.Auth.PLAIN do
  @moduledoc false
  alias Mongo.MongoDBConnection.Utils

  def auth({nil, nil}, _db, _s) do
    :ok
  end

  def auth({username, password}, _db, s) do
    auth_payload = build_auth_payload(username, password)
    message = [saslStart: 1, mechanism: "PLAIN", payload: auth_payload]

    case Utils.command(-3, message, s) do
      {:ok, _flags, %{"ok" => ok, "done" => true}} when ok == 1 ->
        :ok

      {:ok, _flags, %{"ok" => ok, "errmsg" => reason, "code" => code}} when ok == 0 ->
        {:error, Mongo.Error.exception(message: "auth failed for user #{username}: #{reason}", code: code)}

      error ->
        error
    end
  end

  defp build_auth_payload(username, password) do
    # https://www.ietf.org/rfc/rfc4616.txt
    # Null separate listed of authorization ID (blank), username, password. These are sent as raw UTF-8.
    payload = "\0#{username}\0#{password}"
    %BSON.Binary{binary: payload}
  end
end
