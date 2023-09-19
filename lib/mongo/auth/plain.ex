defmodule Mongo.Auth.PLAIN do
  @moduledoc false
  alias Mongo.MongoDBConnection.Utils

  require Logger

  def auth({username, password}, db, s) do
    IO.puts("[PLAIN]: user=#{username} db=#{inspect(db)}")
    auth_payload = build_plain_auth_payload(username, password)
    message = [saslStart: 1, mechanism: "PLAIN", payload: auth_payload]

    result = Utils.command(-3, message, s)
    IO.puts("[PLAIN]: result=#{inspect(result)}")
    case result do
      {:ok, _flags, %{"ok" => ok, "conversationId" => _, "done" => true, "payload" => %BSON.Binary{binary: ""}}} when ok == 1 ->
        :ok

      {:ok, _flags, %{"ok" => ok, "errmsg" => reason, "code" => code}} when ok == 0 ->
        IO.puts("[PLAIN]: error: ok=0, msg=#{reason}, code=#{inspect(code)}")
        {:error, Mongo.Error.exception(message: "auth failed for user #{username}: #{reason}", code: code)}

      error ->
        error
    end
  end

  defp build_plain_auth_payload(username, password) do
    # https://www.ietf.org/rfc/rfc4616.txt
    # Null separate listed of authzorization ID (blank), username, password sent raw.
    payload = "\0#{username}\0#{password}"
    %BSON.Binary{binary: payload}
  end
end
