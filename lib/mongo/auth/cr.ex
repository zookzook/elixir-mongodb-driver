defmodule Mongo.Auth.CR do
  @moduledoc false
  alias Mongo.MongoDBConnection.Utils

  def auth({username, password}, _db, s) do
    with {:ok, _flags, message} <- Utils.command(-2, [getnonce: 1], s),
         do: nonce(message, username, password, s)
  end

  defp nonce(%{"nonce" => nonce, "ok" => ok}, username, password, s)
       # to support a response that returns 1 or 1.0
       when ok == 1 do
    digest = Utils.digest(nonce, username, password)
    command = [authenticate: 1, user: username, nonce: nonce, key: digest]

    case Utils.command(-3, command, s) do
      {:ok, _flags, %{"ok" => ok}} when ok == 1 ->
        :ok

      {:ok, _flags, %{"ok" => 0.0, "errmsg" => reason, "code" => code}} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}': #{reason}", code: code)}

      {:ok, _flags, nil} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}'")}

      error ->
        error
    end
  end
end
