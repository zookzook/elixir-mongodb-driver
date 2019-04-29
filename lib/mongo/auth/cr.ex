defmodule Mongo.Auth.CR do
  @moduledoc false
  alias Mongo.MongoDBConnection.Utils

  def auth({username, password}, _db, s) do
    with {:ok, message} <- Utils.command(-2, [getnonce: 1], s),
         do: nonce(message, username, password, s)
  end

  defp nonce(%{"nonce" => nonce, "ok" => ok}, username, password, s)
  when ok == 1 # to support a response that returns 1 or 1.0
  do
    digest = Utils.digest(nonce, username, password)
    command = [authenticate: 1, user: username, nonce: nonce, key: digest]

    case Utils.command(-3, command, s) do
      {:ok, %{"ok" => ok}} when ok == 1 ->
        :ok
      {:ok, %{"ok" => 0.0, "errmsg" => reason, "code" => code}} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}': #{reason}", code: code)}
      {:ok, nil} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}'")}
      error ->
        error
    end
  end
end
