defmodule Mongo.Auth.CR do
  @moduledoc false
  alias Mongo.MongoDBConnection.Utils

  def auth({nil, nil}, _db, _s) do
    :ok
  end

  def auth({username, password}, _db, s) do
    with {:ok, _flags, message} <- Utils.command(-2, [getnonce: 1], s),
         do: nonce(message, username, password, s)
  end

  # Note that we use numeric comparisons in guards (e.g., `... when ok == 1`)
  # instead of pattern matching below. This is to accommodate responses that
  # return either integer or float values. Pattern matching treats 1 and 1.0,
  # and 0, 0.0 and -0.0 (OTP 27+), as distinct values due to their different
  # types/internal representation. By using numeric comparisons, we can ensure
  # correct behavior regardless of the numeric type returned.
  defp nonce(%{"nonce" => nonce, "ok" => ok}, username, password, s) when ok == 1 do
    digest = Utils.digest(nonce, username, password)
    command = [authenticate: 1, user: username, nonce: nonce, key: digest]

    case Utils.command(-3, command, s) do
      {:ok, _flags, %{"ok" => ok}} when ok == 1 ->
        :ok

      {:ok, _flags, %{"ok" => ok, "errmsg" => reason, "code" => code}} when ok == 0 ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}': #{reason}", code: code)}

      {:ok, _flags, nil} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}'")}

      error ->
        error
    end
  end
end
