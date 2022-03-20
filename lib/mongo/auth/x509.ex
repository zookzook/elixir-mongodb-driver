defmodule Mongo.Auth.X509 do
  @moduledoc false
  alias Mongo.MongoDBConnection.Utils

  def auth({username, _password}, _db, s) do
    cmd = [authenticate: 1, user: username, mechanism: "MONGODB-X509"]

    case Utils.command(-2, cmd, s) do
      {:ok, _flags, _message} -> :ok
      _error -> {:error, "X509 auth failed"}
    end
  end
end
