defmodule Mongo.Auth do
  @moduledoc false

  def run(opts, state) do

    db           = opts[:database]
    auth         = setup(opts)
    auther       = mechanism(state)
    auth_source  = opts[:auth_source]
    wire_version = state[:wire_version]

    # change database for auth
    state = case auth_source != nil && wire_version > 0 do
      true  -> Map.put(state, :database, auth_source)
      false -> state
    end

    # do auth
    Enum.find_value(auth, fn credentials ->
      case auther.auth(credentials, db, state) do
        :ok -> nil    # everything is okay, then return nil
        error ->
          {mod, socket} = state.connection
          mod.close(socket)
          error
      end
    end) || {:ok, Map.put(state, :database, opts[:database])} # restore old database
  end

  defp setup(opts) do
    username = opts[:username]
    password = opts[:password]
    auth     = opts[:auth] || []

    auth =
      Enum.map(auth, fn opts ->
        username = opts[:username]
        password = opts[:password]
        {username, password}
      end)

    if username && password, do: auth ++ [{username, password}], else: auth
  end

  defp mechanism(%{wire_version: version, auth_mechanism: :x509}) when version >= 3, do: Mongo.Auth.X509
  defp mechanism(%{wire_version: version}) when version >= 3,  do: Mongo.Auth.SCRAM
  defp mechanism(_), do: Mongo.Auth.CR
end
