defmodule Mongo.Auth do
  @moduledoc false

  alias Mongo.PasswordSafe

  def run(opts, state) do
    db = opts[:database]
    auth = setup(opts)
    auther = mechanism(state)
    auth_source = opts[:auth_source]
    wire_version = state[:wire_version]

    # change database for auth
    state =
      case auth_source != nil && wire_version > 0 do
        true -> Map.put(state, :database, auth_source)
        false -> state
      end

    # do auth
    # restore old database
    Enum.find_value(auth, fn credentials ->
      case auther.auth(credentials, db, state) do
        # everything is okay, then return nil
        :ok ->
          nil

        error ->
          {mod, socket} = state.connection
          mod.close(socket)
          error
      end
    end) || {:ok, Map.put(state, :database, opts[:database])}
  end

  defp setup(opts) do
    username = opts[:username]
    pw_safe = opts[:pw_safe]
    password = PasswordSafe.get_pasword(pw_safe)
    auth = opts[:auth] || []

    auth =
      Enum.map(auth, fn opts ->
        username = opts[:username]
        password = PasswordSafe.get_pasword(pw_safe)
        {username, password}
      end)

    if username && password, do: auth ++ [{username, password}], else: auth
  end

  defp mechanism(%{wire_version: version, auth_mechanism: :x509}) when version >= 3, do: Mongo.Auth.X509
  defp mechanism(%{wire_version: version}) when version >= 3, do: Mongo.Auth.SCRAM
  defp mechanism(_), do: Mongo.Auth.CR
end
