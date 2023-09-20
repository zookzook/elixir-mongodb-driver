defmodule Mongo.Auth do
  @moduledoc false

  alias Mongo.PasswordSafe

  def run(opts, state) do
    auth_source = opts[:auth_source]
    mechanism = mechanism(state)

    # change database for auth
    auth_state =
      case auth_source != nil && state.wire_version > 0 do
        true ->
          Map.put(state, :database, auth_source)

        false ->
          state
      end

    case opts |> credentials() |> mechanism.auth(state.database, auth_state) do
      :ok ->
        {:ok, state}

      error ->
        {mod, socket} = state.connection
        mod.close(socket)
        error
    end
  end

  defp credentials(opts) do
    username = opts[:username]
    pw_safe = opts[:pw_safe]
    password = PasswordSafe.get_password(pw_safe)
    {username, password}
  end

  defp mechanism(%{wire_version: version, auth_mechanism: :x509}) when version >= 3 do
    Mongo.Auth.X509
  end

  defp mechanism(%{wire_version: version, auth_mechanism: :plain}) when version >= 3 do
    Mongo.Auth.PLAIN
  end

  defp mechanism(%{wire_version: version}) when version >= 3 do
    Mongo.Auth.SCRAM
  end

  defp mechanism(_) do
    Mongo.Auth.CR
  end
end
