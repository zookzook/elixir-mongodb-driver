defmodule Mongo.Auth.SCRAM do
  @moduledoc false
  import Mongo.BinaryUtils
  import Bitwise

  alias Mongo.MongoDBConnection.Utils

  def auth({username, password}, db, s) do
    {mechanism, digest} = select_digest(db, username, s)
    nonce = nonce()
    first_bare = first_bare(username, nonce)
    payload = first_message(first_bare)
    message = [saslStart: 1, mechanism: mechanism, payload: payload]

    result =
      with {:ok, _flags, %{"ok" => ok} = reply} when ok == 1 <- Utils.command(-3, message, s),
           {message, signature} = first(reply, first_bare, username, password, nonce, digest),
           {:ok, _flags, %{"ok" => ok} = reply} when ok == 1 <- Utils.command(-4, message, s),
           message = second(reply, signature),
           {:ok, _flags, %{"ok" => ok} = reply} when ok == 1 <- Utils.command(-5, message, s),
           do: final(reply)

    case result do
      :ok ->
        :ok

      {:ok, _flags, %{"ok" => z, "errmsg" => reason, "code" => code}} when z == 0 ->
        {:error, Mongo.Error.exception(message: "auth failed for user #{username}: #{reason}", code: code)}

      error ->
        error
    end
  end

  defp first(%{"conversationId" => conversation_id, "payload" => server_payload, "done" => false}, first_bare, username, password, client_nonce, digest) do
    params = parse_payload(server_payload)
    server_nonce = params["r"]
    salt = params["s"] |> Base.decode64!()
    iter = params["i"] |> String.to_integer()
    pass = Utils.digest_password(username, password, digest)
    salted_password = hi(pass, salt, iter, digest)

    <<^client_nonce::binary(24), _::binary>> = server_nonce

    client_message = "c=biws,r=#{server_nonce}"
    auth_message = "#{first_bare},#{server_payload.binary},#{client_message}"
    server_signature = generate_signature(salted_password, auth_message, digest)
    proof = generate_proof(salted_password, auth_message, digest)
    client_final_message = %BSON.Binary{binary: "#{client_message},#{proof}"}
    message = [saslContinue: 1, conversationId: conversation_id, payload: client_final_message]

    {message, server_signature}
  end

  defp second(%{"conversationId" => conversation_id, "payload" => payload}, signature) do
    params = parse_payload(payload)
    ^signature = params["v"] |> Base.decode64!()
    [saslContinue: 1, conversationId: conversation_id, payload: %BSON.Binary{binary: ""}]
  end

  defp final(%{"conversationId" => _, "payload" => %BSON.Binary{binary: ""}, "done" => true}) do
    :ok
  end

  defp first_message(first_bare) do
    %BSON.Binary{binary: "n,,#{first_bare}"}
  end

  defp first_bare(username, nonce) do
    "n=#{encode_username(username)},r=#{nonce}"
  end

  defp hi(password, salt, iterations, digest) do
    Mongo.PBKDF2Cache.pbkdf2(password, salt, iterations, digest)
  end

  ## support for OTP 22.x
  if Code.ensure_loaded?(:crypto) and function_exported?(:crypto, :hmac, 3) do
    defp generate_proof(salted_password, auth_message, digest) do
      client_key = :crypto.hmac(digest, salted_password, "Client Key")
      stored_key = :crypto.hash(digest, client_key)
      signature = :crypto.hmac(digest, stored_key, auth_message)
      client_proof = xor_keys(client_key, signature, "")
      "p=#{Base.encode64(client_proof)}"
    end

    defp generate_signature(salted_password, auth_message, digest) do
      server_key = :crypto.hmac(digest, salted_password, "Server Key")
      :crypto.hmac(digest, server_key, auth_message)
    end
  else
    defp generate_proof(salted_password, auth_message, digest) do
      client_key = :crypto.mac(:hmac, digest, salted_password, "Client Key")
      stored_key = :crypto.hash(digest, client_key)
      signature = :crypto.mac(:hmac, digest, stored_key, auth_message)
      client_proof = xor_keys(client_key, signature, "")
      "p=#{Base.encode64(client_proof)}"
    end

    defp generate_signature(salted_password, auth_message, digest) do
      server_key = :crypto.mac(:hmac, digest, salted_password, "Server Key")
      :crypto.mac(:hmac, digest, server_key, auth_message)
    end
  end

  defp xor_keys("", "", result), do: result
  defp xor_keys(<<fa, ra::binary>>, <<fb, rb::binary>>, result), do: xor_keys(ra, rb, <<result::binary, bxor(fa, fb)>>)

  defp nonce do
    :crypto.strong_rand_bytes(18) |> Base.encode64()
  end

  defp encode_username(username) do
    username
    |> String.replace("=", "=3D")
    |> String.replace(",", "=2C")
  end

  defp parse_payload(%BSON.Binary{subtype: :generic, binary: payload}) do
    payload
    |> String.split(",")
    |> Enum.into(%{}, &List.to_tuple(String.split(&1, "=", parts: 2)))
  end

  ##
  # selects the supported sasl mechanism
  # It calls isMaster with saslSupportedMechs option to ask for the selected user which mechanism is supported
  #
  defp select_digest(database, username, state) do
    ### todo
    with {:ok, _flags, doc} <- Utils.command(-2, [hello: 1, saslSupportedMechs: database <> "." <> username], state) do
      select_digest(doc)
    end
  end

  defp select_digest(%{"saslSupportedMechs" => mechs}) do
    case Enum.member?(mechs, "SCRAM-SHA-256") do
      true -> {"SCRAM-SHA-256", :sha256}
      false -> {"SCRAM-SHA-1", :sha}
    end
  end

  defp select_digest(_), do: {"SCRAM-SHA-1", :sha}
end
