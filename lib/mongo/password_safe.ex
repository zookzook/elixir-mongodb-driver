defmodule Mongo.PasswordSafe do
  @moduledoc """
  The password safe stores the password while parsing the url and/or the options to avoid it from logging while the sasl logger is activated.

  The password is encrypted before storing in the GenServer's state. It will be encrypted before returning. This should help, that the password
  is not stored as plain text in the memory.
  """

  @me __MODULE__

  use GenServer

  def new() do
    GenServer.start_link(@me, [])
  end

  def set_password(pid, password) do
    GenServer.cast(pid, {:set, password})
  end

  def get_pasword(nil), do: nil

  def get_pasword(pid) do
    GenServer.call(pid, :get)
  end

  def init([]) do
    {:ok, %{key: generate_key(), pw: nil}}
  end

  def handle_cast({:set, password}, %{key: key} = data) do
    {:noreply, %{data | pw: password |> encrypt(key)}}
  end

  def handle_call(:get, _from, %{key: key, pw: password} = data) do
    {:reply, password |> decrypt(key), data}
  end

  if String.to_integer(System.otp_release()) < 22 do
    @aad "AES256GCM"

    defp encrypt(plaintext, key) do
      # create random Initialisation Vector
      iv = :crypto.strong_rand_bytes(16)
      {ciphertext, tag} = :crypto.block_encrypt(:aes_gcm, key, iv, {@aad, to_string(plaintext), 16})
      # "return" iv with the cipher tag & ciphertext
      iv <> tag <> ciphertext
    end

    defp decrypt(ciphertext, key) do
      <<iv::binary-16, tag::binary-16, ciphertext::binary>> = ciphertext
      :crypto.block_decrypt(:aes_gcm, key, iv, {@aad, ciphertext, tag})
    end
  else
    defp encrypt(plaintext, key) do
      # create random Initialisation Vector
      iv = :crypto.strong_rand_bytes(16)
      ciphertext = :crypto.crypto_one_time(:aes_256_ctr, key, iv, plaintext, true)
      # "return" iv & ciphertext
      iv <> ciphertext
    end

    defp decrypt(ciphertext, key) do
      <<iv::binary-16, ciphertext::binary>> = ciphertext
      :crypto.crypto_one_time(:aes_256_ctr, key, iv, ciphertext, false)
    end
  end

  defp generate_key() do
    :crypto.strong_rand_bytes(32)
  end
end
