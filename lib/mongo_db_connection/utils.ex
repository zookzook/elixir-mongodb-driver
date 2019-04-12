defmodule Mongo.MongoDBConnection.Utils do
  @moduledoc false
  import Kernel, except: [send: 2]
  import Mongo.Messages

  @doc"""
    Sends a request id and waits for the response with the same id
  """
  def post_request(id, op, state) do
    with :ok <- send_data(encode(id, op), state),
         {:ok, ^id, response} <- recv_data(nil, "", state),
         do: {:ok, response}
  end

  @doc """
    Invoking a command using connection stored in state, that means within a DBConnection call. Therefore
    we cannot call DBConnect.execute() to reuse the command function in Monto.direct_command()

    Using op_query structure to invoke the command
  """
  def command(id, command, state) do

    # In case of authenticate sometimes the namespace has to be modified
    # If using X509 we need to add the keyword $external to use the external database for the client certificates
    ns = case Keyword.get(command, :mechanism) == "MONGODB-X509" && Keyword.get(command, :authenticate) == 1 do
      true  -> namespace("$cmd", nil, "$external")
      false -> namespace("$cmd", state, nil)
    end

    op = op_query(coll: ns, query: command, select: "", num_skip: 0, num_return: 1, flags: [])

    case post_request(id, op, state) do
      {:ok, op_reply(docs: docs)} ->
        case docs do
          []    -> {:ok, nil}
          [doc] -> {:ok, doc}
        end
      {:disconnect, _, _} = error ->  error
    end
  end

  @doc """
    This function sends the raw data to the mongodb server
  """
  def send_data(data, %{socket: {mod, sock}} = s) do
    case mod.send(sock, data) do
      :ok              -> :ok
      {:error, reason} -> send_error(reason, s)
    end
  end

  defp recv_data(nil, "", %{socket: {mod, sock}} = state) do
    case mod.recv(sock, 0, state.timeout) do
      {:ok, tail}      -> recv_data(nil, tail, state)
      {:error, reason} -> recv_error(reason, state)
    end
  end
  defp recv_data(nil, data, %{socket: {mod, sock}} = state) do
    case decode_header(data) do
      {:ok, header, rest} -> recv_data(header, rest, state)
      :error ->
        case mod.recv(sock, 0, state.timeout) do
          {:ok, tail}      -> recv_data(nil, [data|tail], state)
          {:error, reason} -> recv_error(reason, state)
        end
    end
  end
  defp recv_data(header, data, %{socket: {mod, sock}} = state) do
    case decode_response(header, data) do
      {:ok, id, reply, ""} -> {:ok, id, reply}
      :error ->
        case mod.recv(sock, 0, state.timeout) do
          {:ok, tail}      -> recv_data(header, [data|tail], state)
          {:error, reason} -> recv_error(reason, state)
        end
    end
  end

  defp send_error(reason, s) do
    error = Mongo.Error.exception(tag: :tcp, action: "send", reason: reason)
    {:disconnect, error, s}
  end

  defp recv_error(reason, s) do
    error = Mongo.Error.exception(tag: :tcp, action: "recv", reason: reason)
    {:disconnect, error, s}
  end

  def namespace(coll, state, nil), do: [state.database, ?. | coll]
  def namespace(coll, _, database), do: [database, ?. | coll]

  def digest(nonce, username, password) do
    :crypto.hash(:md5, [nonce, username, digest_password(username, password)])
    |> Base.encode16(case: :lower)
  end

  def digest_password(username, password) do
    :crypto.hash(:md5, [username, ":mongo:", password])
    |> Base.encode16(case: :lower)
  end
end
