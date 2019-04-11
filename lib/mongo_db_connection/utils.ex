defmodule Mongo.MongoDBConnection.Utils do
  @moduledoc false
  import Kernel, except: [send: 2]
  import Mongo.Messages

  @doc"""
    Sends a request id and waits for the response with the same id
  """
  def post_request(id, ops, state) when is_list(ops) do
    with :ok <- send(ops, state),
         {:ok, ^id, response} <- recv(state),
         do: {:ok, response}
  end
  def post_request(id, op, state) do
    with :ok <- send(id, op, state),
         {:ok, ^id, response} <- recv(state),
         do: {:ok, response}
  end

  @doc"""
    Sends a request id and waits for the response with the same id
  """
  def post_msg(id, ops, state) when is_list(ops) do
    with :ok <- send(ops, state),
         {:ok, ^id, response} <- recv_msg(state),
         do: {:ok, response}
  end
  def post_msg(id, op, state) do
    with :ok <- send(id, op, state),
         {:ok, ^id, response} <- recv_msg(state),
         do: {:ok, response}
  end

  def command(id, command, s) do
    ns =
      if Keyword.get(command, :mechanism) == "MONGODB-X509" && Keyword.get(command, :authenticate) == 1 do
        namespace("$cmd", nil, "$external")
      else
        namespace("$cmd", s, nil)
    end

    op = op_query(coll: ns, query: BSON.Encoder.document(command), select: "", num_skip: 0, num_return: 1, flags: [])

    case post_request(id, op, s) do
      {:ok, op_reply(docs: docs)} ->
        case BSON.Decoder.documents(docs) do
          []    -> {:ok, nil}
          [doc] -> {:ok, doc}
        end
      {:disconnect, _, _} = error ->
        error
    end
  end

  def send(id, op, %{socket: {mod, sock}} = s) do
    case mod.send(sock, encode(id, op)) do
      :ok              -> :ok
      {:error, reason} -> send_error(reason, s)
    end
  end

  # Performance regressions of a factor of 1000x have been observed on
  # linux systems for write operations that do not include the getLastError
  # command in the same call to :gen_tcp.send/2 so we hide the workaround
  # for mongosniff behind a flag
  if Mix.env in [:dev, :test] && System.get_env("MONGO_NO_BATCH_SEND") do
    def send(ops, %{socket: {mod, sock}} = s) do
      # Do a separate :gen_tcp.send/2 for each message because mongosniff
      # cannot handle more than one message per packet. TCP is a stream
      # protocol, but no.
      # https://jira.mongodb.org/browse/TOOLS-821
      Enum.find_value(List.wrap(ops), fn {id, op} ->
        data = encode(id, op)
        case mod.send(sock, data) do
          :ok              -> nil
          {:error, reason} -> send_error(reason, s)
        end
      end)
      || :ok
    end
  else
    def send(ops, %{socket: {mod, sock}} = s) do
      data =
        Enum.reduce(List.wrap(ops), "", fn {id, op}, acc ->
          [acc|encode(id, op)]
        end)

      case mod.send(sock, data) do
        :ok              -> :ok
        {:error, reason} -> send_error(reason, s)
      end
    end
  end

  def recv(s) do
    recv(nil, "", s)
  end

  def recv_msg(s) do
    recv_msg(nil, "", s)
  end
  defp recv_msg(nil, data, %{socket: {mod, sock}} = s) do
    case decode_header(data) do
      {:ok, header, rest} -> recv_msg(header, rest, s)
      :error ->
        case mod.recv(sock, 0, s.timeout) do
          {:ok, tail}      -> recv_msg(nil, [data|tail], s)
          {:error, reason} -> recv_error(reason, s)
        end
    end
  end
  defp recv_msg(header, data, %{socket: {mod, sock}} = s) do
    case decode_response_msg(header, data) do
      {:ok, id, msg, ""} -> {:ok, id, msg}
      :error ->
        case mod.recv(sock, 0, s.timeout) do
          {:ok, tail}      -> recv_msg(header, [data|tail], s)
          {:error, reason} -> recv_error(reason, s)
        end
    end
  end

  # TODO: Optimize to reduce :gen_tcp.recv and decode_response calls
  #       based on message size in header.
  #       :gen.tcp.recv(socket, min(size, max_packet))
  #       where max_packet = 64mb
  defp recv(nil, data, %{socket: {mod, sock}} = s) do
    case decode_header(data) do
      {:ok, header, rest} ->
        recv(header, rest, s)
      :error ->
        case mod.recv(sock, 0, s.timeout) do
          {:ok, tail}      -> recv(nil, [data|tail], s)
          {:error, reason} -> recv_error(reason, s)
        end
    end
  end
  defp recv(header, data, %{socket: {mod, sock}} = s) do
    case decode_response(header, data) do
      {:ok, id, reply, ""} ->
        {:ok, id, reply}
      :error ->
        case mod.recv(sock, 0, s.timeout) do
          {:ok, tail}      -> recv(header, [data|tail], s)
          {:error, reason} -> recv_error(reason, s)
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

  def namespace(coll, s, nil), do: [s.database, ?. | coll]
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
