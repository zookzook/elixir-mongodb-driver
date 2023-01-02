defmodule Mongo.MongoDBConnection.Utils do
  @moduledoc false

  require Logger

  import Kernel, except: [send: 2]
  import Mongo.Messages

  # @reply_cursor_not_found   0x1
  # @reply_query_failure      0x2
  # currently not used @reply_shard_config_stale 0x4
  # currently not used @reply_await_capable      0x8

  @header_size 4 * 4

  @doc """
    Sends a request id and waits for the response with the same id
  """
  def post_request(op, id, state) do
    with :ok <- send_data(encode(id, op), state),
         {:ok, ^id, response} <- recv_data(nil, "", state) do
      get_doc(response)
    end
  end

  @doc """
  In case of the flag :exhaust_allowed is used, the driver should read further data from the server.
  """
  def get_response(_id, state) do
    with {:ok, _new_id, response} <- recv_data(nil, "", state) do
      get_doc(response)
    end
  end

  @doc """
    Invoking a command using connection stored in state, that means within a DBConnection call. Therefore
    we cannot call DBConnect.execute() to reuse the command function in Monto.exec_command()

    Using op_msg structure to invoke the command
  """
  def command(id, command, %{use_op_msg: true} = state) do
    # In case of authenticate sometimes the namespace has to be modified
    # If using X509 we need to add the keyword $external to use the external database for the client certificates
    db =
      case Keyword.get(command, :mechanism) == "MONGODB-X509" && Keyword.get(command, :authenticate) == 1 do
        true -> "$external"
        false -> state.database
      end

    command = command ++ ["$db": db]

    request = op_msg(flags: 0, sections: [section(payload_type: 0, payload: payload(doc: command))])

    post_request(request, id, state)
  end

  def command(id, command, state) do
    # In case of authenticate sometimes the namespace has to be modified
    # If using X509 we need to add the keyword $external to use the external database for the client certificates
    ns =
      case Keyword.get(command, :mechanism) == "MONGODB-X509" && Keyword.get(command, :authenticate) == 1 do
        true -> namespace("$cmd", nil, "$external")
        false -> namespace("$cmd", state, nil)
      end

    request = op_query(coll: ns, query: command, select: "", num_skip: 0, num_return: 1, flags: [])
    post_request(request, id, state)
  end

  def get_doc(op_reply() = response) do
    case response do
      op_reply(docs: []) -> {:ok, 0x0, nil}
      op_reply(docs: [doc]) -> {:ok, 0x0, doc}
      op_reply(docs: docs) -> {:ok, 0x0, docs}
    end
  end

  def get_doc(op_msg(flags: flags, sections: sections)) do
    case Enum.map(sections, fn sec -> get_doc(sec) end) do
      [] -> {:ok, flags, nil}
      [doc] -> {:ok, flags, doc}
      docs -> {:ok, flags, List.flatten(docs)}
    end
  end

  def get_doc(section(payload_type: 0, payload: payload(doc: doc))), do: doc
  def get_doc(section(payload_type: 1, payload: payload(sequence: sequence(docs: docs)))), do: docs
  def get_doc(_), do: {:ok, nil}

  @doc """
    This function sends the raw data to the mongodb server
  """
  def send_data(data, %{connection: {mod, socket}} = s) do
    case mod.send(socket, data) do
      :ok -> :ok
      {:error, reason} -> send_error(reason, s)
    end
  end

  defp recv_data(nil, "", %{connection: {mod, socket}} = state) do
    case mod.recv(socket, @header_size, state.timeout) do
      {:ok, tail} -> recv_data(nil, tail, state)
      {:error, reason} -> recv_error(reason, state)
    end
  end

  defp recv_data(nil, data, %{connection: {mod, socket}} = state) do
    case decode_header(data) do
      {:ok, header} ->
        msg_header(length: length) = header

        case mod.recv(socket, length, state.timeout) do
          {:ok, data} -> recv_data(header, data, state)
          {:error, reason} -> recv_error(reason, state)
        end

      :error ->
        recv_error(:header_not_found, state)
    end
  end

  defp recv_data(header, data, %{connection: {mod, socket}} = state) do
    case decode_response(header, data) do
      {:ok, id, reply, ""} ->
        {:ok, id, reply}

      :error ->
        case mod.recv(socket, 0, state.timeout) do
          {:ok, tail} -> recv_data(header, [data | tail], state)
          {:error, reason} -> recv_error(reason, state)
        end
    end
  end

  defp send_error(reason, state) do
    error = Mongo.Error.exception(tag: :tcp, action: "send", reason: reason, host: state.host)
    {:disconnect, error, state}
  end

  defp recv_error(reason, state) do
    error = Mongo.Error.exception(tag: :tcp, action: "recv", reason: reason, host: state.host)
    {:disconnect, error, state}
  end

  def namespace(coll, state, nil), do: [state.database, ?. | coll]
  def namespace(coll, _, database), do: [database, ?. | coll]

  def digest(nonce, username, password) do
    :md5
    |> :crypto.hash([nonce, username, digest_password(username, password, :sha)])
    |> Base.encode16(case: :lower)
  end

  def digest_password(username, password, :sha) do
    :md5
    |> :crypto.hash([username, ":mongo:", password])
    |> Base.encode16(case: :lower)
  end

  def digest_password(_username, password, :sha256) do
    password
  end

  def hostname_port(opts) do
    port = opts[:port] || 27_017

    case Keyword.fetch(opts, :socket) do
      {:ok, socket} ->
        {{:local, socket}, 0}

      :error ->
        case Keyword.fetch(opts, :socket_dir) do
          {:ok, dir} -> {{:local, "#{dir}/mongodb-#{port}.sock"}, 0}
          :error -> {(opts[:hostname] || "localhost") |> to_charlist, port}
        end
    end
  end
end
