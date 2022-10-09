defmodule Mongo.MongoDBConnection do
  @moduledoc """
  Implementation of the DBConnection behaviour module.
  """

  require Logger

  use DBConnection
  use Mongo.Messages

  alias Mongo.Events
  alias Mongo.Events.CommandStartedEvent
  alias Mongo.MongoDBConnection.Utils
  alias Mongo.Events.MoreToComeEvent
  alias Mongo.StableVersion

  @timeout 15_000
  @find_one_flags ~w(slave_ok exhaust partial)a
  @write_concern ~w(w j wtimeout)a
  @insecure_cmds [:authenticate, :saslStart, :saslContinue, :getnonce, :createUser, :updateUser, :copydbgetnonce, :copydbsaslstart, :copydb, :isMaster, :ismaster, :hello]

  @impl true
  def connect(opts) do
    {write_concern, opts} = Keyword.split(opts, @write_concern)
    write_concern = Keyword.put_new(write_concern, :w, 1)

    state = %{
      connection: nil,
      request_id: 0,
      timeout: opts[:timeout] || @timeout,
      connect_timeout: opts[:connect_timeout] || @timeout,
      database: Keyword.fetch!(opts, :database),
      write_concern: Map.new(write_concern),
      wire_version: 0,
      auth_mechanism: opts[:auth_mechanism] || nil,
      connection_type: Keyword.fetch!(opts, :connection_type),
      topology_pid: Keyword.fetch!(opts, :topology_pid),
      stable_api: Keyword.get(opts, :stable_api),
      use_op_msg: Keyword.get(opts, :stable_api) != nil,
      hello_ok: Keyword.get(opts, :stable_api) != nil,
      ssl: opts[:ssl] || opts[:tls] || false
    }

    connect(opts, state)
  end

  @impl true
  def disconnect(_error, %{connection: {mod, socket}, connection_type: type, topology_pid: pid, host: host}) do
    GenServer.cast(pid, {:disconnect, type, host})
    mod.close(socket)
    :ok
  end

  @impl true
  def checkout(state), do: {:ok, state}
  @impl true
  def handle_begin(_opts, state), do: {:ok, nil, state}
  @impl true
  def handle_close(_query, _opts, state), do: {:ok, nil, state}
  @impl true
  def handle_commit(_opts, state), do: {:ok, nil, state}
  @impl true
  def handle_deallocate(_query, _cursor, _opts, state), do: {:ok, nil, state}
  @impl true
  def handle_declare(query, _params, _opts, state), do: {:ok, query, nil, state}
  @impl true
  def handle_fetch(_query, _cursor, _opts, state), do: {:halt, nil, state}
  @impl true
  def handle_prepare(query, _opts, state), do: {:ok, query, state}
  @impl true
  def handle_rollback(_opts, state), do: {:ok, nil, state}
  @impl true
  def handle_status(_opts, state), do: {:idle, state}

  @impl true
  def ping(%{connection_type: :client} = state) do
    cmd = [ping: 1]

    case Utils.command(-1, cmd, state) do
      {:ok, _flags, %{"ok" => ok}} when ok == 1 ->
        {:ok, state}

      {:ok, _flags, %{"ok" => ok, "errmsg" => msg, "code" => code}} when ok == 0 ->
        err = Mongo.Error.exception(message: msg, code: code)
        {:disconnect, err, state}

      {:disconnect, _, _} = error ->
        error
    end
  end

  @impl true
  def ping(state) do
    {:ok, state}
  end

  @impl true
  def handle_execute(%Mongo.Query{action: action} = query, _params, opts, original_state) do
    tmp_state = %{original_state | database: Keyword.get(opts, :database, original_state.database)}

    with {:ok, reply, tmp_state} <- send_command(action, opts, tmp_state) do
      {:ok, query, reply, Map.put(tmp_state, :database, original_state.database)}
    end
  end

  defp connect(opts, state) do
    result =
      with {:ok, state} <- tcp_connect(opts, state),
           {:ok, state} <- maybe_ssl(opts, state),
           {:ok, state} <- hand_shake(opts, state) do
        maybe_auth(opts, state)
      end

    case result do
      {:ok, state} ->
        {:ok, state}

      {:disconnect, reason, state} ->
        reason =
          case reason do
            {:tcp_recv, reason} -> Mongo.Error.exception(tag: :tcp, action: "recv", reason: reason, host: state.host)
            {:tcp_send, reason} -> Mongo.Error.exception(tag: :tcp, action: "send", reason: reason, host: state.host)
            %Mongo.Error{} = reason -> reason
          end

        {mod, socket} = state.connection
        mod.close(socket)
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_auth(opts, state) do
    case opts[:skip_auth] do
      true -> {:ok, state}
      _ -> Mongo.Auth.run(opts, state)
    end
  end

  defp maybe_ssl(opts, %{ssl: true} = state), do: ssl(opts, state)
  defp maybe_ssl(_opts, state), do: {:ok, state}

  defp ssl(opts, %{connection: {:gen_tcp, socket}} = state) do
    host = (opts[:hostname] || "localhost") |> to_charlist
    ssl_opts = Keyword.put_new(opts[:ssl_opts] || [], :server_name_indication, host)

    case :ssl.connect(socket, ssl_opts, state.connect_timeout) do
      {:ok, ssl_sock} ->
        {:ok, %{state | connection: {:ssl, ssl_sock}}}

      {:error, reason} ->
        :gen_tcp.close(socket)
        {:error, Mongo.Error.exception(tag: :ssl, action: "connect", reason: reason, host: state.host)}
    end
  end

  defp tcp_connect(opts, s) do
    {host, port} = Utils.hostname_port(opts)
    sock_opts = [:binary, active: false, packet: :raw, nodelay: true] ++ (opts[:socket_options] || [])

    s =
      case host do
        {:local, socket} -> Map.put(s, :host, socket)
        hostname -> Map.put(s, :host, "#{hostname}:#{port}")
      end

    case :gen_tcp.connect(host, port, sock_opts, s.connect_timeout) do
      {:ok, socket} ->
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} = :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

        buffer =
          buffer
          |> max(sndbuf)
          |> max(recbuf)

        :ok = :inet.setopts(socket, buffer: buffer)

        {:ok, %{s | connection: {:gen_tcp, socket}}}

      {:error, reason} ->
        {:error, Mongo.Error.exception(tag: :tcp, action: "connect", reason: reason, host: s.host)}
    end
  end

  defp hand_shake(opts, state) do
    cmd = handshake_command(state, client(opts[:appname] || "elixir-driver"))

    case Utils.command(-1, cmd, state) do
      {:ok, _flags, %{"ok" => ok, "maxWireVersion" => version} = response} when ok == 1 ->
        {:ok, %{state | wire_version: version, use_op_msg: version >= 6, hello_ok: Map.get(response, "helloOk", false)}}

      {:ok, _flags, %{"ok" => ok}} when ok == 1 ->
        {:ok, %{state | wire_version: 0}}

      {:ok, _flags, %{"ok" => ok, "errmsg" => msg, "code" => code}} when ok == 0 ->
        err = Mongo.Error.exception(message: msg, code: code)
        {:disconnect, err, state}

      {:disconnect, _, _} = error ->
        error
    end
  end

  defp client(app_name) do
    driver_version =
      case :application.get_key(:mongodb_driver, :vsn) do
        {:ok, version} -> to_string(version)
        _ -> "??"
      end

    {architecture, name} = get_architecture()

    version =
      case :os.version() do
        {one, two, tree} -> to_string(one) <> "." <> to_string(two) <> "." <> to_string(tree)
        s -> s
      end

    platform = "Elixir (" <> System.version() <> "), Erlang/OTP (" <> to_string(:erlang.system_info(:otp_release)) <> "), ERTS (" <> to_string(:erlang.system_info(:version)) <> ")"

    type = elem(:os.type(), 1)

    %{
      client: %{
        application: %{name: app_name}
      },
      driver: %{
        name: "mongodb_driver",
        version: driver_version
      },
      os: %{
        type: type,
        name: pretty_name(name),
        architecture: architecture,
        version: version
      },
      platform: platform
    }
  end

  defp get_architecture() do
    case String.split(to_string(:erlang.system_info(:system_architecture)), "-") do
      [architecture, name | _rest] -> {architecture, name}
      ["win32"] -> {"win32", "Windows"}
      [one] -> {"??", one}
      [] -> {"??", "??"}
    end
  end

  defp pretty_name("apple"), do: "Mac OS X"
  defp pretty_name(name), do: name

  defp provide_cmd_data([{command_name, _} | _] = cmd) do
    case Enum.member?(@insecure_cmds, command_name) do
      true -> {command_name, %{}}
      false -> {command_name, cmd}
    end
  end

  ##
  # Executes a hello or the legacy hello command
  ##
  defp send_command({:exec_hello, cmd}, opts, %{use_op_msg: true} = state) do
    db = opts[:database] || state.database
    timeout = Keyword.get(opts, :timeout, state.timeout)
    flags = Keyword.get(opts, :flags, 0x0)

    cmd = hello_command(cmd, state) ++ ["$db": db]

    event = %CommandStartedEvent{
      command: :hello,
      command_name: :hello,
      database_name: db,
      request_id: state.request_id,
      operation_id: opts[:operation_id],
      connection_id: self()
    }

    Events.notify(event, :commands)

    op = op_msg(flags: flags, sections: [section(payload_type: 0, payload: payload(doc: cmd))])

    case :timer.tc(fn -> Utils.post_request(op, state.request_id, %{state | timeout: timeout}) end) do
      {duration, {:ok, flags, doc}} ->
        state = %{state | request_id: state.request_id + 1}
        {:ok, {doc, event, flags, duration}, state}

      {_duration, error} ->
        error
    end
  end

  ##
  # Executes a more to come command
  ##
  defp send_command(:more_to_come, opts, %{use_op_msg: true} = state) do
    event = %MoreToComeEvent{command: :more_to_come, command_name: opts[:command_name] || :more_to_come}

    timeout = Keyword.get(opts, :timeout, state.timeout)

    Events.notify(event, :commands)

    case :timer.tc(fn -> Utils.get_response(state.request_id, %{state | timeout: timeout}) end) do
      {duration, {:ok, flags, doc}} ->
        {:ok, {doc, event, flags, duration}, state}

      {_duration, error} ->
        error
    end
  end

  defp send_command({:command, cmd}, opts, %{use_op_msg: true} = state) do
    {command_name, data} = provide_cmd_data(cmd)
    db = opts[:database] || state.database
    cmd = cmd ++ ["$db": db]
    flags = Keyword.get(opts, :flags, 0x0)

    # MongoDB 3.6 only allows certain command arguments to be provided this way. These are:
    op =
      case pulling_out?(cmd, :documents) || pulling_out?(cmd, :updates) || pulling_out?(cmd, :deletes) do
        nil -> op_msg(flags: flags, sections: [section(payload_type: 0, payload: payload(doc: cmd))])
        key -> pulling_out(cmd, flags, key)
      end

    # overwrite temporary timeout by timeout option
    timeout = Keyword.get(opts, :timeout, state.timeout)

    event = %CommandStartedEvent{
      command: data,
      command_name: opts[:command_name] || command_name,
      database_name: db,
      request_id: state.request_id,
      operation_id: opts[:operation_id],
      connection_id: self()
    }

    Events.notify(event, :commands)

    case :timer.tc(fn -> Utils.post_request(op, state.request_id, %{state | timeout: timeout}) end) do
      {duration, {:ok, flags, doc}} ->
        state = %{state | request_id: state.request_id + 1}
        {:ok, {doc, event, flags, duration}, state}

      {_duration, error} ->
        error
    end
  end

  defp send_command({:command, cmd}, opts, state) do
    [{command_name, _} | _] = cmd

    event = %CommandStartedEvent{
      command: cmd,
      command_name: opts[:command_name] || command_name,
      database_name: opts[:database] || state.database,
      request_id: state.request_id,
      operation_id: opts[:operation_id],
      connection_id: self()
    }

    flags = Keyword.take(opts, @find_one_flags)
    op = op_query(coll: Utils.namespace("$cmd", state, opts[:database]), query: cmd, select: "", num_skip: 0, num_return: 1, flags: flags(flags))
    timeout = Keyword.get(opts, :timeout, state.timeout)

    case :timer.tc(fn -> Utils.post_request(op, state.request_id, %{state | timeout: timeout}) end) do
      {duration, {:ok, flags, doc}} ->
        state = %{state | request_id: state.request_id + 1}
        {:ok, {doc, event, flags, duration}, state}

      {_duration, error} ->
        error
    end
  end

  defp send_command(:error, _opts, state) do
    exception = Mongo.Error.exception("Test-case")
    {:disconnect, exception, state}
  end

  defp pulling_out?(cmd, key) do
    case Keyword.has_key?(cmd, key) do
      true -> key
      false -> nil
    end
  end

  defp pulling_out(cmd, flags, key) when is_atom(key) do
    docs = Keyword.get(cmd, key)
    cmd = Keyword.delete(cmd, key)

    payload_0 = section(payload_type: 0, payload: payload(doc: cmd))
    payload_1 = section(payload_type: 1, payload: payload(sequence: sequence(identifier: to_string(key), docs: docs)))

    op_msg(flags: flags, sections: [payload_0, payload_1])
  end

  defp flags(flags) do
    Enum.reduce(flags, [], fn
      {flag, true}, acc -> [flag | acc]
      {_flag, false}, acc -> acc
    end)
  end

  defp handshake_command(%{stable_api: nil}, client) do
    [ismaster: 1, helloOk: true, client: client]
  end

  defp handshake_command(%{stable_api: stable_api}, client) do
    [client: client]
    |> StableVersion.merge_stable_api(stable_api)
    |> Keyword.put(:hello, 1)
  end

  defp hello_command(cmd, %{hello_ok: false}) do
    cmd
    |> Keyword.put(:helloOk, true)
    |> Keyword.put(:ismaster, 1)
  end

  defp hello_command(cmd, %{hello_ok: true, stable_api: stable_api}) do
    cmd
    |> StableVersion.merge_stable_api(stable_api)
    |> Keyword.put(:hello, 1)
  end
end
