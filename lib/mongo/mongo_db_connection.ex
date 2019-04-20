defmodule Mongo.MongoDBConnection do
  @moduledoc """
  Implementierung für das DBConnection-Protokoll.
  """

  use DBConnection
  use Mongo.Messages
  alias Mongo.MongoDBConnection.Utils

  @timeout        5000
  @find_one_flags ~w(slave_ok exhaust partial)a
  @write_concern  ~w(w j wtimeout)a

  @impl true
  def connect(opts) do
    {write_concern, opts} = Keyword.split(opts, @write_concern)
    write_concern = Keyword.put_new(write_concern, :w, 1)

    state = %{
      connection: nil,
      request_id: 0,
      timeout: opts[:timeout] || @timeout,
      connect_timeout_ms: opts[:connect_timeout_ms] || @timeout,
      database: Keyword.fetch!(opts, :database),
      write_concern: Map.new(write_concern),
      wire_version: nil,
      auth_mechanism: opts[:auth_mechanism] || nil,
      connection_type: Keyword.fetch!(opts, :connection_type),
      topology_pid: Keyword.fetch!(opts, :topology_pid),
      ssl: opts[:ssl] || false
    }

    connect(opts, state)
  end

  @impl true
  def disconnect(_error, %{connection: {mod, socket}} = state) do
    notify_disconnect(state)
    mod.close(socket)
  end

  defp notify_disconnect(%{connection_type: type, topology_pid: pid, host: host}) do
    GenServer.cast(pid, {:disconnect, type, host})
  end

  defp connect(opts, state) do

    result =
      with {:ok, state} <- tcp_connect(opts, state),
           {:ok, state} <- maybe_ssl(opts, state),
           {:ok, state} <- wire_version(state),
           {:ok, state} <- maybe_auth(opts, state) do
        {:ok, state}
      end

    case result do
      {:ok, state} ->
        ## IO.puts inspect state
        {:ok, state}

      {:disconnect, reason, state} ->
        reason = case reason do
          {:tcp_recv, reason} -> Mongo.Error.exception(tag: :tcp, action: "recv", reason: reason, host: state.host)
          {:tcp_send, reason} -> Mongo.Error.exception(tag: :tcp, action: "send", reason: reason, host: state.host)
          %Mongo.Error{} = reason -> reason
        end
        {mod, socket} = state.connection
        mod.close(socket)
        {:error, reason}

      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_auth(opts, state) do
    case opts[:skip_auth] do
      true -> {:ok, state}
      _    -> Mongo.Auth.run(opts, state)
    end
  end

  defp maybe_ssl(opts, %{ssl: true} = state), do: ssl(opts, state)
  defp maybe_ssl(_opts, state), do: {:ok, state}

  defp ssl(opts, %{connection: {:gen_tcp, socket}} = state) do
    host     = (opts[:hostname] || "localhost") |> to_charlist
    ssl_opts = Keyword.put_new(opts[:ssl_opts] || [], :server_name_indication, host)
    case :ssl.connect(socket, ssl_opts, state.connect_timeout_ms) do
      {:ok, ssl_sock}  -> {:ok, %{state | connection: {:ssl, ssl_sock}}}
      {:error, reason} ->
        :gen_tcp.close(socket)
        {:error, Mongo.Error.exception(tag: :ssl, action: "connect", reason: reason, host: state.host)}
    end
  end

  defp tcp_connect(opts, s) do
    host      = (opts[:hostname] || "localhost") |> to_charlist
    port      = opts[:port] || 27017
    sock_opts = [:binary, active: false, packet: :raw, nodelay: true]
                ++ (opts[:socket_options] || [])

    s = Map.put(s, :host, "#{host}:#{port}")

    case :gen_tcp.connect(host, port, sock_opts, s.connect_timeout_ms) do
      {:ok, socket} ->
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} = :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])
        buffer = buffer |> max(sndbuf) |> max(recbuf)
        :ok = :inet.setopts(socket, buffer: buffer)

        {:ok, %{s | connection: {:gen_tcp, socket}}}

      {:error, reason} -> {:error, Mongo.Error.exception(tag: :tcp, action: "connect", reason: reason, host: s.host)}
    end
  end

  defp wire_version(state) do
    # wire version
    # https://github.com/mongodb/mongo/blob/master/src/mongo/db/wire_version.h
    case Utils.command(-1, [ismaster: 1], state) do
      {:ok, %{"ok" => ok, "maxWireVersion" => version}} when ok == 1 ->  {:ok, %{state | wire_version: version}}
      {:ok, %{"ok" => ok}} when ok == 1 ->  {:ok, %{state | wire_version: 0}}
      {:ok, %{"ok" => ok, "errmsg" => msg, "code" => code}} when ok == 0 ->
        err = Mongo.Error.exception(message: msg, code: code)
        {:disconnect, err, state}
      {:disconnect, _, _} = error ->   error
    end
  end

  def checkout(state), do: {:ok, state}
  @impl true
  def checkin(state), do: {:ok, state}

  @impl true
  def handle_begin(_opts, state), do: {:ok, nil, state}
  @impl true
  def handle_close(_query, _opts, state), do: {:ok, nil, state}
  @impl true
  def handle_commit(_opts, state), do: {:ok, nil, state}
  @impl true
  def handle_deallocate(_query, _cursor, _opts, state), do:  {:ok, nil, state}
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
  def ping(%{wire_version: wire_version} = state) do
    with {:ok, %{wire_version: ^wire_version}} <- wire_version(state), do: {:ok, state}
  end

  def handle_execute_close(query, params, opts, s) do
    handle_execute(query, params, opts, s)
  end

  @impl true
  def handle_execute(%Mongo.Query{action: action} = query, params, opts, original_state) do
    tmp_state = %{original_state | database: Keyword.get(opts, :database, original_state.database)}
    with {:ok, reply, tmp_state} <- execute_action(action, params, opts, tmp_state) do
      {:ok, query, reply, Map.put(tmp_state, :database, original_state.database)}
    end
  end

  defp execute_action(:wire_version, _, _, state) do
    {:ok, state.wire_version, state}
  end

  defp execute_action(:command, [query], opts, state) do
    flags = Keyword.take(opts, @find_one_flags)
    op = op_query(coll: Utils.namespace("$cmd", state, opts[:database]), query: query, select: "", num_skip: 0, num_return: 1, flags: flags(flags))
    with {:ok, response} <- Utils.post_request(state.request_id, op, state),
         state = %{state | request_id: state.request_id + 1},
         do: {:ok, response, state}
  end

  defp flags(flags) do
    Enum.reduce(flags, [], fn
      {flag, true},   acc -> [flag|acc]
      {_flag, false}, acc -> acc
    end)
  end


end
