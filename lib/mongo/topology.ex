defmodule Mongo.Topology do
  @moduledoc false

  require Logger

  use GenServer

  alias Mongo.Events.ServerClosedEvent
  alias Mongo.Events.ServerDescriptionChangedEvent
  alias Mongo.Events.ServerOpeningEvent
  alias Mongo.Events.ServerSelectionEmptyEvent
  alias Mongo.Events.TopologyClosedEvent
  alias Mongo.Events.TopologyDescriptionChangedEvent
  alias Mongo.Events.TopologyOpeningEvent
  alias Mongo.Monitor
  alias Mongo.ServerDescription
  alias Mongo.Session
  alias Mongo.Session.SessionPool
  alias Mongo.TopologyDescription

  @limits [
    :compression,
    :logical_session_timeout,
    :max_bson_object_size,
    :max_message_size_bytes,
    :max_wire_version,
    :max_write_batch_size,
    :read_only
  ]

  # https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#heartbeatfrequencyms-defaults-to-10-seconds-or-60-seconds
  @max_heartbeat_frequency_ms 10_000
  @min_heartbeat_frequency_ms 500

  @default_checkout_timeout 60_000

  @spec start_link(Keyword.t(), Keyword.t()) ::
          {:ok, pid}
          | {:error, reason :: atom}
  def start_link(opts, gen_server_opts \\ []) do
    gen_server_opts =
      opts
      |> Keyword.take([:debug, :name, :timeout, :spawn_opt])
      |> Keyword.merge(gen_server_opts)

    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  Update async the server_description received from the Monitor-Process
  """
  def update_server_description(pid, server_description) do
    GenServer.cast(pid, {:server_description, server_description})
  end

  @doc """
  Update async the rrt value received from the Monitor-Process
  """
  def update_rrt(pid, address, round_trip_time) do
    GenServer.cast(pid, {:update_rrt, address, round_trip_time})
  end

  @doc """
  Called from the monitor in case that a connection was established.
  """
  def monitor_connected(pid, monitor_pid) do
    GenServer.cast(pid, {:connected, monitor_pid})
  end

  def connection_for_address(pid, address) do
    GenServer.call(pid, {:connection, address})
  end

  def topology(pid) do
    GenServer.call(pid, :topology)
  end

  def get_state(pid) do
    GenServer.call(pid, :get_state)
  end

  # 97
  def select_server(pid, type, opts \\ []) do
    timeout = Keyword.get(opts, :checkout_timeout, @default_checkout_timeout)
    GenServer.call(pid, {:select_server, type, opts}, timeout)
  end

  def mark_server_unknown(pid, address) do
    server_description = ServerDescription.parse_hello_response(address, "not writable primary or recovering")
    update_server_description(pid, server_description)
  end

  def limits(pid) do
    GenServer.call(pid, :limits)
  end

  def wire_version(pid) do
    GenServer.call(pid, :wire_version)
  end

  def checkout_session(pid, read_write_type, opts \\ []) do
    timeout = Keyword.get(opts, :checkout_timeout, @default_checkout_timeout)
    GenServer.call(pid, {:checkout_session, read_write_type, opts}, timeout)
  end

  def checkin_session(pid, server_session) do
    GenServer.cast(pid, {:checkin_session, server_session})
    :ok
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  ## GenServer Callbacks

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#configuration
  def init(opts) do
    seeds = Keyword.get(opts, :seeds, [seed(opts)])
    type = Keyword.get(opts, :type, :unknown)
    set_name = Keyword.get(opts, :set_name, nil)
    local_threshold_ms = Keyword.get(opts, :local_threshold_ms, 15)

    :ok = Mongo.Events.notify(%TopologyOpeningEvent{topology_pid: self()})

    cond do
      type == :single and length(seeds) > 1 ->
        {:stop, :single_topology_multiple_hosts}

      set_name != nil and type not in [:unknown, :replica_set_no_primary, :single] ->
        {:stop, :set_name_bad_topology}

      true ->
        servers = servers_from_seeds(seeds)

        state =
          %{
            topology:
              TopologyDescription.defaults(%{
                type: type,
                set_name: set_name,
                servers: servers,
                local_threshold_ms: local_threshold_ms,
                heartbeat_frequency_ms: @min_heartbeat_frequency_ms
              }),
            seeds: seeds,
            opts: opts,
            monitors: %{},
            connection_pools: %{},
            session_pool: nil,
            waiting_pids: []
          }
          |> update_monitor()

        {:ok, state}
    end
  end

  defp seed(opts) do
    case Mongo.MongoDBConnection.Utils.hostname_port(opts) do
      {{:local, socket}, 0} -> socket
      {hostname, port} -> "#{hostname}:#{port}"
    end
  end

  def terminate(_reason, state) do
    case state.opts[:pw_safe] do
      nil -> nil
      pid -> GenServer.stop(pid)
    end

    Enum.each(state.connection_pools, fn {_address, pid} -> GenServer.stop(pid) end)
    Enum.each(state.monitors, fn {_address, pid} -> GenServer.stop(pid) end)
    Mongo.Events.notify(%TopologyClosedEvent{topology_pid: self()})
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#updating-the-topologydescription
  def handle_cast({:server_description, server_description}, state) do
    new_state = do_update_server_description(state, server_description)

    if state.topology != new_state.topology do
      Mongo.Events.notify(%TopologyDescriptionChangedEvent{
        topology_pid: self(),
        previous_description: state.topology,
        new_description: new_state.topology
      })
    end

    {:noreply, new_state}
  end

  ##
  # Updates the measured round trip time value for the specified address in the topology data structure
  ##
  def handle_cast({:update_rrt, address, round_trip_time}, state) do
    {:noreply, do_update_rrt(state, address, round_trip_time)}
  end

  def handle_cast(:reconcile, state) do
    new_state = update_monitor(state)
    {:noreply, new_state}
  end

  ##
  # In case of :monitor or :stream_monitor we mark the server description of the address as unknown
  ##
  def handle_cast({:disconnect, kind, address}, state) when kind in [:monitor, :stream_monitor] do
    server_description = ServerDescription.parse_hello_response(address, "#{inspect(kind)} disconnected")

    new_state =
      address
      |> remove_address(state)
      |> maybe_reinit()

    handle_cast({:server_description, server_description}, new_state)
  end

  def handle_cast({:disconnect, _kind, _host}, state) do
    {:noreply, state}
  end

  ##
  # After the monitor is connected to the server, the connection pool is started and
  # the "waiting pids" are informed to call the command again
  ##
  def handle_cast({:connected, monitor_pid}, state) do
    monitor = Enum.find(state.monitors, fn {_key, value} -> value == monitor_pid end)

    new_state =
      case monitor do
        nil ->
          state

        {host, ^monitor_pid} ->
          arbiters = fetch_arbiters(state)

          if host in arbiters do
            state
          else
            conn_opts =
              state.opts
              |> Keyword.put(:connection_type, :client)
              |> Keyword.put(:topology_pid, self())
              |> connect_opts_from_address(host)

            {:ok, pool} = DBConnection.start_link(Mongo.MongoDBConnection, conn_opts)
            connection_pools = Map.put(state.connection_pools, host, pool)

            Process.send_after(self(), {:new_connection, state.waiting_pids}, 10)

            %{state | connection_pools: connection_pools, waiting_pids: []}
          end
      end

    {:noreply, new_state}
  end

  def handle_cast({:force_check, server_address}, state) do
    case Map.fetch(state.monitors, server_address) do
      {:ok, monitor_pid} ->
        _diff = Monitor.force_check(monitor_pid)
        {:noreply, state}

      :error ->
        # ignore force checks on monitors that don't exist
        {:noreply, state}
    end
  end

  ##
  # checkin the current session
  #
  def handle_cast({:checkin_session, server_session}, %{:session_pool => pool} = state) do
    {:noreply, %{state | session_pool: SessionPool.checkin(pool, server_session)}}
  end

  def handle_info({:new_connection, waiting_pids}, state) do
    Enum.each(waiting_pids, fn from -> GenServer.reply(from, :new_connection) end)
    {:noreply, state}
  end

  ##
  # Update server description: in case of logical session the function creates a session pool for the `deployment`.
  #
  defp do_update_server_description(state, %{:logical_session_timeout => logical_session_timeout} = server_description) do
    state
    |> get_and_update_in([:topology], &TopologyDescription.update(&1, server_description, length(state.seeds)))
    |> process_events()
    |> update_heartbeat_frequency()
    |> update_monitor()
    |> update_session_pool(logical_session_timeout)
  end

  defp do_update_server_description(state, server_description) do
    state
    |> get_and_update_in([:topology], &TopologyDescription.update(&1, server_description, length(state.seeds)))
    |> process_events()
    |> update_heartbeat_frequency()
    |> update_monitor()
  end

  ##
  # Updates the rrt
  #
  defp do_update_rrt(state, address, round_trip_time) do
    update_in(state, [:topology], fn topology -> TopologyDescription.update_rrt(topology, address, round_trip_time) end)
  end

  defp update_heartbeat_frequency(%{:topology => topology} = state) do
    update_heartbeat_frequency(state, TopologyDescription.select_servers(topology, :write, []))
  end

  defp update_heartbeat_frequency(%{:topology => %{heartbeat_frequency_ms: current}, monitors: monitors} = state, :empty) do
    case current == @min_heartbeat_frequency_ms do
      true ->
        state

      false ->
        Enum.each(monitors, fn {_address, pid} -> Monitor.set_heartbeat_frequency_ms(pid, @min_heartbeat_frequency_ms) end)
        put_in(state[:topology][:heartbeat_frequency_ms], @min_heartbeat_frequency_ms)
    end
  end

  defp update_heartbeat_frequency(%{:topology => %{heartbeat_frequency_ms: current}, monitors: monitors} = state, _host) do
    case current == @max_heartbeat_frequency_ms do
      true ->
        state

      false ->
        ## filter own pid
        Enum.each(monitors, fn {_address, pid} -> Monitor.set_heartbeat_frequency_ms(pid, @max_heartbeat_frequency_ms) end)
        Process.send_after(self(), {:new_connection, state.waiting_pids}, 10)
        state = put_in(state[:topology][:heartbeat_frequency_ms], @max_heartbeat_frequency_ms)
        %{state | waiting_pids: []}
    end
  end

  defp process_events({events, state}) do
    Enum.each(events, fn
      {:force_check, _} = message ->
        :ok = GenServer.cast(self(), message)

      {previous, next} ->
        if previous != next do
          Mongo.Events.notify(%ServerDescriptionChangedEvent{
            address: next.address,
            topology_pid: self(),
            previous_description: previous,
            new_description: next
          })
        end

      _ ->
        :ok
    end)

    state
  end

  def handle_call(:topology, _from, state) do
    {:reply, state.topology, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:connection, address}, _from, state) do
    {:reply, Map.fetch(state.connection_pools, address), state}
  end

  ##
  # checkout a new session
  #
  def handle_call({:checkout_session, read_write_type, opts}, from, %{:topology => topology, :waiting_pids => waiting} = state) do
    case TopologyDescription.select_servers(topology, read_write_type, opts) do
      :empty ->
        Mongo.Events.notify(%ServerSelectionEmptyEvent{action: :checkout_session, cmd_type: read_write_type, topology: topology, opts: opts})
        ## no servers available, wait for connection
        {:noreply, %{state | waiting_pids: [from | waiting]}}

      ## found
      {:ok, {address, opts}} ->
        with {:ok, connection} <- get_connection(address, state),
             wire_version <- wire_version(address, topology),
             {server_session, new_state} <- checkout_server_session(state),
             {:ok, session} <- Session.start_link(self(), connection, address, server_session, wire_version, opts) do
          {:reply, {:ok, session}, new_state}
        else
          ## in case of an error, just return the error
          error -> {:reply, error, state}
        end

      error ->
        ## in case of an error, just return the error
        {:reply, error, state}
    end
  end

  def handle_call({:select_server, read_write_type, opts}, from, %{:topology => topology, :waiting_pids => waiting} = state) do
    case TopologyDescription.select_servers(topology, read_write_type, opts) do
      :empty ->
        Mongo.Events.notify(%ServerSelectionEmptyEvent{action: :select_server, cmd_type: read_write_type, topology: topology, opts: opts})
        ## no servers available, wait for connection
        {:noreply, %{state | waiting_pids: [from | waiting]}}

      {:ok, {address, _opts}} ->
        case get_connection(address, state) do
          {:ok, connection} ->
            {:reply, {:ok, connection}, state}

          ## in case of an error, just return the error
          error ->
            {:reply, error, state}
        end

      ## in case of an error, just return the error
      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:limits, _from, %{:topology => topology} = state) do
    case TopologyDescription.select_servers(topology, :write, []) do
      :empty ->
        Mongo.Events.notify(%ServerSelectionEmptyEvent{action: :limits, cmd_type: :write, topology: topology})
        {:reply, {:error, :empty}, state}

      {:ok, {address, _opts}} ->
        with {:ok, limits} <- get_limits(address, topology) do
          {:reply, {:ok, limits}, state}
        end

      error ->
        ## in case of an error, just return the error
        {:reply, error, state}
    end
  end

  def handle_call(:wire_version, _from, %{:topology => topology} = state) do
    case TopologyDescription.select_servers(topology, :write, []) do
      :empty ->
        Mongo.Events.notify(%ServerSelectionEmptyEvent{action: :wire_version, cmd_type: :read, topology: topology})
        {:reply, {:error, :empty}, state}

      {:ok, {address, _opts}} ->
        {:reply, {:ok, wire_version(address, topology)}, state}

      error ->
        ## in case of an error, just return the error
        {:reply, error, state}
    end
  end

  defp checkout_server_session(%{:session_pool => session_pool} = state) do
    with {session, pool} <- SessionPool.checkout(session_pool) do
      {session, %{state | session_pool: pool}}
    end
  end

  defp checkout_server_session(_state) do
    nil
  end

  defp get_connection(nil, _state), do: nil

  defp get_connection(address, %{connection_pools: pools}) do
    case Map.fetch(pools, address) do
      :error -> {:error, Mongo.Error.exception("The server #{inspect(address)} is no longer available")}
      conn -> conn
    end
  end

  defp get_limits(nil, _topology), do: nil

  defp get_limits(address, %{servers: servers}) do
    with {:ok, desc} <- Map.fetch(servers, address) do
      {:ok, Map.take(desc, @limits)}
    end
  end

  defp wire_version(nil, _topology), do: nil

  defp wire_version(address, topology) do
    case Map.fetch(topology.servers, address) do
      {:ok, server} -> server.max_wire_version
      _other -> 0
    end
  end

  ##
  # update the monitor process. For new servers the function creates new monitor processes.
  #
  defp update_monitor(%{topology: %{heartbeat_frequency_ms: heartbeat_frequency_ms}} = state) do
    arbiters = fetch_arbiters(state)
    old_addrs = Map.keys(state.monitors)
    # remove arbiters from connection pool as descriptions are received
    new_addrs = Map.keys(state.topology.servers) -- arbiters

    added = new_addrs -- old_addrs
    removed = old_addrs -- new_addrs

    state =
      Enum.reduce(added, state, fn address, state ->
        server_description = state.topology.servers[address]
        connopts = connect_opts_from_address(state.opts, address)

        Mongo.Events.notify(%ServerOpeningEvent{address: address, topology_pid: self()})

        args = [server_description.address, self(), heartbeat_frequency_ms, Keyword.put(connopts, :pool, DBConnection.ConnectionPool)]
        {:ok, pid} = Monitor.start_link(args)

        %{state | monitors: Map.put(state.monitors, address, pid)}
      end)

    Enum.reduce(removed, state, &remove_address/2)
  end

  defp update_session_pool(%{session_pool: nil, opts: opts} = state, logical_session_timeout) do
    %{state | session_pool: SessionPool.new(logical_session_timeout, opts)}
  end

  defp update_session_pool(state, _logical_session_timeout) do
    state
  end

  defp maybe_reinit(%{monitors: monitors} = state) when map_size(monitors) > 0 do
    state
  end

  defp maybe_reinit(state) do
    servers = servers_from_seeds(state.seeds)

    GenServer.cast(self(), :reconcile)

    put_in(state, [:topology, :servers], servers)
  end

  defp servers_from_seeds(seeds) do
    for addr <- seeds, into: %{} do
      {addr, ServerDescription.defaults(%{address: addr, type: :unknown})}
    end
  end

  defp remove_address(address, state) do
    Mongo.Events.notify(%ServerClosedEvent{address: address, topology_pid: self()})
    GenServer.stop(state.monitors[address])

    case state.connection_pools[address] do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    %{state | monitors: Map.delete(state.monitors, address), connection_pools: Map.delete(state.connection_pools, address)}
  end

  defp connect_opts_from_address(opts, address) do
    host_opts =
      ("mongodb://" <> address)
      |> URI.parse()
      |> Map.take([:host, :port])
      |> Enum.into([])
      |> rename_key(:host, :hostname)

    opts
    |> Keyword.merge(host_opts)
    |> Keyword.drop([:name])
  end

  defp rename_key(map, original_key, new_key) do
    value = Keyword.get(map, original_key)
    map |> Keyword.delete(original_key) |> Keyword.put(new_key, value)
  end

  defp fetch_arbiters(state) do
    Enum.flat_map(state.topology.servers, fn {_, s} -> s.arbiters end)
  end
end
