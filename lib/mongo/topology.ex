defmodule Mongo.Topology do
  @moduledoc false

  require Logger

  use GenServer
  alias Mongo.Events.{ServerDescriptionChangedEvent, ServerOpeningEvent, ServerClosedEvent,
                      TopologyDescriptionChangedEvent, TopologyOpeningEvent, TopologyClosedEvent}
  alias Mongo.TopologyDescription
  alias Mongo.ServerDescription
  alias Mongo.Monitor
  alias Mongo.Session.SessionPool
  alias Mongo.Session

  # https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#heartbeatfrequencyms-defaults-to-10-seconds-or-60-seconds
  @heartbeat_frequency_ms 10_000

  @spec start_link(Keyword.t, Keyword.t) ::
          {:ok, pid} |
          {:error, reason :: atom}
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

  def select_server(pid, type, opts \\ []) do
    GenServer.call(pid, {:select_server, type, opts})
  end

  @doc """
  Checkout a new session. In case of an explicit session the session is returned and no new session will be created.
  """
  def checkout_session(pid, cmd_type, type, opts \\ []) do
    session = Keyword.get(opts, :session, nil)
    case Session.alive?(session) do
       false -> GenServer.call(pid, {:checkout_session, cmd_type, type, opts})
       true -> {:ok, session}
    end

  end

  def checkin_session(pid, session) do
    GenServer.cast(pid, {:checkin_session, session})
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  ## GenServer Callbacks

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#configuration
  @doc false
  def init(opts) do
    seeds              = Keyword.get(opts, :seeds, [seed(opts)])
    type               = Keyword.get(opts, :type, :unknown)
    set_name           = Keyword.get(opts, :set_name, nil)
    local_threshold_ms = Keyword.get(opts, :local_threshold_ms, 15)

    :ok = Mongo.Events.notify(%TopologyOpeningEvent{topology_pid: self()})

    cond do
      type == :single and length(seeds) > 1 -> {:stop, :single_topology_multiple_hosts}
      set_name != nil and not(type in [:unknown, :replica_set_no_primary, :single]) -> {:stop, :set_name_bad_topology}
      true ->
        servers = servers_from_seeds(seeds)
        state = %{
            topology: TopologyDescription.defaults(%{
              type: type,
              set_name: set_name,
              servers: servers,
              local_threshold_ms: local_threshold_ms
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
      {hostname, port}      -> "#{hostname}:#{port}"
    end
  end

  def terminate(_reason, state) do
    Enum.each(state.connection_pools, fn {_address, pid} -> GenServer.stop(pid) end)
    Enum.each(state.monitors, fn {_address, pid} -> GenServer.stop(pid) end)
    :ok = Mongo.Events.notify(%TopologyClosedEvent{
      topology_pid: self()
    })
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#updating-the-topologydescription
  def handle_cast({:server_description, server_description}, state) do
    new_state = handle_server_description(state, server_description)
    if state.topology != new_state.topology do
      :ok = Mongo.Events.notify(%TopologyDescriptionChangedEvent{
        topology_pid: self(),
        previous_description: state.topology,
        new_description: new_state.topology
      })
    end
    {:noreply, new_state}
  end

  def handle_cast(:reconcile, state) do
    new_state = update_monitor(state)
    {:noreply, new_state}
  end
  def handle_cast({:disconnect, :monitor, host}, state) do
    new_state = remove_address(host, state)
    maybe_reinit(new_state)
    {:noreply, new_state}
  end
  def handle_cast({:disconnect, :client, _host}, state) do
    {:noreply, state}
  end

  def handle_cast({:connected, monitor_pid}, state) do
    monitor = Enum.find(state.monitors, fn {_key, value} -> value == monitor_pid end)
    new_state = case monitor do
      nil -> state
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

          Process.send_after(self(), {:new_connection, state.waiting_pids, host}, 1000)

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
  # checkin the current session, if the session was implicit created
  #
  def handle_cast({:checkin_session, session}, %{:session_pool => pool} = state) do
    case Session.server_session(session) do
      {:ok, server_session, true} -> SessionPool.checkin(pool, server_session)
      _                           -> []
    end

    {:noreply, state}
  end

  def handle_info({:new_connection, waiting_pids, host}, state) do
    Enum.each(waiting_pids, fn from -> GenServer.reply(from, {:new_connection, host}) end)
    {:noreply, state}
  end

  ##
  # Update server description: in case of logical session the function creates a session pool for the `deployment`.
  #
  defp handle_server_description(state, %{:logical_session_timeout => logical_session_timeout} = server_description) do
    state
    |> get_and_update_in([:topology], &TopologyDescription.update(&1, server_description, length(state.seeds)))
    |> process_events()
    |> update_monitor()
    |> update_session_pool(logical_session_timeout)
  end
  defp handle_server_description(state, server_description) do
    state
    |> get_and_update_in([:topology], &TopologyDescription.update(&1, server_description, length(state.seeds)))
    |> process_events()
    |> update_monitor()
  end

  defp process_events({events, state}) do
    Enum.each(events, fn
      {:force_check, _} = message -> :ok = GenServer.cast(self(), message)
      {previous, next} ->
        if previous != next do
          :ok = Mongo.Events.notify(%ServerDescriptionChangedEvent{
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

  def handle_call({:connection, address}, _from, state) do
    {:reply, Map.fetch(state.connection_pools, address), state}
  end

  # checkout a new session
  def handle_call({:checkout_session, cmd_type, type, opts}, from, %{:topology => topology, :waiting_pids => waiting, connection_pools: pools} = state) do

    case TopologyDescription.select_servers(topology, cmd_type, opts) do
      :empty ->
        Logger.debug("select_server: empty")
        {:noreply, %{state | waiting_pids: [from | waiting]}} ## no servers available, wait for connection

      {:ok, servers, slave_ok, mongos?} ->                ## found, select randomly a server and return its connection_pool
        Logger.debug("select_server: found #{inspect servers}, pools: #{inspect pools}")

        with {:ok, connection} <- servers
                                  |> Enum.take_random(1)
                                  |> get_connection(state),
             server_session <- checkout_server_session(state),
            {:ok, session}  <- Session.start_link(connection, server_session, slave_ok, mongos?, type, opts) do
          Logger.debug("select_server: connection is #{inspect connection}, server_session is #{inspect server_session}")
          {:reply, {:ok, session, slave_ok, mongos?}, state}
        end

      error ->
        Logger.debug("select_servers: #{inspect error}")
        {:reply, error, state} ## in case of an error, just return the error
    end
  end

  def handle_call({:select_server, type, opts}, from, %{:topology => topology, :waiting_pids => waiting, connection_pools: pools} = state) do
    case TopologyDescription.select_servers(topology, type, opts) do
      :empty ->
        Logger.debug("select_server: empty")
        {:noreply, %{state | waiting_pids: [from | waiting]}} ## no servers available, wait for connection

      {:ok, servers, slave_ok, mongos?} ->                ## found, select randomly a server and return its connection_pool
        Logger.debug("select_server: found #{inspect servers}, pools: #{inspect pools}")

        with {:ok, connection} <- servers
                                  |> Enum.take_random(1)
                                  |> get_connection(state) do
          Logger.debug("select_server: connection is #{inspect connection}")

          {:reply, {:ok, connection, slave_ok, mongos?}, state}
        end
      error ->
        Logger.debug("select_servers: #{inspect error}")
        {:reply, error, state} ## in case of an error, just return the error
    end
  end

  defp checkout_server_session(%{:session_pool => session_pool}) do
    SessionPool.checkout(session_pool)
  end
  defp checkout_server_session(_state) do
    nil
  end

  defp get_connection([], _state), do: nil
  defp get_connection([address], %{connection_pools: pools}), do: Map.fetch(pools, address)

  ##
  # update the monitor process. For new servers the function creates new monitor processes.
  #
  defp update_monitor(state) do
    arbiters = fetch_arbiters(state)
    old_addrs = Map.keys(state.monitors)
    # remove arbiters from connection pool as descriptions are recieved
    new_addrs = Map.keys(state.topology.servers) -- arbiters

    added = new_addrs -- old_addrs
    removed = old_addrs -- new_addrs

    state = Enum.reduce(added, state, fn (address, state) ->

      server_description = state.topology.servers[address]
      connopts = connect_opts_from_address(state.opts, address)

      Mongo.Events.notify(%ServerOpeningEvent{address: address, topology_pid: self()})

      args = [server_description.address, self(), @heartbeat_frequency_ms, Keyword.put(connopts, :pool, DBConnection.ConnectionPool)]
      {:ok, pid} = Monitor.start_link(args)

      %{ state | monitors: Map.put(state.monitors, address, pid) }
    end)

    Enum.reduce(removed, state, &remove_address/2)
  end

  defp update_session_pool(%{:session_pool => nil} = state, logical_session_timeout) do
    Logger.debug("Creating session pool")
    {:ok, session_pool} = SessionPool.start_link(self(), logical_session_timeout)
    %{ state | session_pool: session_pool}
  end
  defp update_session_pool(state, _logical_session_timeout) do
    state
  end

  defp maybe_reinit(%{monitors: monitors} = state) when map_size(monitors) > 0,
    do: state
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
    :ok = Mongo.Events.notify(%ServerClosedEvent{address: address, topology_pid: self()})
    :ok = Monitor.stop(state.monitors[address])

    :ok = case state.connection_pools[address] do
            nil -> :ok
            pid -> GenServer.stop(pid)
          end

    %{state | monitors: Map.delete(state.monitors, address),
      connection_pools: Map.delete(state.connection_pools, address)}
  end

  defp connect_opts_from_address(opts, address) do
    host_opts =
      "mongodb://" <> address
      |> URI.parse
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
