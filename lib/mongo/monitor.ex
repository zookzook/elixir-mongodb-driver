# see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#monitoring
defmodule Mongo.Monitor do
  @moduledoc """
  Each server has a monitor process. The monitor process is created by the topology process.

  If the network connection is working, then the monitor process reports this and the topology process starts the
  connection pool. Per server we get 1 + pool size connections to each server.

  After waiting for `heartbeat_frequency_ms` milliseconds, the monitor process calls `isMaster` command and
  reports the result to the topology process.

  The result of the `isMaster` command is mapped the `ServerDescription` structure and sent to the topology process, which
  updates it internal data structure.
  """

  use GenServer

  alias Mongo.Topology
  alias Mongo.ServerDescription
  alias Mongo.Events.{ServerHeartbeatStartedEvent, ServerHeartbeatFailedEvent,ServerHeartbeatSucceededEvent}

  # this is not configurable because the specification says so
  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#minheartbeatfrequencyms
  # not used @min_heartbeat_frequency_ms 500

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  # We need to stop asynchronously because a Monitor can call the Topology
  # which may try to stop the same Monitor that called it. Ending in a timeout.
  # See issues #139 for some information.

  @doc """
  Stop the monitor process.

  We need to stop asynchronously because a Monitor can call the Topology
  which may try to stop the same Monitor that called it. Ending in a timeout.
  """
  def stop(pid) do
    GenServer.cast(pid, :stop)
  end

  @doc """
  Try to update the server description.
  """
  def force_check(pid) do
    GenServer.cast(pid, :update)
  end


  @doc """
  Initialize the monitor process
  """
  def init([address, topology_pid, heartbeat_frequency_ms, connection_opts]) do

    # monitors don't authenticate and use the "admin" database
    opts = connection_opts
           |> Keyword.put(:database, "admin")
           |> Keyword.put(:skip_auth, true)
           |> Keyword.put(:after_connect, {__MODULE__, :connected, [self(), topology_pid]})
           |> Keyword.put(:backoff_min, heartbeat_frequency_ms)
           |> Keyword.put(:backoff_max, heartbeat_frequency_ms)
           |> Keyword.put(:backoff_type, :rand)
           |> Keyword.put(:connection_type, :monitor)
           |> Keyword.put(:topology_pid, topology_pid)
           |> Keyword.put(:pool_size, 1)
           |> Keyword.put(:idle_interval, 5_000)

    with {:ok, pid} <- DBConnection.start_link(Mongo.MongoDBConnection, opts) do
      {:ok, %{
        connection_pid: pid,                            ## our connection pid to the mongodb server
        topology_pid: topology_pid,                     ## the topology_pid to which we report
        address: address,                               ## the address of the server, needed to make updates
        round_trip_time: nil,                           ## current round_trip_time, needed to make average value
        heartbeat_frequency_ms: heartbeat_frequency_ms, ## current heartbeat_frequency_ms
        opts: opts ## options
      }}

    end

  end

  @doc """
  In this case we stop the DBConnection.
  """
  def terminate(reason, state) do
    GenServer.stop(state.connection_pid, reason)
  end

  @doc """
  Report the connection event, so the topology process can now create the connection pool.
  """
  def connected(_connection, me, topology_pid) do
    Topology.monitor_connected(topology_pid, me)
    GenServer.cast(me, :update)
  end

  @doc """
  Time to update the topology. Calling `isMaster` and updating the server description
  """
  def handle_cast(:update, state) do
    new_state = update_server_description(state)
    # we return with heartbeat_frequency_ms, so after heartbeat_frequency_ms handle_info(:timeout...) gets called.
    {:noreply, new_state, new_state.heartbeat_frequency_ms}
  end

  def handle_cast(:stop, state) do
    exit(:normal)
    {:noreply, state}
  end

  @doc """
  The `:timeout` call is the periodic call defined by the heartbeat frequency
  The ':update' call updates the server description for the topology process
  """
  def handle_info(:timeout, state) do
    new_state = update_server_description(state)
    {:noreply, new_state, new_state.heartbeat_frequency_ms}
  end
  def handle_info(:update, state) do
    new_state = update_server_description(state)
    # we return with heartbeat_frequency_ms, so after heartbeat_frequency_ms handle_info(:timeout...) gets called.
    {:noreply, new_state, new_state.heartbeat_frequency_ms}
  end

  ##
  # Get a new server description from the server and send it to the Topology process.
  #
  defp update_server_description(%{topology_pid: topology_pid} = state) do
    %{:round_trip_time => round_trip_time} = server_description = get_server_description(state, 0)
    Topology.update_server_description(topology_pid, server_description)
    %{state | round_trip_time: round_trip_time}
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#network-error-when-calling-ismaster
  ##
  # Calls isMaster and parses the result to update the server description
  # In case of one network error, the function tries one more time to call isMaster command
  #
  defp get_server_description(%{connection_pid: conn_pid, address: address, round_trip_time: last_rtt, opts: opts} = state, counter) do

    Mongo.Events.notify(%ServerHeartbeatStartedEvent{ connection_pid: conn_pid})

    {result, finish_time, rtt} = call_is_master_command(conn_pid, opts)
    case result do
      {:ok, is_master_reply} ->
        notify_success(rtt, is_master_reply, conn_pid)
        ServerDescription.from_is_master(address, average_rtt(last_rtt, rtt), finish_time, is_master_reply)

      {:error, error} when counter == 1 ->
        notify_error(rtt, error, conn_pid)
        ServerDescription.from_is_master_error(address, error)
      {:error, _error} -> get_server_description(state, counter + 1)
    end
  end

  defp average_rtt(nil, rtt) do
    round(rtt)
  end
  defp average_rtt(last_rtt, rtt) do
    round(0.2 * rtt + 0.8 * last_rtt)
  end

  defp call_is_master_command(conn_pid, opts) do
    {rtt, result} = :timer.tc(fn -> Mongo.exec_command(conn_pid, [isMaster: 1], opts) end)
    finish_time   = DateTime.utc_now()
    {result, finish_time, div(rtt, 1000)}
  end

  defp notify_error(rtt, error, conn_pid) do
    :ok = Mongo.Events.notify(%ServerHeartbeatFailedEvent{
            duration: rtt,
             failure: error,
      connection_pid: conn_pid
    })
  end

  defp notify_success(rtt, reply, conn_pid) do
    :ok = Mongo.Events.notify(%ServerHeartbeatSucceededEvent{
      duration: rtt,
      reply: reply,
      connection_pid: conn_pid
    })
  end

end
