defmodule Mongo.Monitor do
  @moduledoc """
  Each server has a monitor process. The monitor process is created by the topology process.

  If the network connection is working, then the monitor process reports this and the topology process starts the
  connection pool. Per server we get 1 + pool size connections to each server.

  After waiting for `heartbeat_frequency_ms` milliseconds, the monitor process calls `hello` command and
  reports the result to the topology process.

  The result of the hello command is mapped the `ServerDescription` structure and sent to the topology process, which
  updates it internal data structure.

  see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#monitoring
  """
  require Logger

  use GenServer

  alias Mongo.Events.ServerHeartbeatFailedEvent
  alias Mongo.Events.ServerHeartbeatStartedEvent
  alias Mongo.Events.ServerHeartbeatSucceededEvent
  alias Mongo.Monitor
  alias Mongo.ServerDescription
  alias Mongo.StreamingHelloMonitor
  alias Mongo.Topology

  @monitor_modes [
    :polling_mode,
    :streaming_mode
  ]

  @min_wire_version_streaming_protocol 9

  # this is not configurable because the specification says so
  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#minheartbeatfrequencyms
  # not used @min_heartbeat_frequency_ms 500

  def start_link(args) do
    GenServer.start_link(Monitor, args)
  end

  @doc """
  Try to update the server description.
  """
  def force_check(pid) do
    GenServer.cast(pid, :update)
  end

  def set_heartbeat_frequency_ms(pid, heartbeat_frequency_ms) do
    GenServer.cast(pid, {:update, heartbeat_frequency_ms})
  end

  def get_state(pid) do
    GenServer.call(pid, :get_state)
  end

  @doc """
  Initialize the monitor process
  """
  def init([address, topology_pid, heartbeat_frequency_ms, connection_opts]) do
    ## debug info("Starting monitor process with pid #{inspect self()}, #{inspect address}")

    # monitors don't authenticate and use the "admin" database
    opts =
      connection_opts
      |> Keyword.put(:database, "admin")
      |> Keyword.put(:skip_auth, true)
      |> Keyword.put(:after_connect, {Monitor, :connected, [self(), topology_pid]})
      |> Keyword.put(:backoff_min, 500)
      |> Keyword.put(:backoff_max, 1_000)
      |> Keyword.put(:connection_type, :monitor)
      |> Keyword.put(:topology_pid, topology_pid)
      |> Keyword.put(:pool_size, 1)
      |> Keyword.put(:idle_interval, 5_000)

    with {:ok, pid} <- DBConnection.start_link(Mongo.MongoDBConnection, opts) do
      {:ok,
       %{
         ## we are starting with the polling mode
         mode: :polling_mode,
         ## our connection pid to the mongodb server
         connection_pid: pid,
         ## the topology_pid to which we report
         topology_pid: topology_pid,
         ## the address of the server, needed to make updates
         address: address,
         ## current round_trip_time, needed to make average value
         round_trip_time: nil,
         ## current heartbeat_frequency_ms
         heartbeat_frequency_ms: heartbeat_frequency_ms,
         ## options
         opts: opts,
         streaming_pid: nil
       }}
    end
  end

  @doc """
  In case of terminating we stop the our linked processes as well:
  * connection
  * streaming process
  """
  def terminate(reason, %{connection_pid: connection_pid, streaming_pid: nil}) do
    ## debug info("Terminating monitor for reason #{inspect reason}")
    GenServer.stop(connection_pid, reason)
  end

  def terminate(reason, %{connection_pid: connection_pid, streaming_pid: streaming_pid}) do
    ## debug info("Terminating monitor for reason #{inspect reason}")

    GenServer.stop(connection_pid, reason)
    GenServer.stop(streaming_pid, reason)
  end

  @doc """
  Report the connection event, so the topology process can now create the connection pool.
  """
  def connected(_connection, me, topology_pid) do
    Topology.monitor_connected(topology_pid, me)
    GenServer.cast(me, :update)
  end

  def handle_call(:get_state, _from, state) do
    {:reply, Map.put(state, :pid, self()), state}
  end

  ##
  # Update the server description or the rrt value
  ##
  def handle_cast(:update, state) do
    handle_info(:update, state)
  end

  ##
  # Update the server description or the rrt value and set new heartbeat value
  ##
  def handle_cast({:update, heartbeat_frequency_ms}, state) do
    new_state =
      state
      |> update_server_description()
      |> Map.put(:heartbeat_frequency_ms, heartbeat_frequency_ms)

    {:noreply, new_state}
  end

  ##
  # Updates the server description or the rrt value
  ##
  def handle_info(:update, state) do
    new_state = update_server_description(state)

    ## debug info("Calling update: #{inspect new_state.address}, #{inspect new_state.heartbeat_frequency_ms}")
    Process.send_after(self(), :update, new_state.heartbeat_frequency_ms)

    {:noreply, new_state}
  end

  ##
  # Polling mode: get a new server description from the server and new round_trip_time value
  # and send it to the topology process. If possible start the streaming mode.
  ##
  defp update_server_description(%{connection_pid: conn_pid, topology_pid: topology_pid, mode: :polling_mode} = state) do
    case get_server_description(state) do
      %{round_trip_time: round_trip_time, max_wire_version: max_wire_version} = server_description ->
        ## debug info("Updating server description: #{inspect(server_description, pretty: true)}")

        Mongo.Events.notify(%ServerHeartbeatStartedEvent{connection_pid: conn_pid})
        Topology.update_server_description(topology_pid, server_description)
        state = %{state | round_trip_time: round_trip_time}

        case max_wire_version >= @min_wire_version_streaming_protocol do
          true ->
            start_streaming_mode(state, server_description)

          false ->
            state
        end

      error ->
        Logger.warn("Unable to update server description because of #{inspect(error)}")
        state
    end
  end

  ##
  # Get a new server description from the server and send it to the Topology process.
  ##
  defp update_server_description(%{topology_pid: topology_pid, address: address, mode: :streaming_mode} = state) do
    case get_server_description(state) do
      %{round_trip_time: round_trip_time} ->
        ## debug info("Updating round_trip_time: #{inspect round_trip_time}")
        Topology.update_rrt(topology_pid, address, round_trip_time)

        %{state | round_trip_time: round_trip_time}

      error ->
        Logger.warn("Unable to round trip time because of #{inspect(error)}")
        state
    end
  end

  ##
  # Starts the streaming mode
  ##
  defp start_streaming_mode(%{address: address, topology_pid: topology_pid, opts: opts} = state, _server_description) do
    args = [topology_pid, address, opts]

    case StreamingHelloMonitor.start_link(args) do
      {:ok, pid} ->
        ## debug info("Starting streaming mode")
        %{state | mode: :streaming_mode, streaming_pid: pid, heartbeat_frequency_ms: 10_000}

      error ->
        Logger.warn("Unable to start the streaming hello monitor, because of #{inspect(error)}")
        state
    end
  end

  ##
  # Streaming mode: calls hello command and updated the round trip time for the command.
  ##
  defp get_server_description(%{connection_pid: conn_pid, round_trip_time: last_rtt, mode: :streaming_mode, opts: opts}) do
    {rtt, response} = :timer.tc(fn -> Mongo.exec_hello(conn_pid, opts) end)

    case response do
      {:ok, {_flags, _hello_doc}} ->
        %{round_trip_time: average_rtt(last_rtt, div(rtt, 1000))}

      error ->
        error
    end
  end

  ##
  # Polling mode: updating the server description and the round trip time together
  ##
  defp get_server_description(%{connection_pid: conn_pid, address: address, round_trip_time: last_rtt, opts: opts}) do
    {rtt, response} = :timer.tc(fn -> Mongo.exec_hello(conn_pid, opts) end)

    case response do
      {:ok, {_flags, hello_doc}} ->
        notify_success(rtt, hello_doc, conn_pid)

        hello_doc
        |> ServerDescription.parse_hello_response()
        |> Map.put(:round_trip_time, average_rtt(last_rtt, div(rtt, 1000)))
        |> Map.put(:address, address)
        |> Map.put(:last_update_time, DateTime.utc_now())
        |> Map.put(:error, nil)

      {:error, error} ->
        notify_error(rtt, error, conn_pid)

        ServerDescription.new()
        |> Map.put(:address, address)
        |> Map.put(:error, error)
    end
  end

  defp average_rtt(nil, rtt) do
    round(rtt)
  end

  defp average_rtt(last_rtt, rtt) do
    round(0.2 * rtt + 0.8 * last_rtt)
  end

  defp notify_error(rtt, error, conn_pid) do
    Mongo.Events.notify(%ServerHeartbeatFailedEvent{duration: rtt, failure: error, connection_pid: conn_pid})
  end

  defp notify_success(rtt, reply, conn_pid) do
    Mongo.Events.notify(%ServerHeartbeatSucceededEvent{duration: rtt, reply: reply, connection_pid: conn_pid})
  end

  @doc """
  Returns the possible modes of the monitor process.
  """
  def modes() do
    @monitor_modes
  end

  def info(message) do
    Logger.info(IO.ANSI.format([:light_magenta, :bright, message]))
  end
end
