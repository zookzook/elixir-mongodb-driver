# see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#monitoring
defmodule Mongo.StreamingHelloMonitor do
  @moduledoc """
  Each server has a monitor process. The monitor process is created by the topology process.

  If the network connection is working, then the monitor process reports this and the topology process starts the
  connection pool. Per server we get 1 + pool size connections to each server.

  After waiting for `heartbeat_frequency_ms` milliseconds, the monitor process calls `isMaster` command and
  reports the result to the topology process.

  The result of the `isMaster` command is mapped the `ServerDescription` structure and sent to the topology process, which
  updates it internal data structure.
  """
  require Logger

  use GenServer
  use Bitwise

  alias Mongo.StreamingHelloMonitor
  alias Mongo.Topology
  alias Mongo.ServerDescription
  alias Mongo.Events.{ServerHeartbeatStartedEvent, ServerHeartbeatFailedEvent,ServerHeartbeatSucceededEvent}

  # this is not configurable because the specification says so
  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#minheartbeatfrequencyms
  # not used @min_heartbeat_frequency_ms 500

  def start_link(args) do
    GenServer.start_link(StreamingHelloMonitor, args)
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

  @doc """
  Initialize the monitor process
  """
  def init([topology_pid, address, heartbeat_frequency_ms, opts]) do

    opts = opts
           |> Keyword.drop([:after_connect])
           |> Keyword.put(:after_connect, {__MODULE__, :connected, [self()]})
           |> Keyword.put(:connection_type, :stream_monitor)


    info("Starting stream hello monitor with options #{inspect(opts, pretty: true)}")

    with {:ok, pid} <- DBConnection.start_link(Mongo.MongoDBConnection, opts) do
      {:ok, %{
        connection_pid: pid,                            ## our connection pid to the mongodb server
        topology_pid: topology_pid,                     ## the topology_pid to which we report
        address: address,                               ## the address of the server, needed to make updates
        heartbeat_frequency_ms: heartbeat_frequency_ms, ## current heartbeat_frequency_ms
        max_await_time_ms: 10000,
        more_to_come: false,
        topology_version: nil, # {processId: <ObjectId>, counter: <int64>},
        opts: opts ## options
      }}
    end

  end

  @doc """
  In this case we stop the DBConnection.
  """
  def terminate(reason, %{connection_pid: connection_pid} = state) do
    info("Terminating streaming hello monitor for reason #{inspect reason}")
    GenServer.stop(connection_pid, reason)
  end

  @doc """
  Report the connection event, so the topology process can now create the connection pool.
  """
  def connected(_connection, me) do
    GenServer.cast(me, :update)
  end

  @doc """
  Time to update the topology. Calling `isMaster` and updating the server description
  """
  def handle_cast(:update, state) do
    handle_info(:update, state)
  end

  def handle_info(:update, state) do
    new_state = update_server_description(state)
    Process.send_after(self(), :update, new_state.heartbeat_frequency_ms)
    {:noreply, new_state}
  end

  ##
  # Get a new server description from the server and send it to the Topology process.
  #
  defp update_server_description(%{topology_pid: topology_pid} = state) do
    with {topology_version, flags, server_description} <- get_server_description(state) do
      Topology.update_server_description(topology_pid, server_description)
      state = %{state | topology_version: topology_version}

      case flags &&& 0x2 do
        0x2 ->
          info("More to come")
          state = %{state | more_to_come: true}
          update_server_description(state)
        _other ->
          %{state | more_to_come: false}
      end

    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#network-error-when-calling-ismaster
  ##
  # Calls isMaster and parses the result to update the server description
  # In case of one network error, the function tries one more time to call isMaster command
  #
  defp get_server_description(%{connection_pid: conn_pid, address: address, topology_version: topology_version, opts: opts} = state) do

    Mongo.Events.notify(%ServerHeartbeatStartedEvent{ connection_pid: conn_pid})

    {duration, result} = case state do
      %{more_to_come: true} ->
        :timer.tc(fn ->

          info("Calling more to come")
          result = Mongo.exec_more_to_come(conn_pid, opts)
          info("End of more to come")
          result
        end)

      _other ->
        :timer.tc(fn -> hello_command(conn_pid, topology_version, opts) end)
    end

    case result do
      {:ok, {flags, hello_doc}} ->

        info("Got flags: #{inspect flags}")

        server_description = hello_doc
                             |> ServerDescription.parse_hello_response()
                             |> Map.put(:address, address)
                             |> Map.put(:last_update_time, DateTime.utc_now())
                             |> Map.put(:error, nil)

        notify_success(duration, hello_doc, conn_pid)
        {hello_doc["topologyVersion"], flags, server_description}

      {:error, error} ->
        notify_error(duration, error, conn_pid)

        server_description = ServerDescription.new()
                             |> Map.put(:address, address)
                             |> Map.put(:error, error)

        {nil, 0x0, server_description}

    end
  end

  defp hello_command(conn_pid, %{"counter" => counter, "processId" => process_id}, opts) do
    opts = Keyword.merge(opts, [flags: [:exhaust_allowed]])
    Mongo.exec_command(conn_pid, [isMaster: 1, maxAwaitTimeMS: 10_000, topologyVersion: %{counter: %BSON.LongNumber{value: counter}, processId: process_id}], opts)
  end

  defp hello_command(conn_pid, _topology_version, opts) do
    Mongo.exec_command(conn_pid, [isMaster: 1], opts)
  end

  defp notify_error(duration, error, conn_pid) do
    Mongo.Events.notify(%ServerHeartbeatFailedEvent{duration: duration, failure: error, connection_pid: conn_pid})
  end

  defp notify_success(duration, reply, conn_pid) do
    Mongo.Events.notify(%ServerHeartbeatSucceededEvent{duration: duration, reply: reply, connection_pid: conn_pid})
  end

  defp info(message) do
    Logger.info(IO.ANSI.format([:blue, :bright, message]))
  end

end
