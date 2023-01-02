defmodule Mongo.StreamingHelloMonitor do
  @moduledoc """
  See https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#streaming-protocol

  The streaming protocol is used to monitor MongoDB 4.4+ servers and optimally reduces the time it takes for a client to discover server state changes.
  Multi-threaded or asynchronous drivers MUST use the streaming protocol when connected to a server that supports the awaitable hello or legacy hello commands.
  This protocol requires an extra thread and an extra socket for each monitor to perform RTT calculations.

  This module implements the streaming protocol. The GenServer is started and maintained by the Monitor process. The streaming hello monitor uses the
  more to come flag while updating the current server description.
  """
  require Logger

  use GenServer
  import Bitwise

  alias Mongo.Events.ServerHeartbeatFailedEvent
  alias Mongo.Events.ServerHeartbeatStartedEvent
  alias Mongo.Events.ServerHeartbeatSucceededEvent
  alias Mongo.ServerDescription
  alias Mongo.StreamingHelloMonitor
  alias Mongo.Topology

  @more_to_come_mask 0x2

  def start_link(args) do
    GenServer.start_link(StreamingHelloMonitor, args)
  end

  @doc """
  Initialize the monitor process
  """
  def init([topology_pid, address, opts]) do
    heartbeat_frequency_ms = 10_000

    opts =
      opts
      |> Keyword.drop([:after_connect])
      |> Keyword.put(:after_connect, {StreamingHelloMonitor, :connected, [self()]})
      |> Keyword.put(:connection_type, :stream_monitor)

    ## debug info("Starting stream hello monitor with options #{inspect(opts, pretty: true)}")

    with {:ok, pid} <- DBConnection.start_link(Mongo.MongoDBConnection, opts) do
      {:ok,
       %{
         ## our connection pid to the mongodb server
         connection_pid: pid,
         ## the topology_pid to which we report
         topology_pid: topology_pid,
         ## the address of the server, needed to make updates
         address: address,
         ## current heartbeat_frequency_ms
         heartbeat_frequency_ms: heartbeat_frequency_ms,
         max_await_time_ms: heartbeat_frequency_ms,
         more_to_come: false,
         # {processId: <ObjectId>, counter: <int64>},
         topology_version: nil,
         ## options
         opts: opts
       }}
    end
  end

  @doc """
  In this case we stop the DBConnection.
  """
  def terminate(reason, %{connection_pid: connection_pid}) do
    ## debug info("Terminating streaming hello monitor for reason #{inspect reason}")
    GenServer.stop(connection_pid, reason)
  end

  @doc """
  Report the connection event, so the topology process can now create the connection pool.
  """
  def connected(_connection, me) do
    GenServer.cast(me, :update)
  end

  @doc """
  Time to update the topology. Calling `hello` and updating the server description
  """
  def handle_cast(:update, state) do
    handle_info(:update, state)
  end

  def handle_info(:update, state) do
    {:noreply, update_server_description(state)}
  end

  ##
  # Get a new server description from the server and send it to the Topology process.
  #
  defp update_server_description(%{topology_pid: topology_pid} = state) do
    with {topology_version, flags, server_description} <- get_server_description(state) do
      ## debug info("Updating server description")
      Topology.update_server_description(topology_pid, server_description)
      state = %{state | topology_version: topology_version}

      case flags &&& @more_to_come_mask do
        @more_to_come_mask ->
          state = %{state | more_to_come: true}
          update_server_description(state)

        _other ->
          Process.send_after(self(), :update, state.heartbeat_frequency_ms)
          %{state | more_to_come: false}
      end
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#network-error-when-calling-ismaster
  ##
  # Calls hello command and parses the result to update the server description
  #
  defp get_server_description(%{connection_pid: conn_pid, address: address, topology_version: topology_version, max_await_time_ms: max_await_time_ms, opts: opts} = state) do
    Mongo.Events.notify(%ServerHeartbeatStartedEvent{connection_pid: conn_pid})

    opts = Keyword.merge(opts, timeout: max_await_time_ms * 2)

    {duration, result} =
      case state do
        %{more_to_come: true} ->
          :timer.tc(fn -> Mongo.exec_more_to_come(conn_pid, opts) end)

        _other ->
          opts = Keyword.put(opts, :max_await_time_ms, max_await_time_ms)
          :timer.tc(fn -> hello_command(conn_pid, topology_version, opts) end)
      end

    case result do
      {:ok, {flags, hello_doc}} ->
        server_description =
          hello_doc
          |> ServerDescription.parse_hello_response()
          |> Map.put(:address, address)
          |> Map.put(:last_update_time, DateTime.utc_now())
          |> Map.put(:error, nil)

        notify_success(duration, hello_doc, conn_pid)
        {hello_doc["topologyVersion"], flags, server_description}

      {:error, error} ->
        notify_error(duration, error, conn_pid)

        server_description =
          ServerDescription.new()
          |> Map.put(:address, address)
          |> Map.put(:error, error)

        {nil, 0x0, server_description}
    end
  end

  defp hello_command(conn_pid, %{"counter" => counter, "processId" => process_id}, opts) do
    max_await_time_ms = Keyword.get(opts, :max_await_time_ms, 10_000)

    opts =
      opts
      |> Keyword.merge(flags: [:exhaust_allowed])
      |> Keyword.merge(timeout: max_await_time_ms * 2)

    cmd = [
      maxAwaitTimeMS: max_await_time_ms,
      topologyVersion: %{
        counter: %BSON.LongNumber{value: counter},
        processId: process_id
      }
    ]

    Mongo.exec_hello(conn_pid, cmd, opts)
  end

  defp hello_command(conn_pid, _topology_version, opts) do
    Mongo.exec_hello(conn_pid, opts)
  end

  defp notify_error(duration, error, conn_pid) do
    Mongo.Events.notify(%ServerHeartbeatFailedEvent{duration: duration, failure: error, connection_pid: conn_pid})
  end

  defp notify_success(duration, reply, conn_pid) do
    Mongo.Events.notify(%ServerHeartbeatSucceededEvent{duration: duration, reply: reply, connection_pid: conn_pid})
  end

  def info(message) do
    Logger.info(IO.ANSI.format([:blue, :bright, message]))
  end
end
