defmodule InsightsWeb.TopologyLive do
  use InsightsWeb, :live_view

  alias Mongo.Monitor
  alias Mongo.StreamingHelloMonitor
  alias Mongo.Topology

  require Logger

  @impl true
  def mount(_params, _session, socket) do

    if connected?(socket) do
      Phoenix.PubSub.subscribe(Insights.PubSub, "topology")
      Phoenix.PubSub.subscribe(Insights.PubSub, "commands")
    end

    {:ok, reset_defaults(socket)}
  end

  @impl true
  def render(assigns) do
    Phoenix.View.render(InsightsWeb.TopologyView, "index.html", assigns)
  end

  @impl true
  def handle_info(%Mongo.Events.ServerDescriptionChangedEvent{} = event, %{assigns: %{events: events}} = socket) do

    event = event
            |> Map.put(:time_stamp, DateTime.utc_now())
            |> Map.put(:id, random_string(10))
    events = [event | events] |> Enum.take(10)
    socket = socket
             |> set_topology(Topology.get_state(:mongo))
             |> assign(events: events)

    {:noreply, socket}
  end

  def handle_info(event, %{assigns: %{events: events}} = socket) do
    event = event
            |> Map.put(:time_stamp, DateTime.utc_now())
            |> Map.put(:id, random_string(10))
    events = [event | events] |> Enum.take(10)
    {:noreply, assign(socket, events: events)}
  end

  def handle_event("show-events", _params, socket) do
    {:noreply, assign(socket, tab: "events")}
  end

  def handle_event("show-details", _params, socket) do
    {:noreply, assign(socket, tab: "details")}
  end

  def handle_event("select-event", %{"id" => event_id}, %{assigns: %{events: events}} = socket) do
    {:noreply, assign(socket, event: Enum.find(events, fn %{id: id} -> event_id == id end))}
  end

  defp reset_defaults(socket) do
    socket
    |> set_topology(Topology.get_state(:mongo))
    |> assign(events: [])
    |> assign(tab: "details")
    |> assign(event: nil)
  end

  def set_topology(socket, %{topology: %{servers: servers} = topology, monitors: monitors}) do

    monitors = monitors
               |> Enum.map(fn {address, pid} -> {address, Monitor.get_state(pid)} end)
               |> Enum.into(%{})

    socket
    |> assign(topology: topology)
    |> assign(servers: Map.values(servers))
    |> assign(monitors: monitors)
  end

  def set_topology(socket, _other) do
    socket
    |> assign(topology: nil)
    |> assign(servers: [])
    |> assign(monitors: [])
  end

  def random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64 |> binary_part(0, length)
  end


end
