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
    end

    {:ok, reset_defaults(socket)}
  end

  @impl true
  def render(assigns) do
    Phoenix.View.render(InsightsWeb.TopologyView, "index.html", assigns)
  end

  @impl true
  def handle_info(%Mongo.Events.ServerDescriptionChangedEvent{}, socket) do
    {:noreply, reset_defaults(socket)}
  end

  def handle_info(_message, socket) do
    {:noreply, socket}
  end

  defp reset_defaults(socket) do
    set_topology(socket, Topology.get_state(:mongo))
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


end
