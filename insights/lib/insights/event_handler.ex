defmodule Insights.EventHandler do

  require Logger

  use GenServer

  @me __MODULE__

  @doc """
  Starts the GenServer.
  """
  def start_link(_args) do
    GenServer.start_link(@me, :no_args, name: @me)
  end

  @impl true
  def init(:no_args) do

    info("Starting insights event handler")

    Registry.register(:events_registry, :commands, [])
    Registry.register(:events_registry, :topology, [])

    {:ok, %{}}
  end

  def handle_info({:broadcast, :topology, event}, state) do
    Phoenix.PubSub.local_broadcast(Insights.PubSub, "topology", event)
    {:noreply, state}
  end

  def handle_info({:broadcast, :commands, event}, state) do
    Phoenix.PubSub.local_broadcast(Insights.PubSub, "commands", event)
    {:noreply, state}
  end

  def handle_info(_message, state) do
    ## info("Receiving message: #{inspect message}")
    {:noreply, state}
  end

  defp info(message) do
    Logger.info(IO.ANSI.format([:light_magenta, :bright, message]))
  end
end