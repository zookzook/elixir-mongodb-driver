defmodule EventCatcher do
  @moduledoc false

  use GenServer

  require Logger

  alias Mongo.Events.CommandSucceededEvent
  alias Mongo.Events.CommandFailedEvent
  alias Mongo.Events.RetryReadEvent
  alias Mongo.Events.RetryWriteEvent
  alias Mongo.Events.ServerSelectionEmptyEvent

  @all [:commands, :topology]

  def start_link(topics \\ @all) do
    GenServer.start_link(__MODULE__, topics)
  end

  def stop(pid) do
    GenServer.cast(pid, :stop)
  end

  def events(pid) do
    GenServer.call(pid, :events)
  end

  def terminate(_reason, _state) do
    :ok
  end

  def succeeded_events(pid) do
    GenServer.call(pid, :succeeded_events)
  end

  def failed_events(pid) do
    GenServer.call(pid, :failed_events)
  end

  def retryable_read_events(pid) do
    GenServer.call(pid, :retryable_read_events)
  end

  def retry_write_events(pid) do
    GenServer.call(pid, :retry_write_events)
  end

  def empty_selection_events(pid) do
    GenServer.call(pid, :empty_selection_events)
  end

  def init(topics) do
    Enum.each(topics, fn topic -> Registry.register(:events_registry, topic, []) end)
    {:ok, []}
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_call(:events, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:succeeded_events, _from, state) do
    {:reply, state |> Enum.filter(fn
      %CommandSucceededEvent{} -> true
      _other                   -> false
    end), state}
  end

  def handle_call(:failed_events, _from, state) do
    {:reply, state |> Enum.filter(fn
      %CommandFailedEvent{} -> true
      _other                -> false
    end), state}
  end

  def handle_call(:retryable_read_events, _from, state) do
    {:reply, state |> Enum.filter(fn
      %RetryReadEvent{} -> true
      _other            -> false
    end), state}
  end

  def handle_call(:retry_write_events, _from, state) do
    {:reply, state |> Enum.filter(fn
      %RetryWriteEvent{} -> true
      _other             -> false
    end), state}
  end

  def handle_call(:empty_selection_events, _from, state) do
    {:reply, state |> Enum.filter(fn
      %ServerSelectionEmptyEvent{} -> true
      _other                       -> false
    end), state}
  end

  def handle_info({:broadcast, :commands, msg}, state) do
    {:noreply, [msg|state]}
  end
  def handle_info({:broadcast, :topology, %ServerSelectionEmptyEvent{} = msg}, state) do
    {:noreply, [msg|state]}
  end
  def handle_info(_ignored, state) do
    {:noreply, state}
  end
end
