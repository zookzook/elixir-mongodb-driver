defmodule Mongo.EventHandler do
  @moduledoc false

  require Logger

  @all [:commands, :is_master, :topology]

  def start(opts \\ [:commands]) do
    Logger.info("Starting EventHandler")
    spawn(__MODULE__, :register, [opts])
  end

  def register([]) do
    register(@all)
    with {:ok, _} <- Registry.register(:events_registry, :topology, []),
         {:ok, _} <- Registry.register(:events_registry, :commands, []) do
      listen()
    end
  end

  def register(opts) do
    with true <- opts
                |> Enum.map(fn topic -> Registry.register(:events_registry, topic, []) end)
                |> Enum.all?(fn
                  {:ok, _} -> true
                  _other   -> false
                end) do
      listen()
      :ok
    end
  end

  def listen() do
    # fun.(topic, message)
    receive do
      {:broadcast, topic, message} ->
        Logger.info("Received #{topic}-message:" <> (inspect message))
        listen()
      other ->
        Logger.info("Stopping EventHandler received unknown message:" <> inspect other)
    end
  end

end
