defmodule Mongo.EventHandler do
  @moduledoc false

  require Logger

  @all [:commands, :topology]

  def start(opts \\ [topics: [:commands]]) do
    spawn(__MODULE__, :register, [opts])
  end

  def register(opts) do
    with true <-
           (opts[:topics] || @all)
           |> Enum.map(fn topic -> Registry.register(:events_registry, topic, []) end)
           |> Enum.all?(fn
             {:ok, _} -> true
             _other -> false
           end) do
      listen(opts)
      :ok
    end
  end

  def listen(opts) do
    receive do
      {:broadcast, :commands, %{command_name: cmd} = message} when cmd != :isMaster and cmd != :hello ->
        Logger.info("Received command: " <> inspect(message))
        listen(opts)

      {:broadcast, :commands, hello} ->
        case opts[:is_master] || opts[:hello] do
          true -> Logger.info("Received hello:" <> inspect(hello))
          _ -> []
        end

        listen(opts)

      {:broadcast, topic, message} ->
        Logger.info("Received #{topic}: " <> inspect(message))
        listen(opts)

      other ->
        Logger.info("Stopping EventHandler received unknown message:" <> inspect(other))
    end
  end
end
