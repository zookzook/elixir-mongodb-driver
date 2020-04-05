defmodule XEventHandler do
  @moduledoc false

  require Logger

  def start() do
    Logger.info("Starting EventHandler")
    spawn(__MODULE__, :listen, [])
  end

  def listen() do

    receive do
      {:boardcast, message} ->
        Logger.info("Received message:" <> (inspect message))
        listen()
        _other ->
        Logger.info("Stopping EventHandler")
    end
  end

end
