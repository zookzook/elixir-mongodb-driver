defmodule ChangeStream do
  use GenServer

  require Logger

  @collection "http_errors"
  @me __MODULE__

  def start_link() do
    GenServer.start_link(__MODULE__, nil, name: @me)
  end

  def new_token(token) do
    GenServer.cast(@me, {:token, token})
  end

  def new_doc(doc) do
    GenServer.cast(@me, {:doc, doc})
  end

  def init(_) do
    state = %{last_resume_token: nil}
    Process.send_after(self(), :connect, 3000)
    {:ok, state}
  end

  def handle_info({:DOWN, _, :process, _pid, reason}, state) do
    Logger.info("#Cursor process is down: #{inspect reason}")
    Process.send_after(self(), :connect, 3000)
    {:noreply, state}
  end

  def handle_info(:connect, state) do
    Logger.info("Connecting change stream")
    # Span a new process
    pid = spawn(fn -> Enum.each(get_cursor(state), fn doc -> new_doc(doc) end) end)

    # Monitor the process
    Process.monitor(pid)

    {:noreply, state}
  end

  def handle_cast({:doc, doc}, state) do
    Logger.info("Receiving new document #{inspect doc["ns"]}")
    process_doc(doc)
    {:noreply, state}
  end

  def handle_cast({:token, token}, state) do
    Logger.info("Receiving new token #{inspect token}")
    {:noreply, %{state | last_resume_token: token}}
  end

  defp process_doc(%{"fullDocument" => %{"url" => url}, "ns" => %{"coll" => "http_errors", "db" => "db-1"}}) do
    Logger.info("Got http error for url #{url}")
  end

  defp get_cursor(%{last_resume_token: nil}) do
    Mongo.watch_collection(:mongo, @collection, [], fn token -> new_token(token) end, max_time: 2_000)
  end
  defp get_cursor(%{last_resume_token: token}) do
    Mongo.watch_collection(:mongo, @collection, [], fn token -> new_token(token) end, max_time: 2_000, resume_after: token)
  end

end