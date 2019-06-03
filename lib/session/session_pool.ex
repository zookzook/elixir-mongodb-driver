defmodule Mongo.Session.SessionPool do
  @moduledoc """

  A FIFO cache for sessions. To get a new session, call `checkout`. This returns a new session or a cached session.
  After running the operation call `checkin(session)` to put the session into the FIFO cache for reuse.

  The MongoDB specifications allows to generate the uuid from the client. That means, that we can
  just create server sessions and use them for logicial sessions. If they expire then we drop these sessions,
  otherwise we can reuse the server sessions.
  """

  alias Mongo.Session.ServerSession

  use GenServer

  @me __MODULE__

  @doc """
  Starts the GenServer. The `logical_session_timeout` is the timeout in minutes for each server session.
  """
  @spec start_link(GenServer.server, integer) :: GenServer.on_start()
  def start_link(top, logical_session_timeout) do

    state = %{
      top: top,
      timeout: logical_session_timeout,
      queue: []
    }

    GenServer.start_link(__MODULE__, state, name: @me)
  end

  @doc """
  Return a server session. If the session timeout is not reached, then a cached server session is return for reuse.
  Otherwise a newly created server session is returned.
  """
  @spec checkout() :: ServerSession.t
  def checkout() do
    GenServer.call(@me, :checkout)
  end

  @doc """
  Checkin a used server session. It if is already expired, the server session is dropped. Otherwise the server session
  is cache for reuse, until it expires due of being cached all the time.
  """
  @spec checkin(ServerSession.t) :: none()
  def checkin(session) do
    GenServer.cast(@me, {:checkin, session})
  end

  @doc """
  Initiaize an empty cache.
  """
  def init(state) do
    {:ok, state}
  end

  @doc """
  Handle a checkin cast.
  """
  def handle_cast({:checkin, session}, %{queue: queue, timeout: timeout} = state) do

    queue = prune(queue, timeout)

    case ServerSession.about_to_expire?(session, timeout) do
      true  -> {:noreply, %{state | queue: queue}}
      false -> {:noreply, %{state | queue: [session | queue]}}
    end

  end

  @doc """
  Handle a shutdown cast.
  """
  def handle_cast(:shutdown, state) do
    end_sessions(state)
    {:noreply, %{state | queue: []}}
  end

  @doc """
  Handle a checkout call.
  """
  def handle_call(:checkout, _from, %{queue: queue, timeout: timeout} = state) do
    {session, queue} = find_session(queue, timeout)
    {:reply, session, %{state | queue: queue}}
  end

  ##
  # Send a end_sessions command to the server
  #
  defp end_sessions(%{top: top, queue: queue}) do
    queue
    |> Enum.chunk_every(10_000)
    |> Enum.each(fn sessions -> end_sessions(top, sessions) end)
  end

  defp end_sessions(top, sessions) do
    with {:ok, %{"ok" => ok}} when ok == 1 <- Mongo.command(top, [endSessions: sessions], database: "admin") do
      :ok
    end
  end

  ##
  # remove all old sessions
  #
  defp prune(sessions, timeout), do: Enum.reject(sessions, fn session -> ServerSession.about_to_expire?(session, timeout) end)

  ##
  # find the next valid sessions and removes all sessions that timed out
  #
  defp find_session([], _timeout), do: {ServerSession.new(), []}
  defp find_session([session | rest], timeout) do
    case ServerSession.about_to_expire?(session, timeout) do
      true  -> find_session(rest, timeout)
      false -> {session, rest}
    end
  end

end