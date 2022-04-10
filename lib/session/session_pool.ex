defmodule Mongo.Session.SessionPool do
  @moduledoc """

  A FIFO cache for sessions. To get a new session, call `checkout`. This returns a new session or a cached session.
  After running the operation call `checkin(session)` to put the session into the FIFO cache for reuse.

  The MongoDB specifications allows to generate the uuid from the client. That means, that we can
  just create server sessions and use them for logicial sessions. If they expire then we drop these sessions,
  otherwise we can reuse the server sessions.
  """

  alias Mongo.Session.ServerSession

  @type session_pool() :: %{:pool_size => any, :queue => [ServerSession.t()], :timeout => any, optional(any) => any}

  def new(logical_session_timeout, opts \\ []) do
    pool_size = Keyword.get(opts, :session_pool, 1000)

    %{
      timeout: logical_session_timeout * 60 - 60,
      queue: Enum.map(1..pool_size, fn _ -> ServerSession.new() end),
      pool_size: pool_size
    }
  end

  @doc """
  Return a server session. If the session timeout is not reached, then a cached server session is return for reuse.
  Otherwise a newly created server session is returned.
  """
  @spec checkout(session_pool()) :: {ServerSession.t(), session_pool()}
  @compile {:inline, checkout: 1}
  def checkout(%{queue: queue, timeout: timeout, pool_size: size} = pool) do
    {session, queue} = find_session(queue, timeout, size)
    {session, %{pool | queue: queue}}
  end

  @doc """
  Checkin a used server session. It if is already expired, the server session is dropped. Otherwise the server session
  is cache for reuse, until it expires due of being cached all the time.
  """
  @spec checkin(session_pool(), ServerSession.t()) :: session_pool()
  @compile {:inline, checkin: 2}
  def checkin(%{queue: queue, timeout: timeout} = pool, session) do
    case ServerSession.about_to_expire?(session, timeout) do
      true -> %{pool | queue: queue}
      false -> %{pool | queue: [session | queue]}
    end
  end

  ##
  # remove all old sessions, dead code
  #
  # def prune(%{queue: queue, timeout: timeout} = pool) do
  #  queue = Enum.reject(queue, fn session -> ServerSession.about_to_expire?(session, timeout) end)
  #  %{pool | queue: queue}
  # end

  ##
  # find the next valid sessions and removes all sessions that timed out
  #
  @compile {:inline, find_session: 3}
  defp find_session([], _timeout, size) do
    {ServerSession.new(), Enum.map(1..size, fn _ -> ServerSession.new() end)}
  end

  defp find_session([session | rest], timeout, size) do
    case ServerSession.about_to_expire?(session, timeout) do
      true -> find_session(rest, timeout, size)
      false -> {session, rest}
    end
  end
end
