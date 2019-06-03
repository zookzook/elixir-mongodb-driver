defmodule Mongo.Session.ServerSession do
  @moduledoc """
  This module represents the server-side session. There are three fields:

    * `last_use` - The timestamp for the last use of this server session
    * `txn_num` - The current transaction number
    * `session_id` - The session id of this server session

  When a transaction is active, all operations in that transaction
  use the same transaction number.

  Transaction number is also used outside of transactions for
  retryable writes. In this case, each write operation has its own
  transaction number, but retries of a write operation use the same
  transaction number as the first write (which is how the server
  knows that subsequent writes are retries and should be ignored if
  the first write succeeded on the server but was not read by the
  client, for example).
  """

  alias Mongo.Session.ServerSession

  @type t :: %__MODULE__{
               last_use: integer,
               txn_num: non_neg_integer,
               session_id: BSON.Binary.t
             }

  defstruct last_use: 0, txn_num: 0, session_id: nil

  @doc """
  Create a new server session.
  """
  @spec new() :: ServerSession.t
  def new() do
    %ServerSession{session_id: uuid(), last_use: System.system_time(:second)}
  end

  @doc """
  Update the last_use attribute of the server session to now.
  """
  @spec set_last_use(ServerSession.t) :: ServerSession.t
  def set_last_use(%ServerSession{} = session) do
    %ServerSession{session | last_use: System.system_time(:second)}
  end

  @doc """
  Increment the current transaction number and return the new value.
  """
  @spec next_txn_num(ServerSession.t) :: ServerSession.t
  def next_txn_num(%ServerSession{:txn_num => txn_num} = session) do
    %ServerSession{session | txn_num: txn_num + 1}
  end


  @doc """
    Return true, if the server session will time out. In this case the session
    can be removed from the queue.
  """
  @spec about_to_expire?(ServerSession.t, integer) :: boolean
  def about_to_expire?(%ServerSession{:last_use => last_use}, logical_session_timeout) do

    idle_time_minutes = div((System.system_time(:second) - last_use), 60)
    (idle_time_minutes + 1) >= logical_session_timeout

  end

  defp uuid() do
    %BSON.Binary{binary: uuid4(), subtype: :uuid}
  end

  #
  # From https://github.com/zyro/elixir-uuid/blob/master/lib/uuid.ex
  # with modifications:
  #
  # We don't need a string version, so we use the binary directly
  #
  @uuid_v4   4
  @variant10 2

  defp uuid4() do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    <<u0::48, @uuid_v4::4, u1::12, @variant10::2, u2::62>>
  end

  defimpl Inspect, for: ServerSession do

    def inspect(%ServerSession{last_use: last_use, txn_num: txn, session_id: session_id}, _opts) do

      "#ServerSession(" <> inspect(DateTime.from_unix(last_use)) <> ", " <> to_string(txn) <> ", session_id: " <> (inspect session_id) <> ")"
    end

  end
end