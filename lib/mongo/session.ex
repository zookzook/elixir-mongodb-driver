defmodule Mongo.Session do

  @moduledoc """

    see https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#committransaction


    see https://andrealeopardi.com/posts/connection-managers-with-gen_statem/
  """

  @behaviour :gen_statem

  alias Mongo.Session.ServerSession
  alias Mongo.Session

  @type t :: pid()

  ##
  # The data:
  # * `conn` the used connection to the database
  # * `server_session` the server_session data
  # * `opts` options
  # * `slave_ok` true or false
  # * `mongos` true or false
  # * `implicit` true or false
  defstruct [conn: nil, server_session: nil, slave_ok: false, mongos: false, implicit: false, opts: []]

  @impl true
  def callback_mode() do
    :handle_event_function
  end

  @doc """
  Start the generic state machine.
  """
  @spec start_link(GenServer.server, ServerSession.t, boolean, boolean, atom, keyword()) :: {:ok, Session.t} | :ignore | {:error, term()}
  def start_link(conn, server_session, slave_ok, mongos, type, opts) do
    :gen_statem.start_link(__MODULE__, {conn, server_session, slave_ok, mongos, type, opts}, [])
  end

  @doc """
  Start a new transation.
  """
  @spec start_transaction(Session.t) :: :ok | {:error, term()}
  def start_transaction(pid) do
    :gen_statem.call(pid, {:start_transaction})
  end

  @doc """
  Commit the current transation
  """
  def commit_transaction(pid) do
    :gen_statem.call(pid, {:commit_transaction})
  end

  @doc """
  Abort the current transation and rollback all updates.
  """
  def abort_transaction(pid) do
    :gen_statem.call(pid, {:abort_transaction})
  end

  @doc """
  Merge the session / transaction data into the cmd.
  """
  def bind_session(nil, cmd) do
    cmd
  end
  def bind_session(pid, cmd) do
    :gen_statem.call(pid, {:bind_session, cmd})
  end

  def end_session(pid) do
    :gen_statem.call(pid, {:end_session})
  end

  def connection(pid) do
    :gen_statem.call(pid, {:connection})
  end

  def server_session(pid) do
    :gen_statem.call(pid, {:server_session})
  end

  def alive?(nil), do: false
  def alive?(pid), do: Process.alive?(pid)

  @impl true
  def init({conn, server_session, slave_ok, mongos, type, opts}) do
    data = %Session{conn: conn, server_session: server_session, slave_ok: slave_ok, mongos: mongos, implicit: (type == :implict), opts: opts}
    {:ok, :no_transaction, data}
  end

  @impl true
  def handle_event({:call, from}, {:start_transaction}, state, %Session{server_session: session} = data) when state in [:no_transaction, :transaction_aborted, :transaction_committed] do
    {:next_state, :starting_transaction, %Session{data | server_session: ServerSession.next_txn_num(session)}, {:reply, from, :ok}}
  end
  def handle_event({:call, from}, {:bind_session, cmd}, :no_transaction, %Session{conn: conn, server_session: %ServerSession{session_id: id}}) do
    {:keep_state_and_data, {:reply, from, conn, Keyword.merge(cmd, lsid: id)}}
  end
  def handle_event({:call, from}, {:bind_session, cmd}, :starting_transaction, %Session{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}} = data) do
    result = Keyword.merge(cmd,
                           lsid: id,
                           txnNumber: txn_num,
                           startTransaction: true,
                           autocommit: false)
    {:next_state, :transaction_in_progress, data, {:reply, from, conn, result}}
  end
  def handle_event({:call, from}, {:bind_session, cmd}, :transaction_in_progress, %Session{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}}) do
    result = Keyword.merge(cmd,
                           lsid: id,
                           txnNumber: txn_num,
                           autocommit: false)
    {:keep_state_and_data, {:reply, from, conn, result}}
  end

  def handle_event({:call, from}, {:commit_transaction}, :transaction_in_progress, data) do
    {:next_state, :transaction_committed, data, {:reply, from, run_commit_command(data)}}
  end
  def handle_event({:call, from}, {:abort_transaction}, :transaction_in_progress, data) do
    {:next_state, :transaction_aborted, data, {:reply, from, run_abort_command(data)}}
  end
  def handle_event({:call, from}, {:connection}, _state,  %{conn: conn}) do
    {:keep_state_and_data, {:reply, from, conn}}
  end
  def handle_event({:call, from}, {:end_session}, _state, _data) do
    {:stop_and_reply, :normal, {:reply, from, :ok}}
  end

  defp run_commit_command(%{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}}) do

    #{
    #    commitTransaction : 1,
    #    lsid : { id : <UUID> },
    #    txnNumber : <Int64>,
    #    autocommit : false,
    #    writeConcern : {...},
    #    maxTimeMS: <Int64>,
    #    recoveryToken : {...}
    #}

    cmd = [
      commitTransaction: 1,
      lsid: id,
      txnNumber: txn_num,
      autocommit: false,
      writeConcern: %{w: 1}
    ]

    Mongo.exec_command(conn, cmd, database: "admin")
  end

  defp run_abort_command(%{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}}) do

    #{
    #    abortTransaction : 1,
    #    lsid : { id : <UUID> },
    #    txnNumber : <Int64>,
    #    autocommit : false,
    #    writeConcern : {...}
    #}

    cmd = [
      abortTransaction: 1,
      lsid: id,
      txnNumber: txn_num,
      autocommit: false,
      writeConcern: %{w: 1}
    ]

    Mongo.exec_command(conn, cmd, database: "admin")
  end

  @impl true
  def terminate(_reason, state, data) when state in [:transaction_in_progress] do
    IO.puts "terminating"
    run_abort_command(data)
  end
  def terminate(_reason, _state, _data) do
    IO.puts "terminating"
  end

  def test() do
#    {:ok, session_pool} = Mongo.Session.SessionPool.start_link(self(), 3_000)
#    ssession = Mongo.Session.SessionPool.checkout(session_pool)
#    {:ok, session} = Mongo.Session.start_link(self(), ssession, [])
#
#    :sys.trace session, true
#    cmd = [
#      insert: "Test",
#      documents: [%{name: "Waldo"}]
#    ]
#
#    cmd = Mongo.Session.bind_session(session, cmd)
#
#
#    Mongo.Session.start_transaction(session)
#
#    cmd = [
#      insert: "Test",
#      documents: [%{name: "Greta"}]
#    ]
#
#    cmd = Mongo.Session.bind_session(session, cmd)
#
#    IO.puts inspect cmd
#
#    cmd = [
#      insert: "Test",
#      documents: [%{name: "Tom"}]
#    ]
#
#    cmd = Mongo.Session.bind_session(session, cmd)
#
#    IO.puts inspect cmd
#
#    IO.puts inspect Mongo.Session.alive?(session)
#
#    IO.puts inspect Mongo.Session.commit_transaction(session)
#    IO.puts inspect Mongo.Session.end_session(session)
#
#    IO.puts inspect Mongo.Session.alive?(session)

  end

end