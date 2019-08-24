defmodule Mongo.Session do

  @moduledoc """

    see https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#committransaction


    see https://andrealeopardi.com/posts/connection-managers-with-gen_statem/
  """

  @behaviour :gen_statem

  import Keywords

  alias Mongo.Session.ServerSession
  alias Mongo.Session
  alias Mongo.Topology

  require Logger

  @type t :: pid()

  ##
  # The data:
  # * `conn` the used connection to the database
  # * `server_session` the server_session data
  # * `opts` options
  # * `implicit` true or false
  defstruct [conn: nil, server_session: nil, implicit: false, opts: []]

  @impl true
  def callback_mode() do
    :handle_event_function
  end

  @doc """
  Start the generic state machine.
  """
  @spec start_link(GenServer.server, ServerSession.t, atom, keyword()) :: {:ok, Session.t} | :ignore | {:error, term()}
  def start_link(conn, server_session, type, opts) do
    :gen_statem.start_link(__MODULE__, {conn, server_session, type, opts}, [])
  end

  @doc """
  Start a new transation.
  """
  @spec start_transaction(Session.t) :: :ok | {:error, term()}
  def start_transaction(pid) do
    :gen_statem.call(pid, {:start_transaction})
  end

  @doc """
  Start a new session
  """
  def start_session(topology_pid, type, opts) do
    with {:ok, session} <- Topology.checkout_session(topology_pid, type, :explicit, opts) do
      {:ok, session}
    else
      {:new_connection, _server} -> start_session(topology_pid, type, opts)
    end
  end

  @doc """
  Start a new implicit session only if no explicit session exists.
  """
  def start_implicit_session(topology_pid, type, opts) do
    case Keyword.get(opts, :session, nil) do
       nil ->
         with {:ok, session} <- Topology.checkout_session(topology_pid, type, :implicit, opts) do
           {:ok, session}
         else
           {:new_connection, _server} -> start_implicit_session(topology_pid, type, opts)
         end
       session -> {:ok, session}
    end
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

  def end_implict_session(topology_pid, session) do
    with {:ok, session_server} <- :gen_statem.call(session, {:end_implicit_session}) do
      Topology.checkin_session(topology_pid, session_server)
    else
      :noop -> :ok
      _     -> :error
    end
  end

  def end_session(topology_pid, session) do
    with {:ok, session_server} <- :gen_statem.call(session, {:end_session}) do
      Topology.checkin_session(topology_pid, session_server)
    end
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
  def init({conn, server_session, type, opts}) do
    data = %Session{conn: conn,
      server_session: server_session,
      implicit: (type == :implicit), opts: opts}
    {:ok, :no_transaction, data}
  end

  @impl true
  def handle_event({:call, from}, {:start_transaction}, state, %Session{server_session: session} = data) when state in [:no_transaction, :transaction_aborted, :transaction_committed] do
    {:next_state, :starting_transaction, %Session{data | server_session: ServerSession.next_txn_num(session)}, {:reply, from, :ok}}
  end
  def handle_event({:call, from}, {:bind_session, cmd}, :no_transaction, %Session{conn: conn, server_session: %ServerSession{session_id: id}}) do
    {:keep_state_and_data, {:reply, from, {:ok, conn, Keyword.merge(cmd, lsid: %{id: id})}}}
  end
  def handle_event({:call, from}, {:bind_session, cmd}, :starting_transaction, %Session{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}, opts: opts} = data) do
    result = Keyword.merge(cmd,
                           readConcern: Keyword.get(opts, :read_concern),
                           lsid: %{id: id},
                           txnNumber: %BSON.LongNumber{value: txn_num},
                           startTransaction: true,
                           autocommit: false) |> filter_nils()
    {:next_state, :transaction_in_progress, data, {:reply, from, {:ok, conn, result}}}
  end
  def handle_event({:call, from}, {:bind_session, cmd}, :transaction_in_progress, %Session{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}}) do
    result = Keyword.merge(cmd,
                           lsid: %{id: id},
                           txnNumber: %BSON.LongNumber{value: txn_num},
                           autocommit: false)
    {:keep_state_and_data, {:reply, from, {:ok, conn, result}}}
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
  def handle_event({:call, from}, {:end_session}, _state, %Session{server_session: session_server}) do
    {:stop_and_reply, :normal, {:reply, from, {:ok, session_server}}}
  end
  def handle_event({:call, from}, {:end_implicit_session}, _state, %Session{server_session: session_server, implicit: true}) do
    {:stop_and_reply, :normal, {:reply, from, {:ok, session_server}}}
  end
  def handle_event({:call, from}, {:end_implicit_session}, _state, %Session{server_session: session_server, implicit: false}) do
    {:keep_state_and_data, {:reply, from, :noop}}
  end

  def handle_event({:call, from}, {:server_session}, _state,  %Session{server_session: session_server, implicit: implicit}) do
    {:keep_state_and_data, {:reply, from, {:ok, session_server, implicit}}}
  end

  @impl true
  def terminate(reason, state, data) when state in [:transaction_in_progress] do
    Logger.debug("Terminating because of #{inspect reason}")
    run_abort_command(data)
  end
  def terminate(reason, _state, _data) do
    Logger.debug("Terminating because of #{inspect reason}")
  end

  defp run_commit_command(%{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}, opts: opts}) do

    Logger.debug("Running commit transaction")

    #{
    #    recoveryToken : {...}
    #}

    cmd = [
      commitTransaction: 1,
      lsid: %{id: id},
      txnNumber: %BSON.LongNumber{value: txn_num},
      autocommit: false,
      writeConcern: Keyword.get(opts, :write_concern),
      maxTimeMS: Keyword.get(opts, :max_commit_time_ms)
      ] |> filter_nils()

    Mongo.exec_command(conn, cmd, database: "admin")
  end

  defp run_abort_command(%{conn: conn, server_session: %ServerSession{session_id: id, txn_num: txn_num}, opts: opts}) do

    Logger.debug("Running abort transaction")
    cmd = [
      abortTransaction: 1,
      lsid: %{id: id},
      txnNumber: %BSON.LongNumber{value: txn_num},
      autocommit: false,
      writeConcern: Keyword.get(opts, :write_concern),
    ] |> filter_nils()

    Mongo.exec_command(conn, cmd, database: "admin")
  end


end