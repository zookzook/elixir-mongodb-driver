defmodule Mongo.SessionTest do
  use MongoTest.Case

  alias Mongo.InsertOneResult
  alias Mongo.Session.SessionPool
  alias Mongo.Session.ServerSession

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  @tag :mongo_3_6
  test "simple session insert", top do
    coll = unique_name()

    sessionId = Mongo.start_session(top.pid)
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, lsid: sessionId)
    assert id != nil
    assert :ok == Mongo.end_sessions(top.pid, [sessionId])

  end

  @tag :mongo_3_6
  test "session pool fifo", top do

    SessionPool.start_link(top.pid, 30)

    session_a = SessionPool.checkout()
    session_b = SessionPool.checkout()

    SessionPool.checkin(session_a)
    SessionPool.checkin(session_b)

    assert session_b.session_id == SessionPool.checkout().session_id
    assert session_a.session_id == SessionPool.checkout().session_id
  end

  @tag :mongo_3_6
  test "session pool checkin prune", top do

    SessionPool.start_link(top.pid, 1)

    session_a = SessionPool.checkout() |> make_old(-2*60)
    session_b = SessionPool.checkout() |> make_old(-2*60)

    SessionPool.checkin(session_a)
    SessionPool.checkin(session_b)

    assert session_b.session_id != SessionPool.checkout().session_id
    assert session_a.session_id != SessionPool.checkout().session_id
  end

  @tag :mongo_3_6
  test "session pool checkout prune", top do

    SessionPool.start_link(top.pid, 2)

    session_a = SessionPool.checkout() |> make_old(-59)
    session_b = SessionPool.checkout() |> make_old(-59)

    SessionPool.checkin(session_a)
    SessionPool.checkin(session_b)

    Process.sleep(2000) # force to timeout

    assert session_b.session_id != SessionPool.checkout().session_id
    assert session_a.session_id != SessionPool.checkout().session_id
  end

  def make_old(%ServerSession{last_use: last_use} = session, delta) do
    %ServerSession{session | last_use: last_use + delta}
  end

end