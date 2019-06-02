defmodule Mongo.SessionTest do
  use MongoTest.Case

  alias Mongo.InsertOneResult

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  test "simple session insert", top do
    coll = unique_name()

    sessionId = Mongo.start_session(top.pid)
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, lsid: sessionId)
    assert id != nil
    assert :ok == Mongo.end_sessions(top.pid, [sessionId])

  end
end