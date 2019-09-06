defmodule Mongo.SessionTest do
  use MongoTest.Case

  alias Mongo.InsertOneResult
  alias Mongo.Session.SessionPool
  alias Mongo.Session.ServerSession
  alias Mongo.Session
  alias Mongo.UnorderedBulk
  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite
  alias Mongo.BulkOps
  alias Mongo.BulkWriteResult

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  @tag :mongo_3_6
  test "simple session insert", top do
    coll = unique_name()

    {:ok, session} = Session.start_session(top.pid, :write, [])
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, session: session)
    assert id != nil
    assert :ok == Session.end_session(top.pid, session)

  end

  @tag :mongo_3_6
  test "session pool fifo", top do

    {:ok, session_pool} = SessionPool.start_link(top.pid, 30)

    session_a = SessionPool.checkout(session_pool)
    session_b = SessionPool.checkout(session_pool)

    SessionPool.checkin(session_pool, session_a)
    SessionPool.checkin(session_pool, session_b)

    assert session_b.session_id == SessionPool.checkout(session_pool).session_id
    assert session_a.session_id == SessionPool.checkout(session_pool).session_id
  end

  @tag :mongo_3_6
  test "session pool checkin prune", top do

    {:ok, session_pool} = SessionPool.start_link(top.pid, 1)

    session_a = SessionPool.checkout(session_pool) |> make_old(-2*60)
    session_b = SessionPool.checkout(session_pool) |> make_old(-2*60)

    SessionPool.checkin(session_pool, session_a)
    SessionPool.checkin(session_pool, session_b)

    assert session_b.session_id != SessionPool.checkout(session_pool).session_id
    assert session_a.session_id != SessionPool.checkout(session_pool).session_id
  end

  @tag :mongo_3_6
  test "session pool checkout prune", top do

    {:ok, session_pool} = SessionPool.start_link(top.pid, 2)

    session_a = SessionPool.checkout(session_pool) |> make_old(-59)
    session_b = SessionPool.checkout(session_pool) |> make_old(-59)

    SessionPool.checkin(session_pool, session_a)
    SessionPool.checkin(session_pool, session_b)

    Process.sleep(2000) # force to timeout

    assert session_b.session_id != SessionPool.checkout(session_pool).session_id
    assert session_a.session_id != SessionPool.checkout(session_pool).session_id
  end

  def make_old(%ServerSession{last_use: last_use} = session, delta) do
    %ServerSession{session | last_use: last_use + delta}
  end

  @tag :mongo_3_6
  test "explicit_sessions", top do

    coll = unique_name()

    {:ok, session} = Session.start_session(top.pid, :write, [])
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Waldo"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Tom"}, session: session)
    assert id != nil

    assert %{"name" => "Tom"} = Mongo.find_one(top.pid, coll, %{name: "Tom"}, session: session)

    assert :ok == Session.end_session(top.pid, session)

  end

  @tag :mongo_3_6
  test "abort_transaction", top do

    coll = "dogs"

    Mongo.insert_one(top.pid, coll, %{name: "Wuff"})
    Mongo.delete_many(top.pid, coll, %{})

    {:ok, session} = Session.start_session(top.pid, :write, [])
    assert :ok = Session.start_transaction(session)

    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Waldo"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Tom"}, session: session)
    assert id != nil

    assert %{"name" => "Tom"} = Mongo.find_one(top.pid, coll, %{name: "Tom"}, session: session)
    assert {:ok, 3} == Mongo.count_documents(top.pid, coll, %{}, session: session)
    assert {:ok, 0} == Mongo.count_documents(top.pid, coll, %{})

    {:ok, doc} = Session.abort_transaction(session) ## todo

    assert nil == Mongo.find_one(top.pid, coll, %{name: "Tom"}, session: session)
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

    assert :ok == Session.end_session(top.pid, session)

  end

  @tag :mongo_3_6
  test "with_transaction", top do

    coll = "dogs_with_transaction"

    Mongo.insert_one(top.pid, coll, %{name: "Wuff"})
    Mongo.delete_many(top.pid, coll, %{})

    Session.with_transaction(top.pid, fn opts ->

     {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, opts)
      assert id != nil
      {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Waldo"}, opts)
      assert id != nil
      {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Tom"}, opts)
      assert id != nil
     {:ok, :ok}
    end, w: 1)
    assert {:ok, 3} == Mongo.count(top.pid, coll, %{})
  end

  @tag :mongo_3_6
  test "with_transaction_abort", top do

    coll = "dogs_with_transaction"

    Mongo.insert_one(top.pid, coll, %{name: "Wuff"})
    Mongo.delete_many(top.pid, coll, %{})

    assert :error == Session.with_transaction(top.pid, fn opts ->

      {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, opts)
      assert id != nil
      {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Waldo"}, opts)
      assert id != nil
      {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Tom"}, opts)
      assert id != nil
      :error
    end, w: 1)
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  @tag :mongo_3_6
  test "with_transaction_abort_exception", top do

    coll = "dogs_with_transaction"

    Mongo.insert_one(top.pid, coll, %{name: "Wuff"})
    Mongo.delete_many(top.pid, coll, %{})

    assert {:error, %ArgumentError{message: "test"}} == Session.with_transaction(top.pid, fn opts ->

     {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Greta"}, opts)
     assert id != nil
     {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Waldo"}, opts)
     assert id != nil
     {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top.pid, coll, %{name: "Tom"}, opts)
     assert id != nil

     raise(ArgumentError, "test")

    end, w: 1)

    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "check unordered bulk with transaction", top do

    coll = unique_name()
    Mongo.insert_one(top.pid, coll, %{name: "Wuff"})
    Mongo.delete_many(top.pid, coll, %{})

    bulk = coll
           |> UnorderedBulk.new()
           |> UnorderedBulk.insert_one(%{name: "Greta"})
           |> UnorderedBulk.insert_one(%{name: "Tom"})
           |> UnorderedBulk.insert_one(%{name: "Waldo"})
           |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
           |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
           |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
           |> UnorderedBulk.delete_one(%{kind: "dog"})
           |> UnorderedBulk.delete_one(%{kind: "dog"})
           |> UnorderedBulk.delete_one(%{kind: "dog"})

    {:ok, result} = Session.with_transaction(top.pid, fn opts ->
      {:ok, BulkWrite.write(top.pid, bulk, opts)}
    end, w: 1)

    assert %{:inserted_count => 3, :matched_count => 3, :deleted_count => 3 } ==  Map.take(result, [:inserted_count, :matched_count, :deleted_count])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

  test "check invalid unordered bulk with transaction", top do

    coll = unique_name()

    bulk = coll
           |> UnorderedBulk.new()
           |> UnorderedBulk.insert_one(%{name: "Greta"})
           |> UnorderedBulk.insert_one(%{name: "Tom"})
           |> UnorderedBulk.insert_one(%{name: "Waldo"})
           |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
           |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
           |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})

    {:error, [result|xs]} = Session.with_transaction(top.pid, fn opts ->

      %BulkWriteResult{errors: errors} = result = BulkWrite.write(top.pid, bulk, opts)

      case Enum.empty?(errors) do
         true  -> {:ok, result}
         false -> {:error, errors}
      end

    end, w: 1)

    assert 263  == result["code"]
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

end