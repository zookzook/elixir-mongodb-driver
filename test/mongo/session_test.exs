defmodule Mongo.SessionTest do
  use MongoTest.Case

  alias Mongo.InsertOneResult
  alias Mongo.Session.SessionPool
  alias Mongo.Session.ServerSession
  alias Mongo.Session
  alias Mongo.UnorderedBulk
  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite
  alias Mongo.BulkWriteResult
  alias Mongo.BulkOps

  setup_all do
    assert {:ok, top} = Mongo.TestConnection.connect()
    {:ok, %{top: top}}
  end

  @tag :mongo_3_6
  test "simple session insert", %{top: top} do
    coll = unique_collection()

    {:ok, session} = Session.start_session(top, :write, [])
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)
    assert id != nil
    assert :ok == Session.end_session(top, session)
  end

  @tag :mongo_3_6
  test "session pool fifo", _ do
    session_pool = SessionPool.new(30)

    {session_a, session_pool} = SessionPool.checkout(session_pool)
    {session_b, session_pool} = SessionPool.checkout(session_pool)

    session_pool = SessionPool.checkin(session_pool, session_a)
    session_pool = SessionPool.checkin(session_pool, session_b)

    {session_bb, session_pool} = SessionPool.checkout(session_pool)
    {session_aa, _session_pool} = SessionPool.checkout(session_pool)

    assert session_b.session_id == session_bb.session_id
    assert session_a.session_id == session_aa.session_id
  end

  @tag :mongo_3_6
  test "session pool checkin prune", _ do
    session_pool = SessionPool.new(1)

    {session_a, session_pool} = SessionPool.checkout(session_pool)
    {session_b, session_pool} = SessionPool.checkout(session_pool)

    session_a = session_a |> make_old(-2 * 60)
    session_b = session_b |> make_old(-2 * 60)

    session_pool = SessionPool.checkin(session_pool, session_a)
    session_pool = SessionPool.checkin(session_pool, session_b)

    {session_bb, session_pool} = SessionPool.checkout(session_pool)
    {session_aa, _session_pool} = SessionPool.checkout(session_pool)

    assert session_b.session_id != session_bb.session_id
    assert session_a.session_id != session_aa.session_id
  end

  @tag :mongo_3_6
  test "session pool checkout prune", _ do
    session_pool = SessionPool.new(2)

    {session_a, session_pool} = SessionPool.checkout(session_pool)
    {session_b, session_pool} = SessionPool.checkout(session_pool)

    session_a = session_a |> make_old(-59)
    session_b = session_b |> make_old(-59)

    session_pool = SessionPool.checkin(session_pool, session_a)
    session_pool = SessionPool.checkin(session_pool, session_b)

    # force to timeout
    Process.sleep(2000)

    {session_bb, session_pool} = SessionPool.checkout(session_pool)
    {session_aa, _session_pool} = SessionPool.checkout(session_pool)

    assert session_b.session_id != session_bb.session_id
    assert session_a.session_id != session_aa.session_id
  end

  def make_old(%ServerSession{last_use: last_use} = session, delta) do
    %ServerSession{session | last_use: last_use + delta}
  end

  @tag :mongo_3_6
  test "explicit_sessions", %{top: top} do
    coll = unique_collection()

    {:ok, session} = Session.start_session(top, :write, [])
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"}, session: session)
    assert id != nil

    assert %{"name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session)

    assert :ok == Session.end_session(top, session)
  end

  @tag :mongo_4_2
  test "commit_transaction", %{top: top} do
    coll = "dogs"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    {:ok, session} = Session.start_session(top, :write, [])
    assert :ok = Session.start_transaction(session)

    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"}, session: session)
    assert id != nil

    assert %{"name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session)
    assert {:ok, 3} == Mongo.count_documents(top, coll, %{}, session: session)
    assert {:ok, 0} == Mongo.count_documents(top, coll, %{})

    :ok = Session.commit_transaction(session)

    %{"_id" => _id, "name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session)
    assert {:ok, 3} == Mongo.count(top, coll, %{})

    assert :ok == Session.end_session(top, session)
  end

  @tag :mongo_4_2
  test "commit_transaction on multiple database", %{top: top} do
    coll = "dogs"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})
    Mongo.insert_one(top, coll, %{name: "Wuff"}, database: "mongodb_test_2")
    Mongo.delete_many(top, coll, %{}, database: "mongodb_test_2")

    {:ok, session} = Session.start_session(top, :write, [])
    assert :ok = Session.start_transaction(session)

    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"}, session: session)
    assert id != nil

    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session, database: "mongodb_test_2")
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"}, session: session, database: "mongodb_test_2")
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"}, session: session, database: "mongodb_test_2")
    assert id != nil

    assert %{"name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session)
    assert {:ok, 3} == Mongo.count_documents(top, coll, %{}, session: session)
    assert {:ok, 0} == Mongo.count_documents(top, coll, %{})

    assert %{"name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session, database: "mongodb_test_2")
    assert {:ok, 3} == Mongo.count_documents(top, coll, %{}, session: session, database: "mongodb_test_2")
    assert {:ok, 0} == Mongo.count_documents(top, coll, %{}, database: "mongodb_test_2")

    :ok = Session.commit_transaction(session)

    %{"_id" => _id, "name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session)
    assert {:ok, 3} == Mongo.count(top, coll, %{})
    assert {:ok, 3} == Mongo.count(top, coll, %{}, session: session)

    assert :ok == Session.end_session(top, session)
  end

  @tag :mongo_4_2
  test "abort_transaction", %{top: top} do
    coll = "dogs"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    {:ok, session} = Session.start_session(top, :write, [])
    assert :ok = Session.start_transaction(session)

    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"}, session: session)
    assert id != nil
    {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"}, session: session)
    assert id != nil

    assert %{"name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"}, session: session)
    assert {:ok, 3} == Mongo.count_documents(top, coll, %{}, session: session)
    assert {:ok, 0} == Mongo.count_documents(top, coll, %{})

    :ok = Session.abort_transaction(session)

    assert nil == Mongo.find_one(top, coll, %{name: "Tom"}, session: session)
    assert {:ok, 0} == Mongo.count(top, coll, %{})

    assert :ok == Session.end_session(top, session)
  end

  @tag :mongo_4_2
  test "transaction", %{top: top} do
    coll = "dogs_with_commit_transaction"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    Mongo.transaction(
      top,
      fn ->
        {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"})
        assert id != nil
        {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"})
        assert id != nil
        {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"})
        assert id != nil
        {:ok, :ok}
      end,
      w: 1
    )

    assert {:ok, 3} == Mongo.count(top, coll, %{})
  end

  @tag :mongo_4_2
  test "transaction_causal_consistency", %{top: top} do
    coll = "dogs_with_commit_transaction_causal_consistency"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    Mongo.transaction(
      top,
      fn ->
        {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"})
        assert id != nil
        {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"})
        assert id != nil
        {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"})
        assert id != nil
        {:ok, :ok}
      end,
      w: 1,
      causal_consistency: true
    )

    assert {:ok, 3} == Mongo.count(top, coll, %{})
  end

  @tag :mongo_4_2
  test "transaction_abort", %{top: top} do
    coll = "dogs_with_about_transaction"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    assert :error ==
             Mongo.transaction(
               top,
               fn ->
                 {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"})
                 assert id != nil
                 {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"})
                 assert id != nil
                 {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"})
                 assert id != nil
                 :error
               end,
               w: 1
             )

    assert {:ok, 0} == Mongo.count(top, coll, %{})
  end

  @tag :mongo_4_2
  test "transaction_abort_exception", %{top: top} do
    coll = "dogs_with_transaction_abort_exception"

    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    assert {:error, %ArgumentError{message: "test"}} ==
             Mongo.transaction(
               top,
               fn ->
                 {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Greta"})
                 assert id != nil
                 {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Waldo"})
                 assert id != nil
                 {:ok, %InsertOneResult{:inserted_id => id}} = Mongo.insert_one(top, coll, %{name: "Tom"})
                 assert id != nil

                 raise(ArgumentError, "test")
               end,
               w: 1
             )

    assert {:ok, 0} == Mongo.count(top, coll, %{})
  end

  @tag :mongo_4_2
  test "check unordered bulk with transaction", %{top: top} do
    coll = unique_collection()
    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    bulk =
      coll
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

    {:ok, result} =
      Mongo.transaction(
        top,
        fn ->
          {:ok, BulkWrite.write(top, bulk)}
        end,
        w: 1
      )

    assert %{:inserted_count => 3, :matched_count => 3, :deleted_count => 3} == Map.take(result, [:inserted_count, :matched_count, :deleted_count])
    assert {:ok, 0} == Mongo.count(top, coll, %{})
  end

  @tag :rs_required
  test "check invalid unordered bulk with transaction", %{top: top} do
    coll = unique_collection()

    bulk =
      coll
      |> UnorderedBulk.new()
      |> UnorderedBulk.insert_one(%{name: "Greta"})
      |> UnorderedBulk.insert_one(%{name: "Tom"})
      |> UnorderedBulk.insert_one(%{name: "Waldo"})
      |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 3, failCommands: ["insert"]]
    ]

    assert {:ok, _} = Mongo.admin_command(top, cmd)

    {:error, [result | _xs]} =
      Mongo.transaction(
        top,
        fn ->
          %BulkWriteResult{errors: errors} = result = BulkWrite.write(top, bulk)

          case Enum.empty?(errors) do
            true -> {:ok, result}
            false -> {:error, errors}
          end
        end,
        w: 1
      )

    assert 3 == result.code
    assert {:ok, 0} == Mongo.count(top, coll, %{})
  end

  @tag :mongo_4_2
  test "check streaming bulk with transaction", %{top: top} do
    coll = unique_collection()
    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    assert {:ok, :ok} =
             Mongo.transaction(
               top,
               fn ->
                 1..1000
                 |> Stream.map(fn
                   1 -> BulkOps.get_insert_one(%{count: 1})
                   1000 -> BulkOps.get_delete_one(%{count: 999})
                   i -> BulkOps.get_update_one(%{count: i - 1}, %{"$set": %{count: i}})
                 end)
                 |> OrderedBulk.write(top, coll, 25)
                 |> Stream.run()

                 {:ok, :ok}
               end,
               w: 1
             )
  end

  @tag :mongo_4_2
  test "commit empty transaction", %{top: top} do
    assert :ok =
             Mongo.transaction(
               top,
               fn _opts ->
                 :ok
               end,
               w: 1
             )
  end

  @tag :mongo_4_2
  test "abort empty transaction", %{top: top} do
    assert :error =
             Mongo.transaction(
               top,
               fn _opts ->
                 :error
               end,
               w: 1
             )
  end

  @tag :mongo_4_2
  test "check ordered bulk with transaction", %{top: top} do
    coll = unique_collection()
    Mongo.insert_one(top, coll, %{name: "Wuff"})
    Mongo.delete_many(top, coll, %{})

    bulk =
      coll
      |> OrderedBulk.new()
      |> OrderedBulk.insert_one(%{name: "Greta"})
      |> OrderedBulk.insert_one(%{name: "Tom"})
      |> OrderedBulk.insert_one(%{name: "Waldo"})
      |> OrderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
      |> OrderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
      |> OrderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}})
      |> OrderedBulk.update_many(%{kind: "dog"}, %{"$set": %{kind: "cat"}})
      |> OrderedBulk.delete_one(%{kind: "cat"})
      |> OrderedBulk.delete_one(%{kind: "cat"})
      |> OrderedBulk.delete_one(%{kind: "cat"})

    {:ok, result} =
      Mongo.transaction(
        top,
        fn ->
          result = BulkWrite.write(top, bulk)
          {:ok, result}
        end,
        w: 1
      )

    assert %{:inserted_count => 3, :matched_count => 6, :deleted_count => 3} == Map.take(result, [:inserted_count, :matched_count, :deleted_count])
    assert {:ok, 0} == Mongo.count(top, coll, %{})
  end
end
