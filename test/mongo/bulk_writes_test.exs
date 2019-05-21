defmodule Mongo.BulkWritesTest do
  use MongoTest.Case

  alias Mongo.UnorderedBulk
  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite
  alias Mongo.BulkOps

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  test "check unordered bulk", top do
    coll = unique_name()

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

    result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:insertedCount => 3, :matchedCount => 3, :deletedCount => 3 } ==  Map.take(result, [:insertedCount, :matchedCount, :deletedCount])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

  test "check ordered bulk", top do
    coll = unique_name()

    bulk = coll
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

    result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:insertedCount => 3, :matchedCount => 6, :deletedCount => 3 } ==  Map.take(result, [:insertedCount, :matchedCount, :deletedCount])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

  test "check ordered bulk with stream and a buffer of 25 operations", top do
    coll = unique_name()

    1..1000
    |> Stream.map(fn
      1    -> BulkOps.get_insert_one(%{count: 1})
      1000 -> BulkOps.get_delete_one(%{count: 999})
      i    -> BulkOps.get_update_one(%{count: i - 1}, %{"$set": %{count: i}})
    end)
    |> OrderedBulk.write(top.pid, coll, 25)
    |> Stream.run()

    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

  test "check unordered bulk upserts", top do
    coll = unique_name()

    bulk = coll
           |> UnorderedBulk.new()
           |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}}, upsert: true)
           |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}}, upsert: true)
           |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true)
           |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true) ## <- this works
           |> UnorderedBulk.delete_one(%{kind: "dog"})
           |> UnorderedBulk.delete_one(%{kind: "dog"})
           |> UnorderedBulk.delete_one(%{kind: "dog"})

    result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:upsertedCount => 3, :matchedCount => 1, :deletedCount => 3} ==  Map.take(result, [:upsertedCount, :matchedCount, :deletedCount])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

  test "check ordered bulk upserts", top do
    coll = unique_name()

    bulk = coll
           |> OrderedBulk.new()
           |> OrderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}}, upsert: true)
           |> OrderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}}, upsert: true)
           |> OrderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true)
           |> OrderedBulk.update_one(%{name: "Greta"}, %{"$set": %{color: "brown"}}) ## first match + modified
           |> OrderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true) ## second match
           |> OrderedBulk.delete_one(%{kind: "dog"})
           |> OrderedBulk.delete_one(%{kind: "dog"})
           |> OrderedBulk.delete_one(%{kind: "dog"})

    result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:upsertedCount => 3, :matchedCount => 2, :deletedCount => 3, :modifiedCount => 1} ==  Map.take(result, [:upsertedCount, :matchedCount, :deletedCount, :modifiedCount])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end

end