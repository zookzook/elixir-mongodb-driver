defmodule Mongo.BulkWritesTest do
  use MongoTest.Case

  alias Mongo.UnorderedBulk
  alias Mongo.OrderedBulk
  alias Mongo.BulkWrite

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

    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})

  end


end