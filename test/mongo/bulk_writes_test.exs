defmodule Mongo.BulkWritesTest do
  use CollectionCase

  require Logger

  alias Mongo.BulkOps
  alias Mongo.BulkWrite
  alias Mongo.BulkWriteResult
  alias Mongo.OrderedBulk
  alias Mongo.UnorderedBulk

  test "check unordered bulk", top do
    coll = unique_collection()

    bulk =
      coll
      |> UnorderedBulk.new()
      |> UnorderedBulk.insert_one(%{name: "Greta"})
      |> UnorderedBulk.insert_one(%{name: "Tom"})
      |> UnorderedBulk.insert_one(%{name: "Waldo"})
      |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}})
      |> UnorderedBulk.replace_one(%{name: "Waldo"}, %{name: "Bello", kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})

    %BulkWriteResult{} = result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:inserted_count => 3, :matched_count => 3, :deleted_count => 3} == Map.take(result, [:inserted_count, :matched_count, :deleted_count])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "check unordered bulk with limit", top do
    coll = unique_collection()

    [batch_1, batch_2] =
      1..49
      |> Stream.map(fn i -> Mongo.BulkOps.get_insert_one(%{number: i}) end)
      |> Mongo.UnorderedBulk.write(top.pid, coll, 25)
      |> Enum.map(& &1)

    assert %{:inserted_count => 25} == Map.take(batch_1, [:inserted_count])
    assert %{:inserted_count => 24} == Map.take(batch_2, [:inserted_count])
  end

  test "check ordered bulk with limit", top do
    coll = unique_collection()

    [batch_1, batch_2] =
      1..49
      |> Stream.map(fn i -> Mongo.BulkOps.get_insert_one(%{number: i}) end)
      |> Mongo.OrderedBulk.write(top.pid, coll, 25)
      |> Enum.map(& &1)

    assert %{:inserted_count => 25} == Map.take(batch_1, [:inserted_count])
    assert %{:inserted_count => 24} == Map.take(batch_2, [:inserted_count])
  end

  test "check ordered bulk", top do
    coll = unique_collection()

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

    %BulkWriteResult{} = result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:inserted_count => 3, :matched_count => 6, :deleted_count => 3} == Map.take(result, [:inserted_count, :matched_count, :deleted_count])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "check ordered bulk with stream and a buffer of 25 operations", top do
    coll = unique_collection()

    1..1000
    |> Stream.map(fn
      1 -> BulkOps.get_insert_one(%{count: 1})
      1000 -> BulkOps.get_delete_one(%{count: 999})
      i -> BulkOps.get_update_one(%{count: i - 1}, %{"$set": %{count: i}})
    end)
    |> OrderedBulk.write(top.pid, coll, 25)
    |> Stream.run()

    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "check unordered bulk upserts", top do
    coll = unique_collection()

    bulk =
      coll
      |> UnorderedBulk.new()
      |> UnorderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}}, upsert: true)
      |> UnorderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}}, upsert: true)
      |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true)
      ## <- this works
      |> UnorderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true)
      |> UnorderedBulk.delete_one(%{kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})
      |> UnorderedBulk.delete_one(%{kind: "dog"})

    %BulkWriteResult{} = result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:upserted_count => 3, :matched_count => 1, :deleted_count => 3} == Map.take(result, [:upserted_count, :matched_count, :deleted_count])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "check ordered bulk upserts", top do
    coll = unique_collection()

    bulk =
      coll
      |> OrderedBulk.new()
      |> OrderedBulk.update_one(%{name: "Greta"}, %{"$set": %{kind: "dog"}}, upsert: true)
      |> OrderedBulk.update_one(%{name: "Tom"}, %{"$set": %{kind: "dog"}}, upsert: true)
      |> OrderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true)
      ## first match + modified
      |> OrderedBulk.update_one(%{name: "Greta"}, %{"$set": %{color: "brown"}})
      ## second match
      |> OrderedBulk.update_one(%{name: "Waldo"}, %{"$set": %{kind: "dog"}}, upsert: true)
      |> OrderedBulk.delete_one(%{kind: "dog"})
      |> OrderedBulk.delete_one(%{kind: "dog"})
      |> OrderedBulk.delete_one(%{kind: "dog"})

    %BulkWriteResult{} = result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:upserted_count => 3, :matched_count => 2, :deleted_count => 3, :modified_count => 1} == Map.take(result, [:upserted_count, :matched_count, :deleted_count, :modified_count])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "create one small document and one large 16mb document", top do
    coll = unique_collection()
    # 44 bytes for 'key: "big" and v:'
    max_n = 16 * 1024 * 1024 - 44

    a_line_1k = Enum.reduce(1..1_024, "", fn _, acc -> acc <> "A" end)
    a_line_1m = Enum.reduce(1..1_024, "", fn _, acc -> acc <> a_line_1k end)
    a_line_16m = String.slice(Enum.reduce(1..16, "", fn _, acc -> acc <> a_line_1m end), 0..max_n)

    b_line_1k = Enum.reduce(1..1_024, "", fn _, acc -> acc <> "B" end)
    b_line_1m = Enum.reduce(1..1_024, "", fn _, acc -> acc <> b_line_1k end)
    b_line_16m = String.slice(Enum.reduce(1..15, "", fn _, acc -> acc <> b_line_1m end), 0..max_n)

    bulk =
      coll
      |> OrderedBulk.new()
      |> OrderedBulk.insert_one(%{v: a_line_1k, key: "small"})
      |> OrderedBulk.insert_one(%{v: a_line_16m, key: "big"})
      |> OrderedBulk.update_one(%{key: "small"}, %{"$set": %{v: b_line_1k}})
      |> OrderedBulk.update_one(%{key: "big"}, %{"$set": %{v: b_line_16m}})
      |> OrderedBulk.delete_one(%{key: "small"})
      |> OrderedBulk.delete_one(%{key: "big"})

    %BulkWriteResult{} = result = BulkWrite.write(top.pid, bulk, w: 1)

    assert %{:matched_count => 2, :deleted_count => 2, :modified_count => 2} == Map.take(result, [:matched_count, :deleted_count, :modified_count])
    assert {:ok, 0} == Mongo.count(top.pid, coll, %{})
  end

  test "create one small document and one too large document", top do
    coll = unique_collection()
    max_n = 16 * 1024 * 1024

    a_line_1k = Enum.reduce(1..1_024, "", fn _, acc -> acc <> "A" end)
    a_line_1m = Enum.reduce(1..1_024, "", fn _, acc -> acc <> a_line_1k end)
    a_line_16m = String.slice(Enum.reduce(1..16, "", fn _, acc -> acc <> a_line_1m end), 0..max_n)

    b_line_1k = Enum.reduce(1..1_024, "", fn _, acc -> acc <> "B" end)
    b_line_1m = Enum.reduce(1..1_024, "", fn _, acc -> acc <> b_line_1k end)
    b_line_16m = String.slice(Enum.reduce(1..15, "", fn _, acc -> acc <> b_line_1m end), 0..max_n)

    bulk =
      coll
      |> OrderedBulk.new()
      |> OrderedBulk.insert_one(%{v: a_line_1k, key: "small"})
      |> OrderedBulk.insert_one(%{v: a_line_16m, key: "big"})
      |> OrderedBulk.update_one(%{key: "small"}, %{"$set": %{v: b_line_1k}})
      |> OrderedBulk.update_one(%{key: "big"}, %{"$set": %{v: b_line_16m}})
      |> OrderedBulk.delete_one(%{key: "small"})
      |> OrderedBulk.delete_one(%{key: "big"})

    %BulkWriteResult{errors: [%{"code" => code}]} = result = BulkWrite.write(top.pid, bulk, w: 1)

    assert code in [10_334, 2]
    assert %{:matched_count => 0, :deleted_count => 0, :modified_count => 0} == Map.take(result, [:matched_count, :deleted_count, :modified_count])
    assert {:ok, 1} == Mongo.count(top.pid, coll, %{})
  end

  test "stop when any operation fails: ordered", top do
    coll = unique_collection()

    bulk =
      coll
      |> OrderedBulk.new()
      |> OrderedBulk.insert_one(_id: 4, name: "Test4")
      |> OrderedBulk.insert_one(_id: 3, name: "Test4")
      |> OrderedBulk.insert_one(_id: 1, name: "Test1")
      |> OrderedBulk.insert_one(_id: 1, name: "Test2")
      |> OrderedBulk.delete_one(_id: 1)
      |> OrderedBulk.insert_one(_id: 2, name: "Test2")

    %BulkWriteResult{errors: [%{"code" => code}]} = result = BulkWrite.write(top.pid, bulk, [])

    assert code == 11_000
    assert %{:inserted_count => 3, :inserted_ids => [4, 3, 1]} == Map.take(result, [:inserted_count, :inserted_ids])
    assert {:ok, 3} == Mongo.count(top.pid, coll, %{})
  end

  test "stop when any operation fails: unordered", top do
    coll = unique_collection()

    bulk =
      coll
      |> UnorderedBulk.new()
      |> UnorderedBulk.insert_one(_id: 4, name: "Test4")
      |> UnorderedBulk.insert_one(_id: 3, name: "Test4")
      |> UnorderedBulk.insert_one(_id: 1, name: "Test1")
      |> UnorderedBulk.insert_one(_id: 1, name: "Test2")
      |> UnorderedBulk.delete_one(_id: 1)
      |> UnorderedBulk.insert_one(_id: 2, name: "Test2")

    %BulkWriteResult{errors: [%{"code" => code}]} = result = BulkWrite.write(top.pid, bulk, [])

    assert code == 11_000
    assert %{:inserted_count => 2, :inserted_ids => [2, 1]} == Map.take(result, [:inserted_count, :inserted_ids])
    assert {:ok, 2} == Mongo.count(top.pid, coll, %{})
  end
end
