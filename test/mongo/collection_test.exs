defmodule Mongo.CollectionTest do
  use CollectionCase

  test "rename_collection", %{pid: top} do
    coll = unique_collection()
    new_coll = "this_is_my_new_collection"

    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oska", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    assert top
           |> Mongo.show_collections()
           |> Enum.to_list()
           |> Enum.find(fn name -> name == coll end)

    assert :ok = Mongo.rename_collection(top, "mongodb_test.#{coll}", "mongodb_test.#{new_coll}")

    assert {:ok, 4} == Mongo.count(top, new_coll, %{})

    assert top
           |> Mongo.show_collections()
           |> Enum.to_list()
           |> Enum.find(fn name -> name == new_coll end)
  end

  test "create collection", c do
    coll = unique_collection()

    assert nil == Mongo.show_collections(c.pid) |> Enum.find(fn c -> c == coll end)
    assert :ok == Mongo.create(c.pid, coll)
    assert nil != Mongo.show_collections(c.pid) |> Enum.find(fn c -> c == coll end)
  end

  test "drop collection", c do
    coll = unique_collection()

    assert nil == Mongo.show_collections(c.pid) |> Enum.find(fn c -> c == coll end)
    assert :ok == Mongo.create(c.pid, coll)
    assert nil != Mongo.show_collections(c.pid) |> Enum.find(fn c -> c == coll end)
    assert :ok == Mongo.drop_collection(c.pid, coll)
    assert nil == Mongo.show_collections(c.pid) |> Enum.find(fn c -> c == coll end)
  end
end
