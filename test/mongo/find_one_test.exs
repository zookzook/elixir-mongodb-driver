defmodule Mongo.FindOneTest do
  use ExUnit.Case

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    Mongo.drop_database(pid)
    {:ok, [pid: pid]}
  end

  test "find_one, using :sort options", %{pid: top} do

    coll = "find_one_sort"
    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oska", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    assert %{"name" => "Greta"} == Mongo.find_one(top, coll, %{}, sort: %{name: 1}) |> Map.take(["name"])
    assert %{"name" => "Waldo"} == Mongo.find_one(top, coll, %{}, sort: %{name: -1}) |> Map.take(["name"])
    assert %{"name" => "Oska"} == Mongo.find_one(top, coll, %{}, sort: %{age: 1}) |> Map.take(["name"])
    assert %{"name" => "Tom"} == Mongo.find_one(top, coll, %{}, sort: %{age: -1}) |> Map.take(["name"])
  end
end