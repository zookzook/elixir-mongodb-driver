defmodule Mongo.BatchSizeTest do
  require Logger

  use CollectionCase

  test "find, using :batch_size of 100 options", %{pid: top, catcher: catcher} do
    coll = unique_collection()
    n = 10_000
    Mongo.delete_many(top, coll, %{})

    Enum.each(1..n, fn i ->
      Mongo.insert_one(top, coll, %{index: i}, w: 0)
    end)

    assert {:ok, n} == Mongo.count(top, coll, %{})

    assert n ==
             top
             |> Mongo.find(coll, %{}, batch_size: 100)
             |> Enum.to_list()
             |> Enum.count()

    get_mores =
      catcher
      |> EventCatcher.succeeded_events()
      |> Enum.map(fn event -> event.command_name end)
      |> Enum.filter(fn command_name -> command_name == :getMore end)
      |> Enum.count()

    assert 100 == get_mores
  end
end
