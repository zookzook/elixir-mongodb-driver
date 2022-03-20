defmodule Mongo.RetryableReadsTest do
  use CollectionCase

  alias Mongo.Error
  alias Mongo.Session

  test "find_one", %{pid: top, catcher: catcher} do
    coll = unique_collection()

    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oska", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 6, failCommands: ["find"]]
    ]

    Mongo.admin_command(top, cmd)
    {:error, %Error{code: 6, retryable_reads: true}} = Mongo.find_one(top, coll, %{"name" => "Waldo"})

    Mongo.admin_command(top, cmd)
    assert %{"_id" => _id, "age" => 5, "name" => "Waldo"} = Mongo.find_one(top, coll, %{"name" => "Waldo"}, retryable_reads: true)

    assert [:find | _] = EventCatcher.retryable_read_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end

  test "find_one in transaction", %{pid: top, catcher: catcher} do
    coll = unique_collection()
    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oska", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 6, failCommands: ["find"]]
    ]

    {:ok, session} = Session.start_session(top, :read, [])

    Mongo.admin_command(top, cmd)
    {:error, %Error{code: 6, retryable_reads: true}} = Mongo.find_one(top, coll, %{"name" => "Waldo"}, retryable_reads: true, session: session)

    Session.end_session(top, session)

    assert [] = EventCatcher.retryable_read_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end

  test "count", %{pid: top, catcher: catcher} do
    coll = unique_collection()
    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oska", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 6, failCommands: ["count"]]
    ]

    Mongo.admin_command(top, cmd)
    {:error, %Error{code: 6, retryable_reads: true}} = Mongo.count(top, coll, %{})

    Mongo.admin_command(top, cmd)
    assert {:ok, 4} == Mongo.count(top, coll, %{}, retryable_reads: true)

    assert [:count | _] = EventCatcher.retryable_read_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end
end
