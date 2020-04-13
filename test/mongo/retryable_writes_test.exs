defmodule Mongo.RetryableWritesTest do
  use ExUnit.Case

  alias Mongo.Error

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    Mongo.drop_database(pid)
    {:ok, [pid: pid]}
  end

  setup do
    {:ok, catcher} = EventCatcher.start_link()

    on_exit(fn -> EventCatcher.stop(catcher) end)

    [catcher: catcher]
  end

  test "retryable writes: insert one", %{pid: top, catcher: catcher} do

    coll = "retryable_writes_1"

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 6, failCommands: ["insert"]]
    ]

    assert {:ok, _} = Mongo.admin_command(top, cmd)
    assert {:error, %Error{code: 6, retryable_writes: true}} = Mongo.insert_one(top, coll, %{"name" => "Waldo"}, retryable_writes: false)

    assert {:ok, _} = Mongo.admin_command(top, cmd)
    assert {:ok, _} = Mongo.insert_one(top, coll, %{"name" => "Waldo"})

    assert [:insert | _] = EventCatcher.retry_write_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end

  test "retryable writes: delete one", %{pid: top, catcher: catcher} do

    coll = "retryable_writes_2"

    Mongo.insert_one(top, coll, %{"name" => "Waldo"})

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 6, failCommands: ["delete"]]
    ]

    assert {:ok, _} = Mongo.admin_command(top, cmd)
    assert {:error, %Error{code: 6, retryable_writes: true}} = Mongo.delete_one(top, coll, %{"name" => "Waldo"}, retryable_writes: false)

    assert {:ok, _} = Mongo.admin_command(top, cmd)
    assert {:ok, _} = Mongo.delete_one(top, coll, %{"name" => "Waldo"})

    assert [:delete | _] = EventCatcher.retry_write_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end

end