defmodule Mongo.RetryableWritesTest do
  use CollectionCase

  alias Mongo.Error

  @tag :rs_required
  test "retryable writes: insert one", %{pid: top, catcher: catcher} do
    coll = unique_collection()

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

  @tag :rs_required
  test "retryable writes: delete one", %{pid: top, catcher: catcher} do
    coll = unique_collection()

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

  test "retryable writes: replace one", %{pid: top} do
    coll = unique_collection()

    assert {:ok, %Mongo.InsertOneResult{acknowledged: true, inserted_id: 42}} = Mongo.insert_one(top, coll, %{"_id" => 42, "name" => "Waldo"}, retryable_writes: true)
    assert %{"_id" => 42, "name" => "Waldo"} = Mongo.find_one(top, coll, %{"_id" => 42})
    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1}} = Mongo.replace_one(top, coll, %{"_id" => 42}, %{"_id" => 42, "name" => "Greta"}, retryable_writes: true)
    assert %{"_id" => 42, "name" => "Greta"} = Mongo.find_one(top, coll, %{"_id" => 42})
    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1}} = Mongo.replace_one(top, coll, %{"_id" => 42}, %{"_id" => 42, "name" => "Merlin"}, retryable_writes: true)
    assert %{"_id" => 42, "name" => "Merlin"} = Mongo.find_one(top, coll, %{"_id" => 42})
  end
end
