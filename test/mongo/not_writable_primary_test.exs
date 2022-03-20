defmodule Mongo.NotWritablePrimaryTest do
  use ExUnit.Case, async: false

  setup_all do
    assert {:ok, top} = Mongo.TestConnection.connect()
    Mongo.drop_database(top)
    %{pid: top}
  end

  test "not writable primary", c do
    top = c.pid

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 10107, failCommands: ["insert"], closeConnection: false]
    ]

    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta1"})
    Mongo.admin_command(top, cmd)
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta2"})
  end
end
