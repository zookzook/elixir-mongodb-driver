defmodule Mongo.ChangeStreamTest do
  # DO NOT MAKE ASYNCHRONOUS
  use ExUnit.Case, async: false

  setup_all do
    assert {:ok, top} = Mongo.TestConnection.connect()
    Mongo.drop_database(top, nil, w: 3)
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Waldo"})
    %{pid: top}
  end

  setup do
    {:ok, catcher} = EventCatcher.start_link()
    on_exit(fn -> EventCatcher.stop(catcher) end)
    [catcher: catcher]
  end

  def consumer(top, monitor) do
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, max_time: 1_000)
    send(monitor, :go)
    result = cursor |> Enum.take(1) |> Enum.at(0)
    send(monitor, {:insert, result})
  end

  def consumer_1(top, monitor) do
    Process.sleep(1000)
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, max_time: 1_000)
    result = cursor |> Enum.take(1) |> Enum.at(0)
    send(monitor, {:insert, result})
  end

  def consumer_2(top, monitor, token) do
    Process.sleep(1000)
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, resume_after: token, max_time: 1_000)
    result = cursor |> Enum.take(1) |> Enum.at(0)
    send(monitor, {:insert, result})
  end

  def consumer_3(top, monitor, token) do
    Process.sleep(1000)
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, resume_after: token, max_time: 1_000)
    result = cursor |> Enum.take(4) |> Enum.map(fn %{"fullDocument" => %{"name" => name}} -> name end)
    send(monitor, {:insert, result})
  end

  def producer(top) do
    Process.sleep(2000)
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Gustav"})
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Tom"})
  end

  @tag :rs_required
  test "change stream resumes after HostUnreachable", c do
    top = c.pid
    catcher = c.catcher

    cmd = [
      configureFailPoint: "failGetMoreAfterCursorCheckout",
      mode: [times: 1],
      data: [errorCode: 6, closeConnection: false]
    ]

    me = self()
    Mongo.admin_command(top, cmd)
    spawn(fn -> consumer(top, me) end)
    assert_receive :go
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})

    assert_receive {:token, _}, 5_000
    assert_receive {:token, _token}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Greta"}}}, 5_000

    assert [{:getMore, ["ResumableChangeStreamError"]}] == EventCatcher.failed_events(catcher) |> Enum.map(fn event -> {event.command_name, event.failure.error_labels} end)
  end

  @tag :rs_required
  test "change stream resumes after HostNotFound", c do
    top = c.pid
    catcher = c.catcher

    cmd = [
      configureFailPoint: "failGetMoreAfterCursorCheckout",
      mode: [times: 1],
      data: [errorCode: 7, closeConnection: false]
    ]

    me = self()
    Mongo.admin_command(top, cmd)
    spawn(fn -> consumer(top, me) end)
    assert_receive :go
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})

    assert_receive {:token, _}, 5_000
    assert_receive {:token, _token}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Greta"}}}, 5_000

    assert [{:getMore, ["ResumableChangeStreamError"]}] == EventCatcher.failed_events(catcher) |> Enum.map(fn event -> {event.command_name, event.failure.error_labels} end)
  end

  @tag :rs_required
  test "change stream resumes after NetworkTimeout", c do
    top = c.pid
    catcher = c.catcher

    cmd = [
      configureFailPoint: "failGetMoreAfterCursorCheckout",
      mode: [times: 1],
      data: [errorCode: 89, closeConnection: false]
    ]

    me = self()
    Mongo.admin_command(top, cmd)
    spawn(fn -> consumer(top, me) end)
    assert_receive :go

    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})

    assert_receive {:token, _}, 5_000
    assert_receive {:token, _token}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Greta"}}}, 5_000

    assert [{:getMore, ["ResumableChangeStreamError"]}] == EventCatcher.failed_events(catcher) |> Enum.map(fn event -> {event.command_name, event.failure.error_labels} end)
  end

  @tag :rs_required
  test "change stream resumes after ShutdownInProgress", c do
    top = c.pid
    catcher = c.catcher

    cmd = [
      configureFailPoint: "failGetMoreAfterCursorCheckout",
      mode: [times: 1],
      data: [errorCode: 91, closeConnection: false]
    ]

    me = self()
    Mongo.admin_command(top, cmd)
    spawn(fn -> consumer(top, me) end)
    assert_receive :go

    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})

    assert_receive {:token, _}, 5_000
    assert_receive {:token, _token}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Greta"}}}, 5_000

    assert [{:getMore, ["ResumableChangeStreamError"]}] == EventCatcher.failed_events(catcher) |> Enum.map(fn event -> {event.command_name, event.failure.error_labels} end)
  end

  @tag :rs_required
  test "change stream resumes if error contains ResumableChangeStreamError", c do
    top = c.pid
    catcher = c.catcher

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 50, failCommands: ["getMore"], closeConnection: false, errorLabels: ["ResumableChangeStreamError"]]
    ]

    me = self()
    Mongo.admin_command(top, cmd)
    spawn(fn -> consumer(top, me) end)
    assert_receive :go

    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})

    assert_receive {:token, _}, 5_000
    assert_receive {:token, _token}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Greta"}}}, 5_000

    assert [{:getMore, ["ResumableChangeStreamError"]}] == EventCatcher.failed_events(catcher) |> Enum.map(fn event -> {event.command_name, event.failure.error_labels} end)
  end

  @tag :mongo_3_6
  test "change stream: watch and resume_after", c do
    top = c.pid
    me = self()
    spawn(fn -> consumer_1(top, me) end)
    spawn(fn -> producer(top) end)

    assert_receive {:token, _}, 5_000
    assert_receive {:token, token}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Greta"}}}, 5_000

    Process.sleep(500)

    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Liese"})

    spawn(fn -> consumer_2(top, me, token) end)
    spawn(fn -> producer(top) end)

    assert_receive {:token, _}, 5_000
    assert_receive {:insert, %{"fullDocument" => %{"name" => "Gustav"}}}, 5_000

    # Process.sleep(500)

    spawn(fn -> consumer_3(top, me, token) end)
    spawn(fn -> producer(top) end)

    assert_receive {:token, _}, 5_000
    assert_receive {:insert, ["Gustav", "Tom", "Liese", "Greta"]}, 5_000
  end
end
