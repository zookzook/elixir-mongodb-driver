defmodule Mongo.ChangeStreamTest do
  use ExUnit.Case # DO NOT MAKE ASYNCHRONOUS

  setup_all do
    assert {:ok, top} = Mongo.TestConnection.connect
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Waldo"})
    %{pid: top}
  end

  def consumer_1(top, monitor) do
    Process.sleep(1000)
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, max_time: 1_000, debug: true )
    result = cursor |> Enum.take(2) |> Enum.at(0)
    send(monitor, {:insert, result})
  end

  def consumer_2(top, monitor, token) do
    Process.sleep(1000)
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, resume_after: token, max_time: 1_000 )
    result = cursor |> Enum.take(1) |> Enum.at(0)
    send(monitor, {:insert, result})
  end

  def consumer_3(top, monitor, token) do
    Process.sleep(1000)
    cursor = Mongo.watch_collection(top, "users", [], fn doc -> send(monitor, {:token, doc}) end, resume_after: token, max_time: 1_000 )
    result = cursor |> Enum.take(4) |> Enum.map(fn %{"fullDocument" => %{"name" => name}} -> name end)
    send(monitor, {:insert, result})

  end

  def producer(top) do
    Process.sleep(2000)
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Greta"})
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Gustav"})
    assert {:ok, %Mongo.InsertOneResult{}} = Mongo.insert_one(top, "users", %{name: "Tom"})
  end

  @tag :change_streams
  test "change stream: watch and resume_after", %{pid: top} do

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

    Process.sleep(500)

    spawn(fn -> consumer_3(top, me, token) end)
    spawn(fn -> producer(top) end)

    assert_receive {:token, _}, 5_000
    assert_receive {:insert, ["Gustav", "Tom", "Liese", "Greta"]}, 5_000

    Process.sleep(1000)

  end
end
