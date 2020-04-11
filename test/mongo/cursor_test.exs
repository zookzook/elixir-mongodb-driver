defmodule Mongo.CursorTest do
  use ExUnit.Case, async: false

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  setup do
    {:ok, catcher} = EventCatcher.start_link()

    on_exit(fn -> EventCatcher.stop(catcher) end)

    [catcher: catcher]
  end

  test "checking if killCursor is called properly", c do

    coll    = "kill_cursors"
    catcher = c.catcher
    docs    = Stream.cycle([%{foo: 42}]) |> Enum.take(1000) ## forcing to get a cursor id

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, docs)
    assert [%{"foo" => 42}, %{"foo" => 42}] = Mongo.find(c.pid, coll, %{}) |> Enum.take(2) |> Enum.map(fn m ->  Map.pop(m, "_id") |> elem(1) end)
    assert [:killCursors | _] = EventCatcher.succeeded_events(catcher) |> Enum.map(fn event -> event.command_name end)

  end

  # issue #35: Crash executing find function without enough permission
  test "matching errors in the next function of the stream api", c do
    assert {:error, %Mongo.Error{__exception__: true, code: 2, error_labels: '', host: nil, message: "unknown operator: $gth", resumable: false, retryable_reads: false}} == Mongo.find(c.pid, "test", [_id: ["$gth": 1]])
  end

end
