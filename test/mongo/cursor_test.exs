defmodule Mongo.CursorTest do
  use CollectionCase, async: false

  test "tailable cursors with awaitData", c do

    coll      = "tailable_cursors"
    init_docs = Stream.cycle([%{"foo" => 42}]) |> Enum.take(5)
    tail_docs = Stream.cycle([%{"foo" => 10}]) |> Enum.take(10)

    assert :ok      = Mongo.create(c.pid, coll, capped: true, size: 1_000_000)
    assert {:ok, _} = Mongo.insert_many(c.pid, coll, init_docs)

    tailing_task = Task.async fn ->
      Mongo.find(c.pid, coll, %{}, tailable: true, await_data: true)
      |> Enum.take(15)
    end

    Enum.each tail_docs, fn doc ->
      Process.sleep 100
      Mongo.insert_one(c.pid, coll, doc)
    end

    expected_docs = init_docs ++ tail_docs
    assert ^expected_docs = Task.await(tailing_task) |> Enum.map(fn m ->  Map.pop(m, "_id") |> elem(1) end)

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
    assert {:error, %Mongo.Error{__exception__: true, code: 2, error_labels: '', fail_command: false, host: nil, message: "unknown operator: $gth", resumable: false, retryable_reads: false, retryable_writes: false, not_writable_primary_or_recovering: false}} == Mongo.find(c.pid, "test", [_id: ["$gth": 1]])
  end

end
