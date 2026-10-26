defmodule Mongo.StreamTest do
  use CollectionCase

  setup %{pid: top, test: test} do
    # A collection per test: some tests count the commands sent on it.
    coll = "stream_#{:erlang.phash2(test)}"
    {:ok, _} = Mongo.delete_many(top, coll, %{})
    {:ok, _} = Mongo.insert_many(top, coll, Enum.map(1..1_000, fn i -> %{index: i, name: "doc #{i}", tags: ["a", "b"]} end))
    [coll: coll]
  end

  test "find returns every document in order for small and large batches", %{pid: top, coll: coll} do
    for batch_size <- [1, 7, 100, 1_000, 5_000] do
      indexes =
        top
        |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: batch_size)
        |> Enum.map(& &1["index"])

      assert indexes == Enum.to_list(1..1_000)
    end
  end

  test "aggregate returns decoded documents", %{pid: top, coll: coll} do
    assert [%{"index" => 1, "name" => "doc 1", "tags" => ["a", "b"]}] =
             top
             |> Mongo.aggregate(coll, [%{"$sort" => %{index: 1}}, %{"$limit" => 1}, %{"$project" => %{_id: 0}}])
             |> Enum.to_list()
  end

  test "halting a stream early still closes the cursor", %{pid: top, coll: coll, catcher: catcher} do
    assert [1, 2, 3] == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: 100) |> Stream.map(& &1["index"]) |> Enum.take(3)

    assert catcher |> EventCatcher.succeeded_events() |> Enum.any?(&(&1.command_name == :killCursors))
  end

  test "the docs of a stream are still a list of decoded documents", %{pid: top, coll: coll} do
    %Mongo.Stream{docs: docs} = stream = Mongo.find(top, coll, %{}, sort: %{index: 1}, batch_size: 10)
    assert is_list(docs)
    assert Enum.map(docs, & &1["index"]) == Enum.to_list(1..10)

    # Consume the stream so that its session and cursor are released.
    Stream.run(stream)
  end

  test "command event subscribers still get decoded batches", %{pid: top, coll: coll, catcher: catcher} do
    assert 1_000 == top |> Mongo.find(coll, %{}, batch_size: 400) |> Enum.count()

    batches =
      catcher
      |> EventCatcher.succeeded_events()
      |> Enum.filter(&(&1.command_name in [:find, :getMore]))
      |> Enum.map(fn event -> event.reply["cursor"]["firstBatch"] || event.reply["cursor"]["nextBatch"] end)

    assert batches |> Enum.map(&length/1) |> Enum.sort() == [200, 400, 400]
    assert %{"index" => _} = hd(hd(batches))
  end

  # Command events carry decoded replies, so with a subscriber batches are decoded as before.
  defp without_command_subscribers(catcher) do
    ref = Process.monitor(catcher)
    EventCatcher.stop(catcher)
    assert_receive {:DOWN, ^ref, _, _, _}
    assert [] == Registry.lookup(:events_registry, :commands)
  end

  test "without command event subscribers, getMore batches are decoded as they are consumed", %{pid: top, coll: coll, catcher: catcher} do
    without_command_subscribers(catcher)
    %Mongo.Stream{session: {:own, session}, cursor: cursor_id} = stream = Mongo.find(top, coll, %{}, sort: %{index: 1}, batch_size: 300)

    get_more = [getMore: %BSON.LongNumber{value: cursor_id}, collection: coll, batchSize: 300]
    assert {:ok, %{"cursor" => %{"nextBatch" => %BSON.LazyDocuments{}}}} = Mongo.exec_command_session(session, get_more, Keyword.put(stream.opts, :lazy_cursor, true))
    assert {:ok, %{"cursor" => %{"nextBatch" => [_ | _]}}} = Mongo.exec_command_session(session, get_more, stream.opts)

    # Every document but the 600 read above.
    assert Enum.map(stream, & &1["index"]) == Enum.to_list(1..300) ++ Enum.to_list(901..1_000)
  end

  test "without command event subscribers, streams return every document in order", %{pid: top, coll: coll, catcher: catcher} do
    without_command_subscribers(catcher)

    for batch_size <- [1, 7, 100, 1_000] do
      assert Enum.to_list(1..1_000) == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: batch_size) |> Enum.map(& &1["index"])
    end

    assert [1, 2, 3] == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: 100) |> Stream.drop(100) |> Stream.map(&(&1["index"] - 100)) |> Enum.take(3)
  end

  defmodule MatchStage do
    # A pipeline stage built by a query builder.
    defstruct [:filter]

    defimpl Mongo.Encoder do
      def encode(%{filter: filter}), do: %{"$match" => filter}
    end
  end

  test "aggregate pipelines whose stages are structs", %{pid: top, coll: coll, catcher: catcher} do
    without_command_subscribers(catcher)
    assert 1_000 == top |> Mongo.aggregate(coll, [%MatchStage{filter: %{}}], batch_size: 100) |> Enum.count()
  end

  defmodule FailingDecoder do
    def document(_binary), do: raise(ArgumentError, "bad document")
  end

  defmodule SlowDecoder do
    def document(binary) do
      Process.sleep(2)
      BSON.Decoder.document(binary)
    end
  end

  # A stream whose next batch is the given documents, undecoded, as if a getMore had returned them.
  defp with_lazy_batch(stream, decoder, docs) do
    binary = docs |> Enum.with_index() |> Enum.map(fn {doc, i} -> [0x03, Integer.to_string(i), 0, BSON.Encoder.document(doc)] end) |> IO.iodata_to_binary()
    get_more = [getMore: %BSON.LongNumber{value: stream.cursor}, collection: stream.coll |> String.split(".") |> List.last()]
    %{stream | docs: %BSON.LazyDocuments{binary: binary, decoder: decoder}, batch_log: {get_more, 0}}
  end

  def forward_execution(_event, %{duration: duration}, %{command: command}, parent), do: send(parent, {:executed, command, duration})

  defp attach_telemetry(test) do
    :telemetry.attach("#{inspect(test)}", [:mongodb_driver, :execution], &__MODULE__.forward_execution/4, self())
    on_exit(fn -> :telemetry.detach("#{inspect(test)}") end)
  end

  defp executed() do
    receive do
      {:executed, command, _duration} -> [command | executed()]
    after
      0 -> []
    end
  end

  test "a batch that fails to decode fails like a getMore whose reply cannot be decoded", %{pid: top, coll: coll, catcher: catcher, test: test} do
    attach_telemetry(test)
    stream = top |> Mongo.find(coll, %{}, batch_size: 10) |> with_lazy_batch(FailingDecoder, [%{a: 1}])

    # Mongo.Stream raises the {:error, error} that the getMore returns.
    error = assert_raise ArgumentError, fn -> Enum.to_list(stream) end
    assert error.message =~ ~s(got: {:error, %ArgumentError{message: "bad document"}})
    assert catcher |> EventCatcher.succeeded_events() |> Enum.any?(&(&1.command_name == :killCursors))

    # As before, a getMore that fails to decode is not logged.
    refute_received {:executed, :getMore, _duration}
  end

  test "without command event subscribers, getMores are logged once their batch is decoded", %{pid: top, coll: coll, catcher: catcher, test: test} do
    without_command_subscribers(catcher)
    attach_telemetry(test)

    assert 1_000 == top |> Mongo.find(coll, %{}, batch_size: 300) |> Enum.count()
    assert_received {:executed, :find, _duration}
    for _ <- 1..3, do: assert_received({:executed, :getMore, _duration})
    refute_received {:executed, :getMore, _duration}
  end

  test "the logged duration of a getMore includes decoding its batch", %{pid: top, coll: coll, test: test} do
    attach_telemetry(test)
    docs = Enum.map(1..10, &%{index: &1})

    # With a limit, the cursor is closed after the first batch, which is replaced by a lazy one.
    assert 10 == top |> Mongo.find(coll, %{}, limit: 10, batch_size: 10) |> with_lazy_batch(SlowDecoder, docs) |> Enum.count()
    assert_received {:executed, :getMore, duration}
    assert duration >= 10 * 2_000

    # A getMore whose batch was only partly read is logged when the stream halts.
    docs = Enum.map(1..150, &%{index: &1})
    assert [1] == top |> Mongo.find(coll, %{}, limit: 10, batch_size: 10) |> with_lazy_batch(SlowDecoder, docs) |> Stream.map(& &1["index"]) |> Enum.take(1)
    assert_received {:executed, :getMore, duration}
    assert duration >= 100 * 2_000

    # And before its cursor is killed, if it is still open.
    _ = executed()
    assert [1] == top |> Mongo.find(coll, %{}, batch_size: 10) |> with_lazy_batch(SlowDecoder, docs) |> Stream.map(& &1["index"]) |> Enum.take(1)
    assert [:find, :getMore, :killCursors] == executed()
  end
end
