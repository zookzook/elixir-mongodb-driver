defmodule BSON.LazyDocumentsTest.ChunkDecoder do
  # Decodes a chunk in one call, the way a native decoder would.
  def document(binary), do: BSON.Decoder.document(binary)

  def take_lazy_documents(binary, count) do
    send(self(), {:chunk, count})
    {docs, rest} = BSON.LazyDocuments.take(%BSON.LazyDocuments{binary: binary, decoder: BSON.Decoder}, count)
    {docs, if(rest == [], do: "", else: rest.binary)}
  end
end

defmodule BSON.LazyDocumentsTest.FailingDecoder do
  def document(_binary), do: raise(ArgumentError, "bad document")
end

defmodule BSON.LazyDocumentsTest.PlainDecoder do
  # A decoder with only document/1, like a custom decoder without lazy batch hooks.
  def document(binary), do: BSON.Decoder.document(binary)
end

defmodule BSON.LazyDocumentsTest do
  use ExUnit.Case, async: true

  defp reply(batch_key, docs) do
    %{"cursor" => %{batch_key => docs, "id" => %BSON.LongNumber{value: 42}, "ns" => "db.coll"}, "ok" => 1.0}
    |> BSON.encode()
    |> IO.iodata_to_binary()
  end

  defp docs(n), do: Enum.map(1..n, fn i -> %{"i" => i, "name" => "doc #{i}", "tags" => ["a", "b"], "sub" => %{"x" => i}} end)

  for batch_key <- ["firstBatch", "nextBatch"] do
    test "keeps cursor.#{batch_key} raw and decodes it to the same documents" do
      binary = reply(unquote(batch_key), docs(250))

      {lazy_reply, ""} = BSON.Decoder.lazy_cursor_document(binary)
      {eager_reply, ""} = BSON.Decoder.document(binary)

      assert %BSON.LazyDocuments{} = lazy = lazy_reply["cursor"][unquote(batch_key)]
      assert BSON.LazyDocuments.to_list(lazy) == eager_reply["cursor"][unquote(batch_key)]
      assert Map.delete(lazy_reply["cursor"], unquote(batch_key)) == Map.delete(eager_reply["cursor"], unquote(batch_key))
      assert lazy_reply["ok"] == 1.0
    end
  end

  test "an empty batch stays an empty list" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("nextBatch", []))
    assert reply["cursor"]["nextBatch"] == []
  end

  test "only cursor batches are kept raw" do
    binary = %{"firstBatch" => [%{"a" => 1}], "cursor" => %{"other" => [%{"b" => 2}]}} |> BSON.encode() |> IO.iodata_to_binary()
    {reply, ""} = BSON.Decoder.lazy_cursor_document(binary)
    assert reply == BSON.Decoder.decode(binary)
  end

  test "take/2 decodes in chunks and ends with []" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("firstBatch", docs(5)))
    lazy = reply["cursor"]["firstBatch"]

    {first, rest} = BSON.LazyDocuments.take(lazy, 2)
    {second, rest} = BSON.LazyDocuments.take(rest, 2)
    {third, rest} = BSON.LazyDocuments.take(rest, 2)

    assert Enum.map(first ++ second ++ third, & &1["i"]) == [1, 2, 3, 4, 5]
    assert rest == []
  end

  test "keeps the decoder's options, e.g. preserving key order" do
    binary = reply("firstBatch", [[{"b", 1}, {"a", 2}]])
    {reply, ""} = BSON.PreserveOrderDecoder.lazy_cursor_document(binary)

    assert [%{"b" => 1, "a" => 2, __order__: ["b", "a"]}] = BSON.LazyDocuments.to_list(reply["cursor"]["firstBatch"])
    assert reply.__order__ == ["cursor", "ok"]
  end

  test "take/2 uses the decoder's take_lazy_documents/2 when it has one" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("firstBatch", docs(5)))
    lazy = %{reply["cursor"]["firstBatch"] | decoder: BSON.LazyDocumentsTest.ChunkDecoder}

    {first, rest} = BSON.LazyDocuments.take(lazy, 3)
    assert_received {:chunk, 3}
    assert %BSON.LazyDocuments{decoder: BSON.LazyDocumentsTest.ChunkDecoder} = rest
    {second, []} = BSON.LazyDocuments.take(rest, 3)

    assert first ++ second == docs(5)
  end

  test "a decoding failure raises the decoder's error" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("firstBatch", docs(2)))
    lazy = %{reply["cursor"]["firstBatch"] | decoder: BSON.LazyDocumentsTest.FailingDecoder}

    assert_raise ArgumentError, "bad document", fn -> BSON.LazyDocuments.take(lazy, 10) end
  end

  test "a decoder passed to lazy_cursor_document/2 decodes the batch" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("firstBatch", docs(250)), BSON.LazyDocumentsTest.ChunkDecoder)
    lazy = reply["cursor"]["firstBatch"]

    assert %BSON.LazyDocuments{decoder: BSON.LazyDocumentsTest.ChunkDecoder} = lazy
    assert BSON.LazyDocuments.to_list(lazy) == docs(250)
    assert_received {:chunk, _count}
  end

  test "a decoder without take_lazy_documents/2 decodes each document with document/1" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("firstBatch", docs(5)), BSON.LazyDocumentsTest.PlainDecoder)
    {first, rest} = BSON.LazyDocuments.take(reply["cursor"]["firstBatch"], 3)
    {second, []} = BSON.LazyDocuments.take(rest, 3)

    assert first ++ second == docs(5)
  end

  test "materialize/1 decodes the cursor batch of a reply and leaves other replies alone" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("nextBatch", docs(3)))

    assert BSON.LazyDocuments.materialize(reply) == BSON.Decoder.decode(reply("nextBatch", docs(3)))
    assert BSON.LazyDocuments.materialize(%{"ok" => 1.0}) == %{"ok" => 1.0}
    assert BSON.LazyDocuments.materialize(nil) == nil
  end

  test "a truncated or non-document batch raises" do
    for binary <- [<<0x03, "0", 0, 200, 0, 0, 0, 1, 2>>, <<0x03, "0">>, <<0x10, "0", 0, 1, 0, 0, 0>>] do
      assert catch_error(BSON.LazyDocuments.take(%BSON.LazyDocuments{binary: binary, decoder: BSON.Decoder}, 10))
    end
  end

  test "inspect does not dump the raw batch" do
    {reply, ""} = BSON.Decoder.lazy_cursor_document(reply("firstBatch", docs(3)))
    assert inspect(reply["cursor"]["firstBatch"]) =~ ~r/^#BSON.LazyDocuments<\d+ bytes>$/
  end
end
