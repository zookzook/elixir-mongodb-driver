defmodule BSON.LazyDocuments do
  @moduledoc """
  The documents of a cursor batch, kept as raw BSON and decoded on demand.

  Command replies are decoded with their cursor batch (`cursor.firstBatch` or
  `cursor.nextBatch`) in this form. `Mongo.exec_command/3` and
  `Mongo.exec_command_session/3` decode it into a list before returning, unless they are
  called with `lazy_cursor: true`, as `Mongo.Stream` does for `getMore` batches: it
  decodes them a few documents at a time while the stream is consumed, so a large batch
  never sits fully decoded on the heap. Users of the driver do not see it.

  ## Decoders

  A custom decoder (see the `:decoder` config) takes part by implementing the optional
  callbacks of this behaviour:

    * `c:lazy_cursor_document/1` turns lazy batches on. Decoders built with
      `use BSON.DecoderGenerator` can implement it with their `lazy_cursor_document/2`,
      which takes the decoder that decodes the batch documents.
    * `c:take_lazy_documents/2` decodes a chunk of documents in one call (for example in
      a NIF). Without it, the batch is decoded with `c:document/1`, one document at a time.

  A decoder without `c:lazy_cursor_document/1` still works; its batches are decoded eagerly.

  A document that cannot be decoded raises the decoder's error when it is taken, rather
  than when the batch is received.
  """

  @type t :: %__MODULE__{binary: binary, decoder: module}

  defstruct [:binary, :decoder]

  @doc "Decodes the document at the start of the binary and returns it with the rest."
  @callback document(binary) :: {BSON.document(), binary}

  @doc """
  Decodes a command reply like `c:document/1`, but returns its cursor batch as
  `%BSON.LazyDocuments{}`.
  """
  @callback lazy_cursor_document(binary) :: {BSON.document(), binary}

  @doc """
  Decodes at least one document (if any are left) and at most `count` from the start of
  the raw array body, and returns them with the rest of it.
  """
  @callback take_lazy_documents(binary, count :: pos_integer) :: {[BSON.document()], binary}

  @optional_callbacks lazy_cursor_document: 1, take_lazy_documents: 2

  @doc false
  # Called by Mongo.Messages for the command reply of an OP_MSG.
  @spec decode_reply(module, binary) :: {BSON.document(), binary}
  def decode_reply(decoder, binary) do
    case capabilities(decoder) do
      %{lazy: true} -> decoder.lazy_cursor_document(binary)
      %{lazy: false} -> decoder.document(binary)
    end
  end

  # Which optional callbacks a decoder implements, looked up once.
  defp capabilities(decoder) do
    key = {__MODULE__, decoder}

    with nil <- :persistent_term.get(key, nil) do
      Code.ensure_loaded(decoder)
      capabilities = %{lazy: function_exported?(decoder, :lazy_cursor_document, 1), take: function_exported?(decoder, :take_lazy_documents, 2)}
      :persistent_term.put(key, capabilities)
      capabilities
    end
  end

  @doc """
  Decodes up to `count` documents, or all of them with `:all`. Returns them with the
  remaining documents, or with `[]` once none are left.
  """
  @spec take(t, pos_integer | :all) :: {[BSON.document()], t | []}
  def take(%__MODULE__{binary: binary, decoder: decoder} = lazy, count) do
    {docs, rest} = take(binary, decoder, capabilities(decoder), count)

    case rest do
      "" -> {docs, []}
      rest -> {docs, %{lazy | binary: rest}}
    end
  end

  # Every document takes more than one byte, so the size of the binary is enough for :all.
  defp take(binary, decoder, %{take: true}, :all), do: decoder.take_lazy_documents(binary, byte_size(binary))
  defp take(binary, decoder, %{take: true}, count), do: decoder.take_lazy_documents(binary, count)
  defp take(binary, decoder, %{take: false}, count), do: take_each(binary, decoder, count, [])

  defp take_each("", _decoder, _count, acc), do: {Enum.reverse(acc), ""}
  defp take_each(binary, _decoder, 0, acc), do: {Enum.reverse(acc), binary}

  # The elements of a cursor batch are documents keyed "0", "1", ...
  defp take_each(<<0x03, rest::binary>>, decoder, count, acc) do
    {doc, rest} = rest |> skip_key() |> decoder.document()
    take_each(rest, decoder, decrement(count), [doc | acc])
  end

  defp decrement(:all), do: :all
  defp decrement(count), do: count - 1

  defp skip_key(<<0x00, rest::binary>>), do: rest
  defp skip_key(<<_byte, rest::binary>>), do: skip_key(rest)

  @doc """
  Decodes every document.
  """
  @spec to_list(t) :: [BSON.document()]
  def to_list(%__MODULE__{} = lazy), do: to_list(lazy, [])

  defp to_list([], chunks), do: chunks |> Enum.reverse() |> Enum.concat()

  defp to_list(lazy, chunks) do
    {docs, rest} = take(lazy, :all)
    to_list(rest, [docs | chunks])
  end

  @doc false
  # Replaces the lazy cursor batch of a command reply by the decoded list.
  def materialize(%{"cursor" => %{} = cursor} = reply) do
    %{reply | "cursor" => Map.new(cursor, &materialize_batch/1)}
  end

  def materialize(reply), do: reply

  defp materialize_batch({key, %__MODULE__{} = docs}), do: {key, to_list(docs)}
  defp materialize_batch(other), do: other

  defimpl Inspect do
    def inspect(%BSON.LazyDocuments{binary: binary}, _opts) do
      "#BSON.LazyDocuments<#{byte_size(binary)} bytes>"
    end
  end
end
