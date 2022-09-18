defmodule Mongo.Messages do
  @moduledoc """
    This module encodes and decodes the data from and to the mongodb server.
    We only support MongoDB >= 3.2 and use op_query with the hack collection "$cmd"
    Other op codes are deprecated. Therefore only op_reply and op_query are supported.
  """

  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
    end
  end

  import Record
  import Mongo.BinaryUtils

  @op_reply 1
  @op_query 2004
  @op_msg_code 2013

  @query_flags [
    tailable_cursor: 0x2,
    slave_ok: 0x4,
    oplog_replay: 0x8,
    no_cursor_timeout: 0x10,
    await_data: 0x20,
    exhaust: 0x40,
    partial: 0x80
  ]

  @msg_flags [
    # Checksum present
    checksum_present: 0x00001,
    # Sender will send another message and is not prepared for overlapping messages
    more_to_come: 0x00002,
    # Client is prepared for multiple replies (using the moreToCome bit) to this request
    exhaust_allowed: 0x10000
  ]

  @header_size 4 * 4

  defrecord :msg_header, [:length, :request_id, :response_to, :op_code]
  defrecord :op_query, [:flags, :coll, :num_skip, :num_return, :query, :select]
  defrecord :op_reply, [:flags, :cursor_id, :from, :num, :docs]
  defrecord :sequence, [:size, :identifier, :docs]
  defrecord :payload, [:doc, :sequence]
  defrecord :section, [:payload_type, :payload]
  defrecord :op_msg, [:flags, :sections]

  @doc """
    Decodes the header from response of a request sent by the mongodb server
  """
  def decode_header(iolist) when is_list(iolist) do
    case IO.iodata_length(iolist) == @header_size do
      true ->
        iolist
        |> IO.iodata_to_binary()
        |> decode_header()

      false ->
        :error
    end
  end

  def decode_header(<<length::int32, request_id::int32, response_to::int32, op_code::int32>>) do
    header = msg_header(length: length - @header_size, request_id: request_id, response_to: response_to, op_code: op_code)
    {:ok, header}
  end

  def decode_header(_binary), do: :error

  @doc """
    Decodes the response body of a request sent by the mongodb server
  """
  def decode_response(msg_header(length: length) = header, iolist) when is_list(iolist) do
    case IO.iodata_length(iolist) >= length do
      true -> decode_response(header, IO.iodata_to_binary(iolist))
      false -> :error
    end
  end

  def decode_response(msg_header(length: length, response_to: response_to, op_code: op_code), binary) when byte_size(binary) >= length do
    <<response::binary(length), rest::binary>> = binary

    case op_code do
      @op_reply -> {:ok, response_to, decode_reply(response), rest}
      @op_msg_code -> {:ok, response_to, decode_msg(response), rest}
      _ -> :error
    end
  end

  def decode_response(_header, _binary), do: :error

  @doc """
    Decodes a reply message from the response
  """
  def decode_reply(<<flags::int32, cursor_id::int64, from::int32, num::int32, rest::binary>>) do
    op_reply(flags: flags, cursor_id: cursor_id, from: from, num: num, docs: BSON.Decoder.documents(rest))
  end

  def decode_msg(<<flags::int32, rest::binary>>) do
    op_msg(flags: flags, sections: decode_sections(rest))
  end

  def decode_sections(binary), do: decode_sections(binary, [])
  def decode_sections("", acc), do: Enum.reverse(acc)

  def decode_sections(<<0x00::int8, payload::binary>>, acc) do
    <<size::int32, _rest::binary>> = payload
    <<doc::binary(size), rest::binary>> = payload

    with {doc, ""} <- BSON.Decoder.document(doc) do
      decode_sections(rest, [section(payload_type: 0, payload: payload(doc: doc)) | acc])
    end
  end

  def decode_sections(<<0x01::int8, payload::binary>>, acc) do
    <<size::int32, _rest::binary>> = payload
    <<sequence::binary(size), rest::binary>> = payload
    decode_sections(rest, [section(payload_type: 1, payload: payload(sequence: decode_sequence(sequence))) | acc])
  end

  def decode_sequence(<<size::int32, rest::binary>>) do
    with {identifier, docs} <- cstring(rest) do
      sequence(size: size, identifier: identifier, docs: BSON.Decoder.documents(docs))
    end
  end

  defp cstring(binary) do
    [string, rest] = :binary.split(binary, <<0x00>>)
    {string, rest}
  end

  def encode(request_id, op_query() = op) do
    iodata = encode_op(op)
    header = msg_header(length: IO.iodata_length(iodata) + @header_size, request_id: request_id, response_to: 0, op_code: @op_query)
    [encode_header(header) | iodata]
  end

  def encode(request_id, op_msg() = op) do
    iodata = encode_op(op)
    header = msg_header(length: IO.iodata_length(iodata) + @header_size, request_id: request_id, response_to: 0, op_code: @op_msg_code)
    [encode_header(header) | iodata]
  end

  defp encode_header(msg_header(length: length, request_id: request_id, response_to: response_to, op_code: op_code)) do
    <<length::int32, request_id::int32, response_to::int32, op_code::int32>>
  end

  defp encode_op(op_query(flags: flags, coll: coll, num_skip: num_skip, num_return: num_return, query: query, select: select)) do
    [<<blit_flags(:query, flags)::int32>>, coll, <<0x00, num_skip::int32, num_return::int32>>, BSON.Encoder.document(query), select]
  end

  defp encode_op(op_msg(flags: flags, sections: sections)) do
    [<<blit_flags(:msg, flags)::int32>> | encode_sections(sections)]
  end

  defp encode_sections(sections) do
    Enum.map(sections, fn section -> encode_section(section) end)
  end

  defp encode_section(section(payload_type: t, payload: payload)) do
    [<<t::int8>> | encode_payload(payload)]
  end

  defp encode_payload(payload(doc: doc, sequence: nil)) do
    BSON.Encoder.document(doc)
  end

  defp encode_payload(payload(doc: nil, sequence: sequence(identifier: identifier, docs: docs))) do
    iodata = [identifier, <<0x00>> | Enum.map(docs, fn doc -> BSON.Encoder.encode(doc) end)]
    size = IO.iodata_length(iodata) + 4
    [<<size::int32>> | iodata]
  end

  defp blit_flags(op, flags) when is_list(flags) do
    import Bitwise
    Enum.reduce(flags, 0x0, &(flag_to_bit(op, &1) ||| &2))
  end

  defp blit_flags(_op, flags) when is_integer(flags) do
    flags
  end

  Enum.each(@query_flags, fn {flag, bit} ->
    defp flag_to_bit(:query, unquote(flag)), do: unquote(bit)
  end)

  Enum.each(@msg_flags, fn {flag, bit} ->
    defp flag_to_bit(:msg, unquote(flag)), do: unquote(bit)
  end)

  defp flag_to_bit(_op, _flag), do: 0x0
end
