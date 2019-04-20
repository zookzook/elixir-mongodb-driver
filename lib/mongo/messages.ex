defmodule Mongo.Messages do
  @moduledoc """
    This module encodes and decodes the data from and to the mongodb server.
    We only support MongoDB >= 3.2 and use op_query with the hack collection "$cmd"
    Other op codes are deprecated. Therefore only op_reply and op_query are supported.
  """

  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
      @reply_cursor_not_found   0x1
      @reply_query_failure      0x2
      @reply_shard_config_stale 0x4
      @reply_await_capable      0x8
    end
  end

  import Record
  import Mongo.BinaryUtils

  @op_reply    1
  @op_query    2004
  @op_msg_code 2013

  @query_flags [
    tailable_cursor:   0x2,
    slave_ok:          0x4,
    oplog_replay:      0x8,
    no_cursor_timeout: 0x10,
    await_data:        0x20,
    exhaust:           0x40,
    partial:           0x80
  ]

  @header_size 4 * 4

  defrecordp :msg_header, [:length, :request_id, :response_to, :op_code]
  defrecord  :op_query, [:flags, :coll, :num_skip, :num_return, :query, :select]
  defrecord  :op_reply, [:flags, :cursor_id, :from, :num, :docs]
  defrecord  :op_msg, [:flags, :type, :docs]

  @doc """
    Decodes the header from response of a request sent by the mongodb server
  """
  def decode_header(iolist) when is_list(iolist) do
    case IO.iodata_length(iolist) >= @header_size do
      true  -> iolist |> IO.iodata_to_binary() |> decode_header()
      false -> :error
    end
  end
  def decode_header(<<length::int32, request_id::int32, response_to::int32, op_code::int32, rest::binary>>) do
    header = msg_header(length: length - @header_size, request_id: request_id, response_to: response_to, op_code: op_code) ## todo don't subtract header-size here
    {:ok, header, rest}
  end
  def decode_header(_binary), do: :error

  @doc """
    Decodes the response body of a request sent by the mongodb server
  """
  def decode_response(msg_header(length: length) = header, iolist) when is_list(iolist) do
    case IO.iodata_length(iolist) >= length do
      true  -> decode_response(header, IO.iodata_to_binary(iolist))
      false -> :error
    end
  end
  def decode_response(msg_header(length: length, response_to: response_to, op_code: op_code), binary)  when byte_size(binary) >= length do
    <<response::binary(length), rest::binary>> = binary
    case op_code do
      @op_reply -> {:ok, response_to, decode_reply(response), rest}
      _         -> :error
    end
  end
  def decode_response(_header, _binary), do: :error

  @doc """
    Decodes a reply message from the response
  """
  def decode_reply(<<flags::int32, cursor_id::int64, from::int32, num::int32, rest::binary>>) do
    op_reply(flags: flags, cursor_id: cursor_id, from: from, num: num, docs: BSON.Decoder.documents(rest))
  end

  def encode(request_id, op_query() = op) do
    iodata = encode_op(op)
    header = msg_header(length: IO.iodata_length(iodata) + @header_size,  request_id: request_id, response_to: 0, op_code: @op_query)
    [encode_header(header)|iodata]
  end

  defp encode_header(msg_header(length: length, request_id: request_id, response_to: response_to, op_code: op_code)) do
    <<length::int32, request_id::int32, response_to::int32, op_code::int32>>
  end

  defp encode_op(op_msg(flags: flags, docs: [doc])) do
    [<<0::int32>>, <<0x00>>, doc]
  end

  defp encode_op(op_query(flags: flags, coll: coll, num_skip: num_skip,
                          num_return: num_return, query: query, select: select)) do
    [<<blit_flags(:query, flags)::int32>>,
     coll,
     <<0x00, num_skip::int32, num_return::int32>>,
     BSON.Encoder.document(query),
     select]
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

  defp flag_to_bit(_op, _flag), do: 0x0
end
