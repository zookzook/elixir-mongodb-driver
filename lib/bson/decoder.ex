defmodule BSON.DecoderGenerator do
  @moduledoc false

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use BSON.Utils
      alias BSON.Decimal128

      require Logger

      @behaviour BSON.LazyDocuments

      @preserve_order opts[:preserve_order] || false
      @compile {:inline, cstring: 1, framed: 1, copy_key: 2}
      @max_unix_time ~U[9999-12-31 23:59:59.999Z]
      @min_unix_time ~U[-9999-01-01 00:00:00.000Z]
      @unix_start DateTime.to_unix(@min_unix_time, :millisecond)
      @unix_end DateTime.to_unix(@max_unix_time, :millisecond)

      def decode(binary) do
        {map, ""} = document(binary)
        map
      end

      def documents(binary) do
        documents(binary, [])
      end

      def documents("", acc) do
        Enum.reverse(acc)
      end

      def documents(binary, acc) do
        {doc, rest} = document(binary)
        documents(rest, [doc | acc])
      end

      @impl BSON.LazyDocuments
      def document(binary) do
        {doc, rest} = framed(binary)
        {doc_fields(doc, []), rest}
      end

      # Splits a length-prefixed, null-terminated document or array into its body and the rest.
      defp framed(<<size::int32(), rest::binary>>) do
        size = size - 5
        <<body::binary(size), 0x00, rest::binary>> = rest
        {body, rest}
      end

      defp doc_fields(<<type, rest::binary>>, acc) do
        {key, rest} = cstring(rest)
        {value, rest} = type(type, rest)

        doc_fields(rest, [{key, value} | acc])
      end

      defp doc_fields("", acc) do
        to_map(acc)
      end

      if @preserve_order == false do
        defp to_map(acc) do
          Map.new(acc)
        end
      else
        defp to_map(acc) do
          acc
          |> Map.new()
          |> Map.put(@preserve_order, Enum.map(acc, fn {key, _value} -> key end) |> Enum.reverse())
        end
      end

      @doc """
      Decodes a command reply like `document/1`, but leaves the documents of a cursor
      batch (`cursor.firstBatch` or `cursor.nextBatch`) undecoded in a
      `BSON.LazyDocuments`, to be decoded by `decoder` (see `BSON.LazyDocuments`).
      """
      # Decoding a large batch up front keeps every document alive on the heap at once,
      # and the garbage collector then spends most of the time copying them. A stream
      # decodes them a few at a time instead.
      @impl BSON.LazyDocuments
      def lazy_cursor_document(binary, decoder \\ __MODULE__) do
        reply_document(binary, :reply, decoder)
      end

      defp reply_document(binary, level, decoder) do
        {doc, rest} = framed(binary)
        {reply_fields(doc, [], level, decoder), rest}
      end

      defp reply_fields(<<type, rest::binary>>, acc, level, decoder) do
        {key, rest} = cstring(rest)

        {value, rest} =
          case {level, type, key} do
            {:reply, @type_document, "cursor"} -> reply_document(rest, :cursor, decoder)
            {:cursor, @type_array, batch} when batch in ["firstBatch", "nextBatch"] -> lazy_list(rest, decoder)
            _other -> type(type, rest)
          end

        reply_fields(rest, [{key, value} | acc], level, decoder)
      end

      defp reply_fields("", acc, _level, _decoder) do
        to_map(acc)
      end

      defp lazy_list(binary, decoder) do
        {list, rest} = framed(binary)

        case list do
          "" -> {[], rest}
          list -> {%BSON.LazyDocuments{binary: list, decoder: decoder}, rest}
        end
      end

      defp list(binary) do
        {list, rest} = framed(binary)
        {list_elems(list, []), rest}
      end

      defp list_elems(<<type, rest::binary>>, acc) do
        rest = skip_cstring(rest)
        {value, rest} = type(type, rest)

        list_elems(rest, [value | acc])
      end

      defp list_elems("", acc) do
        Enum.reverse(acc)
      end

      # Scans for the terminating null byte without building a list per key. A key of up to
      # 64 bytes is already copied by binary_part/3; a longer one would be a sub-binary that
      # keeps the (possibly huge) reply binary alive, so it is copied explicitly.
      defp cstring(binary), do: cstring(binary, binary, 0)

      defp cstring(<<0x00, rest::binary>>, orig, len) do
        {copy_key(binary_part(orig, 0, len), len), rest}
      end

      defp cstring(<<_, rest::binary>>, orig, len), do: cstring(rest, orig, len + 1)

      defp copy_key(key, len) when len > 64, do: :binary.copy(key)
      defp copy_key(key, _len), do: key

      defp skip_cstring(<<0x00, rest::binary>>), do: rest
      defp skip_cstring(<<_, rest::binary>>), do: skip_cstring(rest)

      defp subtype(0x00), do: :generic
      defp subtype(0x01), do: :function
      defp subtype(0x02), do: :binary_old
      defp subtype(0x03), do: :uuid_old
      defp subtype(0x04), do: :uuid
      defp subtype(0x05), do: :md5
      defp subtype(int) when is_integer(int) and int in 0x80..0xFF, do: int

      defp type(@type_string, <<size::int32(), rest::binary>>) do
        size = size - 1
        <<string::binary(size), 0x00, rest::binary>> = rest
        {string, rest}
      end

      defp type(@type_document, binary) do
        document(binary)
      end

      defp type(@type_array, binary) do
        list(binary)
      end

      defp type(@type_binary, <<_size::int32(), subtype, length::int32(), binary::binary(length), rest::binary>>) when subtype == 0x02 do
        subtype = subtype(subtype)
        {%BSON.Binary{binary: binary, subtype: subtype}, rest}
      end

      defp type(@type_binary, <<size::int32(), subtype, binary::binary(size), rest::binary>>) do
        subtype = subtype(subtype)
        {%BSON.Binary{binary: binary, subtype: subtype}, rest}
      end

      defp type(@type_objectid, <<binary::binary(12), rest::binary>>) do
        {%BSON.ObjectId{value: binary}, rest}
      end

      defp type(@type_bool, <<0x00, rest::binary>>) do
        {false, rest}
      end

      defp type(@type_bool, <<0x01, rest::binary>>) do
        {true, rest}
      end

      defp type(@type_datetime, <<unix_ms::int64(), rest::binary>>) do
        cond do
          unix_ms < @unix_start ->
            Logger.warning("Invalid unix time: #{inspect(unix_ms)}, converted to #{inspect(@min_unix_time)}")
            {@min_unix_time, rest}

          unix_ms > @unix_end ->
            Logger.warning("Invalid unix time: #{inspect(unix_ms)}, converted to #{inspect(@max_unix_time)}")
            {@max_unix_time, rest}

          true ->
            {DateTime.from_unix!(unix_ms, :millisecond), rest}
        end
      end

      defp type(@type_undefined, rest) do
        {nil, rest}
      end

      defp type(@type_null, rest) do
        {nil, rest}
      end

      defp type(@type_regex, binary) do
        {pattern, rest} = cstring(binary)
        {options, rest} = cstring(rest)
        {%BSON.Regex{pattern: pattern, options: options}, rest}
      end

      defp type(@type_js, binary) do
        {code, rest} = type(@type_string, binary)
        {%BSON.JavaScript{code: code}, rest}
      end

      defp type(@type_symbol, binary) do
        type(@type_string, binary)
      end

      defp type(@type_js_scope, <<size::int32(), binary::binary>>) do
        size = size - 4
        <<binary::binary(size), rest::binary>> = binary
        {code, binary} = type(@type_string, binary)
        {scope, ""} = document(binary)
        {%BSON.JavaScript{code: code, scope: scope}, rest}
      end

      defp type(@type_int32, <<int::int32(), rest::binary>>) do
        {int, rest}
      end

      defp type(@type_timestamp, <<ordinal::int32(), epoch::int32(), rest::binary>>) do
        {%BSON.Timestamp{value: epoch, ordinal: ordinal}, rest}
      end

      defp type(@type_int64, <<int::int64(), rest::binary>>) do
        {int, rest}
      end

      defp type(@type_decimal128, <<bits::binary-size(16), rest::binary>>) do
        {Decimal128.decode(bits), rest}
      end

      defp type(@type_float, <<0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), rest::binary>>) do
        {:inf, rest}
      end

      defp type(@type_float, <<0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 255::little-integer-size(8), rest::binary>>) do
        {:"-inf", rest}
      end

      defp type(@type_float, <<0, 0, 0, 0, 0, 0, 248::little-integer-size(8), 127::little-integer-size(8), rest::binary>>) do
        {:NaN, rest}
      end

      defp type(@type_float, <<0, 0, 0, 0, 0, 0, 248::little-integer-size(8), 255::little-integer-size(8), rest::binary>>) do
        {:NaN, rest}
      end

      defp type(@type_float, <<1, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), rest::binary>>) do
        {:NaN, rest}
      end

      defp type(@type_float, <<float::little-float64(), rest::binary>>) do
        {float, rest}
      end

      defp type(@type_min, rest) do
        {:BSON_min, rest}
      end

      defp type(@type_max, rest) do
        {:BSON_max, rest}
      end
    end
  end
end

defmodule BSON.Decoder do
  # This module provides functions for decoding BSON data into Elixir values.
  # The data type conversions are documented at https://hexdocs.pm/mongodb_driver/readme.html#data-representation.

  @moduledoc false

  use BSON.DecoderGenerator, preserve_order: false
end

defmodule BSON.PreserveOrderDecoder do
  # This module is like `BSON.Decoder`, but it retains the original order of
  # document keys in a list.

  @moduledoc false

  use BSON.DecoderGenerator, preserve_order: :__order__
end
