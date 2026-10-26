defmodule BSON.Encoder do
  @moduledoc false
  use BSON.Utils

  # Precomputed array index keys "0".."1023".
  @index_keys 0..1023 |> Enum.map(&Integer.to_string/1) |> List.to_tuple()
  @index_count tuple_size(@index_keys)

  @unix_epoch_days 719_528

  # Strings and binaries this large are kept as separate iodata parts instead of
  # being copied into the document, and into each enclosing document.
  @large_leaf 65_536

  def document(doc), do: wrap(body(doc))

  def encode(value) when is_binary(value), do: [<<byte_size(value) + 1::int32()>>, value, 0x00]
  def encode(true), do: 0x01
  def encode(false), do: 0x00
  def encode(nil), do: <<>>
  def encode(:BSON_min), do: <<>>
  def encode(:BSON_max), do: <<>>
  def encode(%BSON.JavaScript{code: code, scope: nil}), do: encode(code)
  def encode([]), do: wrap(body([]))
  def encode([{_, _} | _] = value), do: wrap(body(value))
  def encode(value) when is_list(value), do: wrap(array(value))
  def encode(value) when is_map(value) and not is_struct(value), do: wrap(body(value))

  def encode(value) do
    # Encode the value as a field with an empty key, which puts its type byte and
    # a 0 byte in front of the payload, and strip those two bytes.
    case put(<<>>, <<>>, value) do
      <<_type, 0, payload::binary>> -> payload
      [[<<_type, 0, head::binary>> | prefix], tail] -> [[head | prefix], tail]
    end
  end

  # A document's fields plus its trailing null byte; the caller prefixes the size.
  # The fields are encoded into one binary, or into [prefix, tail] once a large
  # leaf is met: prefix is iodata and tail the binary later fields are added to.
  defp body(doc) when is_list(doc), do: fields(doc, :unknown, <<>>, doc)
  defp body(doc) when is_map(doc) and not is_struct(doc), do: fields(map_fields(doc), :unknown, <<>>, doc)
  # Anything else is read through Enumerable, so a struct that isn't Enumerable raises Protocol.UndefinedError.
  defp body(doc), do: fields(Enum.to_list(doc), :unknown, <<>>, doc)

  # Fields in the order Enum.reduce/3 visits them, as before. A map of up to 32 keys
  # is stored sorted, so :maps.to_list/1 gives that order; for a larger map it can
  # differ from :maps.fold/3, which is what Enum.reduce/3 uses (e.g. on OTP 27).
  defp map_fields(doc) when map_size(doc) <= 32, do: :maps.to_list(doc)
  defp map_fields(doc), do: :lists.reverse(:maps.fold(fn key, value, acc -> [{key, value} | acc] end, [], doc))

  defp wrap(body), do: [<<size(body) + 4::int32()>>, body]

  defp size(body) when is_binary(body), do: byte_size(body)
  defp size(body), do: IO.iodata_length(body)

  # After a large leaf, encode the rest on top of the tail.
  defp fields(list, state, [prefix, tail], doc), do: join(prefix, fields(list, state, tail, doc))
  defp fields([], _state, acc, _doc), do: <<acc::binary, 0x00>>
  defp fields([{:__struct__, _} | _], :binary, _acc, doc), do: invalid_doc(doc)
  defp fields([{:__struct__, _} | rest], _state, acc, doc), do: fields(rest, :atom, acc, doc)
  defp fields([{key, _} | _], :binary, _acc, doc) when is_atom(key), do: invalid_doc(doc)
  defp fields([{key, _} | _], :atom, _acc, doc) when is_binary(key), do: invalid_doc(doc)

  defp fields([{key, value} | rest], _state, acc, doc) when is_atom(key) do
    fields(rest, :atom, field(acc, Atom.to_string(key), value), doc)
  end

  defp fields([{key, value} | rest], _state, acc, doc) when is_binary(key) do
    fields(rest, :binary, field(acc, key, value), doc)
  end

  defp join(prefix, body) when is_binary(body), do: [prefix, body]
  defp join(prefix, [more, tail]), do: [[prefix | more], tail]

  defp field(acc, key, value) do
    case Mongo.Encoder.impl_for(value) do
      nil -> put(acc, key, value)
      _ -> put(acc, key, Mongo.Encoder.encode(value))
    end
  end

  defp put(acc, key, value) when is_binary(value) do
    case byte_size(value) do
      size when size < @large_leaf -> <<acc::binary, @type_string, key::binary, 0x00, size + 1::int32(), value::binary, 0x00>>
      size -> [[<<acc::binary, @type_string, key::binary, 0x00, size + 1::int32()>>, value], <<0x00>>]
    end
  end

  defp put(acc, key, value) when is_int32(value), do: <<acc::binary, @type_int32, key::binary, 0x00, value::int32()>>
  defp put(acc, key, value) when is_float(value), do: <<acc::binary, @type_float, key::binary, 0x00, value::little-float64()>>
  defp put(acc, key, true), do: <<acc::binary, @type_bool, key::binary, 0x00, 0x01>>
  defp put(acc, key, false), do: <<acc::binary, @type_bool, key::binary, 0x00, 0x00>>
  defp put(acc, key, nil), do: <<acc::binary, @type_null, key::binary, 0x00>>

  defp put(acc, key, %BSON.ObjectId{value: <<_::binary-size(12)>> = value}),
    do: <<acc::binary, @type_objectid, key::binary, 0x00, value::binary>>

  defp put(acc, key, %DateTime{} = dt), do: <<acc::binary, @type_datetime, key::binary, 0x00, datetime_ms(dt)::int64()>>
  defp put(acc, key, value) when is_int64(value), do: <<acc::binary, @type_int64, key::binary, 0x00, value::int64()>>

  defp put(acc, key, value) when is_map(value) and not is_struct(value),
    do: put_doc(acc, @type_document, key, body(value))

  defp put(acc, key, []), do: <<acc::binary, @type_array, key::binary, 0x00, 5::int32(), 0x00>>
  defp put(acc, key, [{_, _} | _] = value), do: put_doc(acc, @type_document, key, body(value))
  defp put(acc, key, value) when is_list(value), do: put_doc(acc, @type_array, key, array(value))

  defp put(acc, key, %NaiveDateTime{} = dt), do: <<acc::binary, @type_datetime, key::binary, 0x00, naive_ms(dt)::int64()>>

  defp put(acc, key, %Date{} = date), do: <<acc::binary, @type_datetime, key::binary, 0x00, date_ms(date)::int64()>>

  defp put(acc, key, %BSON.LongNumber{value: value}), do: <<acc::binary, @type_int64, key::binary, 0x00, value::int64()>>
  defp put(acc, key, %Decimal{} = value), do: <<acc::binary, @type_decimal128, key::binary, 0x00, BSON.Decimal128.encode(value)::binary>>

  defp put(acc, key, %BSON.Binary{binary: binary, subtype: :binary_old}) do
    binary = IO.iodata_to_binary(binary)
    size = byte_size(binary)
    header = <<acc::binary, @type_binary, key::binary, 0x00, size + 4::int32(), 0x02, size::int32()>>
    put_binary(header, binary)
  end

  defp put(acc, key, %BSON.Binary{binary: binary, subtype: subtype}) do
    subtype = subtype(subtype)
    binary = IO.iodata_to_binary(binary)
    put_binary(<<acc::binary, @type_binary, key::binary, 0x00, byte_size(binary)::int32(), subtype>>, binary)
  end

  defp put(acc, key, %BSON.Regex{pattern: pattern, options: options}),
    do: <<acc::binary, @type_regex, key::binary, 0x00, IO.iodata_to_binary([pattern, 0x00, options, 0x00])::binary>>

  defp put(acc, key, %BSON.JavaScript{code: code, scope: nil}),
    do: <<acc::binary, @type_js, key::binary, 0x00, IO.iodata_to_binary([encode(code)])::binary>>

  defp put(acc, key, %BSON.JavaScript{code: code, scope: scope}) do
    code = IO.iodata_to_binary([encode(code)])
    scope = scope |> body() |> IO.iodata_to_binary()
    size = byte_size(code) + byte_size(scope) + 8
    <<acc::binary, @type_js_scope, key::binary, 0x00, size::int32(), code::binary, byte_size(scope) + 4::int32(), scope::binary>>
  end

  defp put(acc, key, %BSON.Timestamp{value: epoch, ordinal: ordinal}),
    do: <<acc::binary, @type_timestamp, key::binary, 0x00, ordinal::int32(), epoch::int32()>>

  # Any other map is read as an embedded document. A struct that isn't
  # Enumerable, such as an ObjectId with a bad value, raises Protocol.UndefinedError
  # there; the ObjectId clause only keeps upstream's type byte.
  defp put(acc, key, %BSON.ObjectId{} = value), do: put_doc(acc, @type_objectid, key, body(value))
  defp put(acc, key, value) when is_map(value), do: put_doc(acc, @type_document, key, body(value))

  defp put(acc, key, :BSON_min), do: <<acc::binary, @type_min, key::binary, 0x00>>
  defp put(acc, key, :BSON_max), do: <<acc::binary, @type_max, key::binary, 0x00>>
  defp put(acc, key, :inf), do: <<acc::binary, @type_float, key::binary, 0x00, 0, 0, 0, 0, 0, 0, 240, 127>>
  defp put(acc, key, :"-inf"), do: <<acc::binary, @type_float, key::binary, 0x00, 0, 0, 0, 0, 0, 0, 240, 255>>
  defp put(acc, key, :NaN), do: <<acc::binary, @type_float, key::binary, 0x00, 0, 0, 0, 0, 0, 0, 248, 127>>
  defp put(acc, key, value) when is_atom(value), do: put(acc, key, Atom.to_string(value))

  defp put(_acc, _key, value), do: raise(Mongo.Error.exception("invalid document: #{inspect(value)}"))

  defp put_doc(acc, type, key, body) when is_binary(body),
    do: <<acc::binary, type, key::binary, 0x00, byte_size(body) + 4::int32(), body::binary>>

  defp put_doc(acc, type, key, [prefix, tail] = body),
    do: [[<<acc::binary, type, key::binary, 0x00, IO.iodata_length(body) + 4::int32()>> | prefix], tail]

  defp put_binary(header, binary) when byte_size(binary) >= @large_leaf, do: [[header, binary], <<>>]
  defp put_binary(header, binary), do: <<header::binary, binary::binary>>

  # Like upstream, a list holding a tuple, or an improper list, raises
  # FunctionClauseError before any element is encoded.
  defp array(list), do: items(no_tuples(list, list), 0, <<>>)

  defp no_tuples([], list), do: list
  defp no_tuples([hd | tl], list) when not is_tuple(hd), do: no_tuples(tl, list)

  defp items(list, ix, [prefix, tail]), do: join(prefix, items(list, ix, tail))
  defp items([], _ix, acc), do: <<acc::binary, 0x00>>
  defp items([hd | tl], ix, acc), do: items(tl, ix + 1, field(acc, index_key(ix), hd))

  defp index_key(ix) when ix < @index_count, do: elem(@index_keys, ix)
  defp index_key(ix), do: Integer.to_string(ix)

  defp datetime_ms(%DateTime{calendar: Calendar.ISO, year: y, month: mo, day: d, hour: h, minute: mi, second: s, microsecond: {us, _}, utc_offset: uo, std_offset: so})
       when is_integer(y) and y >= 0 and mo in 1..12 and d in 1..28 and is_integer(us) and us >= 0 do
    days = :calendar.date_to_gregorian_days(y, mo, d) - @unix_epoch_days
    (days * 86_400 + h * 3600 + mi * 60 + s - uo - so) * 1000 + div(us, 1000)
  end

  # Days 29-31, negative years, other calendars and invalid microseconds take
  # the slower path, which also validates the date.
  defp datetime_ms(dt), do: DateTime.to_unix(dt, :millisecond)

  defp naive_ms(%NaiveDateTime{calendar: Calendar.ISO, year: y, month: mo, day: d, hour: h, minute: mi, second: s, microsecond: {us, _}})
       when is_integer(y) and y >= 0 and mo in 1..12 and d in 1..28 and is_integer(us) and us >= 0 do
    days = :calendar.date_to_gregorian_days(y, mo, d) - @unix_epoch_days
    (days * 86_400 + h * 3600 + mi * 60 + s) * 1000 + div(us, 1000)
  end

  defp naive_ms(dt), do: dt |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:millisecond)

  defp date_ms(%Date{calendar: Calendar.ISO, year: y, month: mo, day: d}) when is_integer(y) and y >= 0 and mo in 1..12 and d in 1..28,
    do: (:calendar.date_to_gregorian_days(y, mo, d) - @unix_epoch_days) * 86_400_000

  defp date_ms(date) do
    NaiveDateTime.from_erl!({Date.to_erl(date), {0, 0, 0}}, 0, Calendar.ISO)
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_unix(:millisecond)
  end

  defp invalid_doc(doc) do
    raise Mongo.Error.exception("invalid document containing atom and string keys: #{inspect(doc)}")
  end

  defp subtype(:generic), do: 0x00
  defp subtype(:function), do: 0x01
  defp subtype(:uuid_old), do: 0x03
  defp subtype(:uuid), do: 0x04
  defp subtype(:md5), do: 0x05
  defp subtype(int) when is_integer(int) and int in 0x80..0xFF, do: 0x80
end
