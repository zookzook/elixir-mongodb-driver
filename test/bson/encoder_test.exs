defmodule BSON.EncoderTest do
  use ExUnit.Case, async: true

  test "return error in the case of encoder issues" do
    assert_raise Mongo.Error, fn -> %{message: "invalid document: {:error, \"some error\"}"} = BSON.encode(%{"field" => {:error, "some error"}}) end
  end

  test "while decoding use max unix time range for invalid time ranges" do
    assert %{"ts" => ~U[9999-12-31 23:59:59.999Z]} == BSON.decode([<<17, 0, 0, 0>>, ["", 9, ["ts", 0], <<6_312_846_085_200_000::signed-little-64>>], 0])
    assert %{"ts" => ~U[-9999-01-01 00:00:00.000Z]} == BSON.decode([<<17, 0, 0, 0>>, ["", 9, ["ts", 0], <<-6_312_846_085_200_000::signed-little-64>>], 0])
  end

  test "encodes a large map in the order Enum iterates it" do
    map = Map.new(1..100, fn i -> {"k#{i}", i} end)
    keys = map |> BSON.encode() |> IO.iodata_to_binary() |> BSON.PreserveOrderDecoder.decode() |> Map.get(:__order__)
    assert keys == Enum.map(map, fn {key, _} -> key end)
  end

  test "encodes arrays longer than the precomputed index keys" do
    list = Enum.to_list(1..1500)
    assert %{"a" => ^list} = %{"a" => list} |> BSON.encode() |> BSON.decode()
  end

  test "encodes datetimes with offsets and before 1970 like DateTime.to_unix/2" do
    for dt <- [~U[1969-12-31 23:59:59.999999Z], ~U[0001-01-01 00:00:00Z], %{~U[2020-01-01 10:00:00.123Z] | utc_offset: 3600, std_offset: 3600, time_zone: "X/Y", zone_abbr: "X"}] do
      <<_::binary-size(4), 9, "d", 0, ms::signed-little-64, 0>> = %{"d" => dt} |> BSON.encode() |> IO.iodata_to_binary()
      assert ms == DateTime.to_unix(dt, :millisecond)
    end
  end

  test "a struct without an encoder or Enumerable raises Protocol.UndefinedError" do
    assert_raise Protocol.UndefinedError, fn -> BSON.encode(%{"x" => %URI{host: "h"}}) end
  end

  test "mixing atom and string keys raises" do
    assert_raise Mongo.Error, fn -> BSON.encode(%{"a" => %{:b => 1, "c" => 2}}) end
  end

  test "encodes large strings and binaries at any depth" do
    big = :binary.copy("b", 70_000)
    doc = %{"a" => %{"b" => [1, %{"s" => big, "bin" => %BSON.Binary{binary: big}, "old" => %BSON.Binary{binary: big, subtype: :binary_old}}, "x"]}, "z" => 1}
    decoded = doc |> BSON.encode() |> BSON.decode()
    assert decoded["z"] == 1
    assert [1, %{"s" => ^big, "bin" => %BSON.Binary{binary: ^big}, "old" => %BSON.Binary{binary: ^big, subtype: :binary_old}}, "x"] = decoded["a"]["b"]
  end

  test "regex pattern and options are iodata" do
    assert %{"r" => %BSON.Regex{pattern: "A", options: "i"}} == %{"r" => %BSON.Regex{pattern: 65, options: [105]}} |> BSON.encode() |> BSON.decode()
  end

  test "an array holding a tuple raises FunctionClauseError" do
    assert_raise FunctionClauseError, fn -> BSON.encode(%{"a" => [self(), {1, 2}]}) end
  end
end
