defmodule TestUser do
  defstruct name: "John", age: 27
end

defmodule BSONTest do
  use ExUnit.Case, async: true

  import BSON, only: [decode: 1]

  @map_1 %{"hello" => "world"}
  @bin_1 <<22, 0, 0, 0, 2, 104, 101, 108, 108, 111, 0, 6, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0>>

  @map_2 %{"BSON" => ["awesome", 5.05, 1986]}
  @bin_2 <<49, 0, 0, 0, 4, 66, 83, 79, 78, 0, 38, 0, 0, 0, 2, 48, 0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0, 51, 51, 51, 51, 51, 51, 20, 64, 16, 50, 0, 194, 7, 0, 0, 0, 0>>

  @map_3 %{"a" => %{"b" => %{}, "c" => %{"d" => nil}}}
  @bin_3 <<32, 0, 0, 0, 3, 97, 0, 24, 0, 0, 0, 3, 98, 0, 5, 0, 0, 0, 0, 3, 99, 0, 8, 0, 0, 0, 10, 100, 0, 0, 0, 0>>

  @map_4 %{"a" => [], "b" => [1, 2, 3], "c" => [1.1, "2", true]}
  @bin_4 <<74, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 4, 98, 0, 26, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 16, 50, 0, 3, 0, 0, 0, 0, 4, 99, 0, 29, 0, 0, 0, 1, 48, 0, 154, 153, 153, 153, 153, 153, 241, 63, 2, 49, 0, 2, 0, 0, 0, 50, 0, 8,
           50, 0, 1, 0, 0>>

  @map_5 %{"a" => 123.0}
  @bin_5 <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 192, 94, 64, 0>>

  @map_6 %{"b" => "123"}
  @bin_6 <<16, 0, 0, 0, 2, 98, 0, 4, 0, 0, 0, 49, 50, 51, 0, 0>>

  @map_7 %{"c" => %{}}
  @bin_7 <<13, 0, 0, 0, 3, 99, 0, 5, 0, 0, 0, 0, 0>>

  @map_8 %{"d" => []}
  @bin_8 <<13, 0, 0, 0, 4, 100, 0, 5, 0, 0, 0, 0, 0>>

  @map_9 %{"e" => %BSON.Binary{binary: <<1, 2, 3>>, subtype: :generic}}
  @bin_9 <<16, 0, 0, 0, 5, 101, 0, 3, 0, 0, 0, 0, 1, 2, 3, 0>>

  @map_10 %{"f" => %BSON.ObjectId{value: <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11>>}}
  @bin_10 <<20, 0, 0, 0, 7, 102, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0>>

  @map_11 %{"g" => true}
  @bin_11 <<9, 0, 0, 0, 8, 103, 0, 1, 0>>

  @map_12 %{"h" => DateTime.from_unix!(12_345, :millisecond)}
  @bin_12 <<16, 0, 0, 0, 9, 104, 0, 57, 48, 0, 0, 0, 0, 0, 0, 0>>

  @map_13 %{"i" => nil}
  @bin_13 <<8, 0, 0, 0, 10, 105, 0, 0>>

  @map_14 %{"j" => %BSON.JavaScript{code: "1 + 2"}}
  @bin_14 <<18, 0, 0, 0, 13, 106, 0, 6, 0, 0, 0, 49, 32, 43, 32, 50, 0, 0>>

  @map_15 %{"k" => %BSON.JavaScript{code: "a + b", scope: %{"a" => 2, "b" => 2}}}
  @bin_15 <<41, 0, 0, 0, 15, 107, 0, 33, 0, 0, 0, 6, 0, 0, 0, 97, 32, 43, 32, 98, 0, 19, 0, 0, 0, 16, 97, 0, 2, 0, 0, 0, 16, 98, 0, 2, 0, 0, 0, 0, 0>>

  @map_16 %{"l" => 12_345}
  @bin_16 <<12, 0, 0, 0, 16, 108, 0, 57, 48, 0, 0, 0>>

  @map_17 %{"m" => %BSON.Timestamp{value: 1_423_458_185, ordinal: 9}}
  @bin_17 <<16, 0, 0, 0, 17, 109, 0, 9, 0, 0, 0, 137, 63, 216, 84, 0>>

  @map_18 %{"n" => 123_456_789_123_456}
  @bin_18 <<16, 0, 0, 0, 18, 110, 0, 128, 145, 15, 134, 72, 112, 0, 0, 0>>

  @map_19 %{"o" => :BSON_min}
  @bin_19 <<8, 0, 0, 0, 255, 111, 0, 0>>

  @map_20 %{"p" => :BSON_max}
  @bin_20 <<8, 0, 0, 0, 127, 112, 0, 0>>

  @map_21 %{"q" => %BSON.Binary{binary: <<1, 2, 3>>, subtype: :binary_old}}
  @bin_21 <<20, 0, 0, 0, 5, 113, 0, 7, 0, 0, 0, 2, 3, 0, 0, 0, 1, 2, 3, 0>>

  @map_22 %{"regex" => %BSON.Regex{pattern: "acme.*corp", options: "i"}}
  @bin_22 <<25, 0, 0, 0, 11, 114, 101, 103, 101, 120, 0, 97, 99, 109, 101, 46, 42, 99, 111, 114, 112, 0, 105, 0, 0>>

  @map_23 %{"regex" => %BSON.Regex{pattern: "acme.*corp"}}
  @bin_23 <<24, 0, 0, 0, 11, 114, 101, 103, 101, 120, 0, 97, 99, 109, 101, 46, 42, 99, 111, 114, 112, 0, 0, 0>>

  @map_24 %{"number" => %BSON.LongNumber{value: 123}}
  @bin_24 <<21, 0, 0, 0, 18, 110, 117, 109, 98, 101, 114, 0, 123, 0, 0, 0, 0, 0, 0, 0, 0>>

  @map_25 %{"number" => Decimal.new("0.33")}
  @bin_25 <<29, 0, 0, 0, 19, 110, 117, 109, 98, 101, 114, 0, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 60, 48, 0>>

  @map_26 %TestUser{}
  @bin_26 <<29, 0, 0, 0, 16, 97, 103, 101, 0, 27, 0, 0, 0, 2, 110, 97, 109, 101, 0, 5, 0, 0, 0, 74, 111, 104, 110, 0, 0>>

  test "encode" do
    assert encode(@map_1) == @bin_1
    assert encode(@map_2) == @bin_2
    assert encode(@map_3) == @bin_3
    assert encode(@map_4) == @bin_4
    assert encode(@map_5) == @bin_5
    assert encode(@map_6) == @bin_6
    assert encode(@map_7) == @bin_7
    assert encode(@map_8) == @bin_8
    assert encode(@map_9) == @bin_9
    assert encode(@map_10) == @bin_10
    assert encode(@map_11) == @bin_11
    assert encode(@map_12) == @bin_12
    assert encode(@map_13) == @bin_13
    assert encode(@map_14) == @bin_14
    assert encode(@map_15) == @bin_15
    assert encode(@map_16) == @bin_16
    assert encode(@map_17) == @bin_17
    assert encode(@map_18) == @bin_18
    assert encode(@map_19) == @bin_19
    assert encode(@map_20) == @bin_20
    assert encode(@map_21) == @bin_21
    assert encode(@map_22) == @bin_22
    assert encode(@map_23) == @bin_23
    assert encode(@map_24) == @bin_24
    assert encode(@map_25) == @bin_25
    assert encode(@map_26) == @bin_26
  end

  test "decode" do
    assert decode(@bin_1) == @map_1
    assert decode(@bin_2) == @map_2
    assert decode(@bin_3) == @map_3
    assert decode(@bin_4) == @map_4
    assert decode(@bin_5) == @map_5
    assert decode(@bin_6) == @map_6
    assert decode(@bin_7) == @map_7
    assert decode(@bin_8) == @map_8
    assert decode(@bin_9) == @map_9
    assert decode(@bin_10) == @map_10
    assert decode(@bin_11) == @map_11
    assert decode(@bin_12) == @map_12
    assert decode(@bin_13) == @map_13
    assert decode(@bin_14) == @map_14
    assert decode(@bin_15) == @map_15
    assert decode(@bin_16) == @map_16
    assert decode(@bin_17) == @map_17
    assert decode(@bin_18) == @map_18
    assert decode(@bin_19) == @map_19
    assert decode(@bin_20) == @map_20
    assert decode(@bin_21) == @map_21
    assert decode(@bin_22) == @map_22
    assert decode(@bin_23) == @map_23
    assert decode(@bin_24) == %{"number" => 123}
    assert decode(@bin_25) == @map_25
    assert decode(@bin_26) == %{"name" => "John", "age" => 27}
  end

  test "keywords" do
    keyword = [set: [title: "x"]]
    map = %{"set" => %{"title" => "x"}}
    encoded = <<28, 0, 0, 0, 3, 115, 101, 116, 0, 18, 0, 0, 0, 2, 116, 105, 116, 108, 101, 0, 2, 0, 0, 0, 120, 0, 0, 0>>

    assert encode(keyword) == encoded
    assert encode(map) == encoded
    assert decode(encoded) == map
  end

  test "encode atom" do
    assert encode(%{hello: "world"}) == @bin_1
  end

  test "encode atom value" do
    assert encode(%{"hello" => :world}) == @bin_1
  end

  test "decode BSON symbol into string" do
    encoded = <<22, 0, 0, 0, 14, 104, 101, 108, 108, 111, 0, 6, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0>>
    assert decode(encoded) == @map_1
  end

  @map_pos_inf %{"a" => :inf}
  @bin_pos_inf <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), 0>>

  @map_neg_inf %{"a" => :"-inf"}
  @bin_neg_inf <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 255::little-integer-size(8), 0>>

  @map_nan %{"a" => :NaN}
  @bin_nan <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 248::little-integer-size(8), 127::little-integer-size(8), 0>>
  @bin_nan2 <<16, 0, 0, 0, 1, 97, 0, 1, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), 0>>

  test "decode float NaN" do
    assert decode(@bin_nan) == @map_nan
    assert decode(@bin_nan2) == @map_nan
  end

  test "encode float NaN" do
    assert encode(@map_nan) == @bin_nan
  end

  test "decode float positive Infinity" do
    assert decode(@bin_pos_inf) == @map_pos_inf
  end

  test "encode float positive Infinity" do
    assert encode(@map_pos_inf) == @bin_pos_inf
  end

  test "decode float negative Infinity" do
    assert decode(@bin_neg_inf) == @map_neg_inf
  end

  test "encode float negative Infinity" do
    assert encode(@map_neg_inf) == @bin_neg_inf
  end

  test "mixing atoms with binaries" do
    document = 1..33 |> Enum.reduce(%{}, fn x, acc -> Map.put(acc, to_string(x), x) end) |> Map.put(:a, 10)
    assert_raise ArgumentError, fn -> encode(document) end
    document = %{:key => "value", "id" => 10}
    assert_raise ArgumentError, fn -> encode(document) end
    document = 1..33 |> Enum.reduce(%{}, fn x, acc -> Map.put(acc, to_string(x), x) end) |> Map.put(:__struct__, TestUser)
    assert_raise ArgumentError, fn -> encode(document) end
  end

  defp encode(value) do
    value |> BSON.encode() |> IO.iodata_to_binary()
  end
end
