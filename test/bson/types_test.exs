defmodule BSON.TypesTest do
  use ExUnit.Case, async: true

  test "inspect BSON.Binary" do
    value = %BSON.Binary{binary: <<1, 2, 3>>}
    assert inspect(value) == "#BSON.Binary<010203>"

    value = %BSON.Binary{binary: <<132, 142, 144, 233, 87, 80, 78, 10, 171, 115, 102, 172, 107, 50, 130, 66>>, subtype: :uuid}
    assert inspect(value) == "#BSON.UUID<848e90e9-5750-4e0a-ab73-66ac6b328242>"
  end

  @objectid %BSON.ObjectId{value: <<29, 32, 69, 244, 101, 119, 228, 28, 61, 24, 21, 215>>}
  @string "1d2045f46577e41c3d1815d7"
  @string_uppercase "1D2045F46577E41C3D1815D7"
  @timestamp DateTime.from_unix!(488_654_324)

  test "inspect BSON.ObjectId" do
    assert inspect(@objectid) == "#BSON.ObjectId<#{@string}>"
  end

  if Version.match?(System.version(), "<= 1.8.0") do
    test "BSON.ObjectId.encode!/1" do
      assert BSON.ObjectId.encode!(@objectid) == @string

      assert_raise FunctionClauseError, fn ->
        BSON.ObjectId.encode!("")
      end
    end
  else
    test "BSON.ObjectId.encode!/1" do
      assert BSON.ObjectId.encode!(@objectid) == @string
    end
  end

  test "BSON.ObjectId.decode!/1" do
    assert BSON.ObjectId.decode!(@string) == @objectid

    assert_raise FunctionClauseError, fn ->
      BSON.ObjectId.decode!("")
    end
  end

  test "BSON.ObjectId.decode!/1 for uppercase HEX" do
    assert BSON.ObjectId.decode!(@string_uppercase) == @objectid

    assert_raise FunctionClauseError, fn ->
      BSON.ObjectId.decode!("")
    end
  end

  test "BSON.ObjectId.encode/1" do
    assert BSON.ObjectId.encode(@objectid) == {:ok, @string}
    assert BSON.ObjectId.encode("") == :error
  end

  test "BSON.ObjectId.decode/1" do
    assert BSON.ObjectId.decode(@string) == {:ok, @objectid}
    assert BSON.ObjectId.decode("") == :error
  end

  test "to_string BSON.ObjectId" do
    assert to_string(@objectid) == @string
  end

  if Version.match?(System.version(), "<= 1.8.0") do
    test "BSON.ObjectId.get_timestamp!/1" do
      value = BSON.ObjectId.get_timestamp!(@objectid)
      assert DateTime.compare(value, @timestamp) == :eq

      assert_raise FunctionClauseError, fn ->
        BSON.ObjectId.get_timestamp!("")
      end
    end
  else
    test "BSON.ObjectId.get_timestamp!/1" do
      value = BSON.ObjectId.get_timestamp!(@objectid)
      assert DateTime.compare(value, @timestamp) == :eq
    end
  end

  test "BSON.ObjectId.get_timestamp/1" do
    assert {:ok, value} = BSON.ObjectId.get_timestamp(@objectid)
    assert DateTime.compare(value, @timestamp) == :eq
    assert BSON.ObjectId.get_timestamp("") == :error
  end

  test "inspect BSON.Regex" do
    value = %BSON.Regex{pattern: "abc"}
    assert inspect(value) == "#BSON.Regex<\"abc\", \"\">"

    value = %BSON.Regex{pattern: "abc", options: "i"}
    assert inspect(value) == "#BSON.Regex<\"abc\", \"i\">"
  end

  test "inspect BSON.JavaScript" do
    value = %BSON.JavaScript{code: "this === null"}
    assert inspect(value) == "#BSON.JavaScript<\"this === null\">"

    value = %BSON.JavaScript{code: "this === value", scope: %{value: nil}}
    assert inspect(value) == "#BSON.JavaScript<\"this === value\", %{value: nil}>"
  end

  test "inspect BSON.Timestamp" do
    value = %BSON.Timestamp{value: 1_412_180_887, ordinal: 12}
    assert inspect(value) == "#BSON.Timestamp<1412180887:12>"

    {:ok, datetime} = DateTime.now("Etc/UTC")
    date_1 = %BSON.Timestamp{value: DateTime.to_unix(datetime), ordinal: 1}
    date_2 = %BSON.Timestamp{value: DateTime.to_unix(DateTime.add(datetime, 10)), ordinal: 1}

    assert BSON.Timestamp.is_after(date_1, date_2) == false
    assert BSON.Timestamp.is_before(date_1, date_2) == true
  end

  test "inspect BSON.LongNumber" do
    value = %BSON.LongNumber{value: 1_412_180_887}
    assert inspect(value) == "#BSON.LongNumber<1412180887>"
  end
end
