defmodule BSON.UUIDTest do
  use ExUnit.Case

  test "converting uuids" do
    assert %BSON.Binary{binary: <<132, 142, 144, 233, 87, 80, 78, 10, 171, 115, 102, 172, 107, 50, 130, 66>>, subtype: :uuid} = Mongo.uuid!("848e90e9-5750-4e0a-ab73-66ac6b328242")
    assert_raise ArgumentError, fn -> Mongo.uuid!("848e90e9-5750-4e0a-ab73-66ac6b328242x") end
    assert_raise ArgumentError, fn -> Mongo.uuid!("848e90e9-5750-4e0a-ab73-66ac6-328242") end

    assert {:ok, %BSON.Binary{binary: <<132, 142, 144, 233, 87, 80, 78, 10, 171, 115, 102, 172, 107, 50, 130, 66>>, subtype: :uuid}} = Mongo.uuid("848e90e9-5750-4e0a-ab73-66ac6b328242")
    assert {:error, %ArgumentError{}} = Mongo.uuid("848e90e9-5750-4e0a-ab73-66ac6b328242x")
    assert {:error, %ArgumentError{}} = Mongo.uuid("848e90e9-5750-4e0a-ab73-66ac6-328242")
  end

  test "creating uudis" do
    assert %BSON.Binary{binary: _value, subtype: :uuid} = value_1 = Mongo.uuid()
    value_2 = inspect(value_1) |> String.slice(11..46) |> Mongo.uuid!()
    assert value_1 == value_2
  end
end
