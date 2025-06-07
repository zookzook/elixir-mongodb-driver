defmodule BSON.EncoderTest do
  use ExUnit.Case, async: true

  test "return error in the case of encoder issues" do
    assert_raise Mongo.Error, fn -> %{message: "invalid document: {:error, \"some error\"}"} = BSON.encode(%{"field" => {:error, "some error"}}) end
  end

  test "while decoding use max unix time range for invalid time ranges" do
    assert %{"ts" => ~U[9999-12-31 23:59:59.999Z]} == BSON.decode([<<17, 0, 0, 0>>, ["", 9, ["ts", 0], <<6_312_846_085_200_000::signed-little-64>>], 0])
    assert %{"ts" => ~U[-9999-01-01 00:00:00.000Z]} == BSON.decode([<<17, 0, 0, 0>>, ["", 9, ["ts", 0], <<-6_312_846_085_200_000::signed-little-64>>], 0])
  end
end
