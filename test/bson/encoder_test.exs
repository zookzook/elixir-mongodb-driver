defmodule BSON.EncoderTest do
  use ExUnit.Case, async: true

  test "return error in the case of encoder issues" do
    assert_raise Mongo.Error, fn -> %{message: "invalid document: {:error, \"some error\"}"} = BSON.encode(%{"field" => {:error, "some error"}}) end
  end
end
