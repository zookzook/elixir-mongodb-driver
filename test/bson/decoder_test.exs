defmodule BSON.DecoderTest.CustomPreserveOrderDecoder do
  use BSON.DecoderGenerator, preserve_order: :original_order
end

defmodule BSON.DecoderTest do
  use ExUnit.Case, async: true

  # {
  #   "key1": {
  #     "a": 1,
  #     "b": 2,
  #     "c": 3
  #   },
  #   "key2": {
  #     "x": 4,
  #     "y": 5
  #   }
  # }
  @bson_document <<62, 0, 0, 0, 3, 107, 101, 121, 49, 0, 26, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 16, 98, 0, 2, 0, 0, 0, 16, 99, 0, 3, 0, 0, 0, 0, 3, 107, 101, 121, 50, 0, 19, 0, 0, 0, 16, 120, 0, 4, 0, 0, 0, 16, 121, 0, 5, 0, 0, 0, 0, 0>>

  describe "BSON.Decoder.decode/1" do
    test "decodes binary data into a map" do
      assert BSON.Decoder.decode(@bson_document) == %{
               "key1" => %{
                 "a" => 1,
                 "b" => 2,
                 "c" => 3
               },
               "key2" => %{
                 "x" => 4,
                 "y" => 5
               }
             }
    end
  end

  describe "BSON.PreserveOrderDecoder.decode/1" do
    test "decodes binary data into a map with :__order__" do
      assert BSON.PreserveOrderDecoder.decode(@bson_document) == %{
               "key1" => %{
                 "a" => 1,
                 "b" => 2,
                 "c" => 3,
                 __order__: ["a", "b", "c"]
               },
               "key2" => %{
                 "x" => 4,
                 "y" => 5,
                 __order__: ["x", "y"]
               },
               __order__: ["key1", "key2"]
             }
    end

    test "decodes binary data into a map with custom key" do
      assert BSON.DecoderTest.CustomPreserveOrderDecoder.decode(@bson_document) == %{
               "key1" => %{
                 "a" => 1,
                 "b" => 2,
                 "c" => 3,
                 original_order: ["a", "b", "c"]
               },
               "key2" => %{
                 "x" => 4,
                 "y" => 5,
                 original_order: ["x", "y"]
               },
               original_order: ["key1", "key2"]
             }
    end
  end
end
