defmodule Mongo.CompressorTest do
  use ExUnit.Case

  alias Mongo.Compressor

  test "the map_compressors should filter unsupported compressors" do
    assert [:zstd, :zlib] = Compressor.map_compressors(["snappy", "zstd", "zlib"])
  end

  test "the map_compressors should return [] if no compressor is supported]" do
    assert [] = Compressor.map_compressors(["snappy"])
  end
end
