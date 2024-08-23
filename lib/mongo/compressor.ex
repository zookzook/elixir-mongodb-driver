defmodule Mongo.Compressor do
  @moduledoc false

  @zlib_compressor_id 2
  if Code.ensure_loaded?(:ezstd) do
    @zstd_compressor_id 3
  end

  def compress(binary, :zlib) do
    {@zlib_compressor_id, :zlib.compress(binary)}
  end

  if Code.ensure_loaded?(:ezstd) do
    def compress(binary, :zstd) when is_binary(binary) do
      {@zstd_compressor_id, :ezstd.compress(binary)}
    end

    def compress(iodata, :zstd) when is_list(iodata) do
      {@zstd_compressor_id,
       iodata
       |> IO.iodata_to_binary()
       |> :ezstd.compress()}
    end
  end

  def uncompress(binary, @zlib_compressor_id) do
    :zlib.uncompress(binary)
  end

  if Code.ensure_loaded?(:ezstd) do
    def uncompress(binary, @zstd_compressor_id) do
      :ezstd.decompress(binary)
    end
  end

  def uncompress(binary, :zlib) do
    :zlib.uncompress(binary)
  end

  if Code.ensure_loaded?(:ezstd) do
    def uncompress(binary, :zstd) do
      :ezstd.decompress(binary)
    end
  end
end
