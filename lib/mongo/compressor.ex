defmodule Mongo.Compressor do
  @moduledoc false

  @zlib_compressor_id 2
  @zstd_module Enum.find([:zstd, :ezstd], &Code.ensure_loaded?/1)

  if @zstd_module do
    @support_compressors ["zstd", "zlib"]
  else
    @support_compressors ["zlib"]
  end

  if @zstd_module do
    @zstd_compressor_id 3
  end

  def map_compressors(nil) do
    []
  end

  def map_compressors(compressors) do
    compressors
    |> Enum.filter(fn compressor -> compressor in @support_compressors end)
    |> Enum.map(fn compressor -> compressor_to_atom(compressor) end)
  end

  def compressor_to_atom("zstd") do
    :zstd
  end

  def compressor_to_atom("zlib") do
    :zlib
  end

  def zstd_available?, do: not is_nil(@zstd_module)

  def compressors() do
    @support_compressors
  end

  def compress(binary, :zlib) do
    {@zlib_compressor_id, :zlib.compress(binary)}
  end

  if @zstd_module do
    def compress(binary, :zstd) when is_binary(binary) do
      {@zstd_compressor_id, @zstd_module.compress(binary)}
    end

    def compress(iodata, :zstd) when is_list(iodata) do
      {@zstd_compressor_id,
       iodata
       |> IO.iodata_to_binary()
       |> @zstd_module.compress()}
    end
  end

  def uncompress(binary, @zlib_compressor_id) do
    :zlib.uncompress(binary)
  end

  if @zstd_module do
    def uncompress(binary, @zstd_compressor_id) do
      @zstd_module.decompress(binary)
    end
  end

  def uncompress(binary, :zlib) do
    :zlib.uncompress(binary)
  end

  if @zstd_module do
    def uncompress(binary, :zstd) do
      @zstd_module.decompress(binary)
    end
  end
end
