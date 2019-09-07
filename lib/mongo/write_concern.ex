defmodule Mongo.WriteConcern do
  @moduledoc false

  import Keywords

  def write_concern(opts) do

    %{
      w: Keyword.get(opts, :w),
      j: Keyword.get(opts, :j),
      wtimeout: Keyword.get(opts, :wtimeout)
     } |> filter_nils() |> filter_empty()

  end

  def filter_empty(map) when is_map(map) and map == %{} do
    nil
  end
  def filter_empty(map) when is_map(map) do
    map
  end

  def acknowledged?(nil) do
    true
  end
  def acknowledged?(write_concern) when is_map(write_concern) do
    case Map.get(write_concern, :w) do
      0 -> false
      _ -> true
    end
  end

  def acknowledged?(write_concern) when is_list(write_concern) do
    case Keyword.get(write_concern, :w) do
      0 -> false
      _ -> true
    end
  end

end
