defmodule WriteConcern do
  @moduledoc false

  import Keywords

  def write_concern(opts) do

    %{
      w: Keyword.get(opts, :w),
      j: Keyword.get(opts, :j),
      wtimeout: Keyword.get(opts, :wtimeout)
     } |> filter_nils()

  end

  def acknowledged?(write_concern) do
    case Map.get(write_concern, :w) do
      0 -> true
      _ -> false
    end
  end

end
