defmodule Mongo.WriteConcern do
  @moduledoc false

  import Keywords

  @spec write_concern(keyword) :: nil | map
  def write_concern(opts) do
    %{
      w: Keyword.get(opts, :w),
      j: Keyword.get(opts, :j),
      wtimeout: Keyword.get(opts, :wtimeout)
    }
    |> filter_nils()
    |> filter_empty()
  end

  @spec filter_empty(map) :: nil | map
  defp filter_empty(%{} = map) when map == %{}, do: nil
  defp filter_empty(%{} = map), do: map

  @spec acknowledged?(nil | keyword | map) :: boolean
  def acknowledged?(nil), do: true

  def acknowledged?(%{} = write_concern), do: Map.get(write_concern, :w) != 0

  def acknowledged?(write_concern) when is_list(write_concern), do: Keyword.get(write_concern, :w) != 0
end
