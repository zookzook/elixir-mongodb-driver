defmodule InsightsWeb.TopologyView do
  use InsightsWeb, :view

  def event_name(%{__struct__: name} = struct) do
    name
    |> Module.split
    |> Enum.join(".")
  end

  @doc """
  Support for tabs
  """
  def tab_active(tab, current) do
    case tab == current do
      true -> "active"
      false -> []
    end
  end

  def event_selected?(true) do
    "selected"
  end

  def event_selected?(_) do
    []
  end
end
