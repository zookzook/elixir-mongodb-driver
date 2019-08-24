defmodule Mongo.ReadPreference do
  @moduledoc ~S"""
  Determines which servers are considered suitable for read operations
  """
  @type t :: %{
    mode: :primary | :secondary | :primary_preferred | :secondary_preferred |
          :nearest,
    tag_sets: [%{String.t => String.t}],
    max_staleness_ms: non_neg_integer
  }

  @default %{
    mode: :primary,
    tag_sets: [%{}],
    max_staleness_ms: 0
  }

  def defaults(map \\ nil)
  def defaults(map) when is_map(map) do
    Map.merge(@default, map)
  end
  def defaults(_), do: @default

  @doc """
  Add read preference to the cmd
  """
  def add_read_preference(cmd, opts) do

    read_preference = opts
                      |> Keyword.get(:read_preference)
                      |> Mongo.ReadPreference.defaults()

    cmd ++ ["$readPreference": read_preference]
  end

end
