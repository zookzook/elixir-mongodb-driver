defmodule Mongo.ReadPreference do

  import Keywords

  @moduledoc ~S"""
  Determines which servers are considered suitable for read operations

  A read preference consists of a mode and optional `tag_sets`, max_staleness_ms, and `hedge`.
  The mode prioritizes between primaries and secondaries to produce either a single suitable server or a list of candidate servers.
  If tag_sets and maxStalenessSeconds are set, they determine which candidate servers are eligible for selection.
  If hedge is set, it configures how server hedged reads are used.

  The default mode is 'primary'.
  The default tag_sets is a list with an empty tag set: [{}].
  The default max_staleness_ms is unset.
  The default hedge is unset.

  ## mode

  * `primary` Only an available primary is suitable.
  * `secondary` All secondaries (and only secondaries) are candidates, but only eligible candidates (i.e. after applying tag_sets and maxStalenessSeconds) are suitable.
  * `primaryPreferred` If a primary is available, only the primary is suitable. Otherwise, all secondaries are candidates,
                       but only eligible secondaries are suitable.
  * `secondaryPreferred` All secondaries are candidates. If there is at least one eligible secondary, only eligible secondaries are suitable.
                         Otherwise, when there are no eligible secondaries, the primary is suitable.
  * `nearest` The primary and all secondaries are candidates, but only eligible candidates are suitable.

  """
  @type t :: %{
    mode: :primary |
          :secondary |
          :primary_preferred |
          :secondary_preferred |
          :nearest,
    tag_sets: [%{String.t => String.t}],
    max_staleness_ms: non_neg_integer,
    hedge: BSON.document
  }

  @default %{
    mode: :primary,
    tag_sets: [],
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
                      |> transform()

    cmd ++ ["$readPreference": read_preference]
  end

  defp transform(%{:mode => :primary}) do
    %{:mode => :primary}
  end
  defp transform(config) do

    mode = case config[:mode] do
      :primary_preferred   -> :primaryPreferred
      :secondary_preferred -> :secondaryPreferred
      other -> other
    end

    max_staleness_seconds = case config[:max_staleness_ms] do
      i when is_integer(i) -> div(i, 1000)
      nil                  -> nil
    end

    [mode: mode,
      tag_sets: config[:tag_sets],
      maxStalenessSeconds: max_staleness_seconds,
      hedge: config[:hedge]]
    |> filter_nils()

  end

  defp is_max_staleness_valid?() do
    #max_staleness_ms >= heartbeatFrequencyMS + idleWritePeriodMS
    #max_staleness_ms >= smallestMaxStalenessSeconds

  end
end
