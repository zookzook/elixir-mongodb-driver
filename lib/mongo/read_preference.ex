defmodule Mongo.ReadPreference do

  import Keywords

  @moduledoc ~S"""
  Determines which servers are considered suitable for read operations

  A read preference consists of a mode and optional `tag_sets`, max_staleness_ms, and `hedge`.
  The mode prioritizes between primaries and secondaries to produce either a single suitable server or a list of candidate servers.
  If tag_sets and maxStalenessSeconds are set, they determine which candidate servers are eligible for selection.
  If hedge is set, it configures how server hedged reads are used.

  The default mode is `:primary`.
  The default tag_sets is a list with an empty tag set: [{}].
  The default max_staleness_ms is unset.
  The default hedge is unset.

  ## mode

  * `:primary` Only an available primary is suitable.
  * `:secondary` All secondaries (and only secondaries) are candidates, but only eligible candidates (i.e. after applying tag_sets and maxStalenessSeconds) are suitable.
  * `:primary_preferred` If a primary is available, only the primary is suitable. Otherwise, all secondaries are candidates,
       but only eligible secondaries are suitable.
  * `:secondary_preferred` All secondaries are candidates. If there is at least one eligible secondary, only eligible secondaries are suitable.
       Otherwise, when there are no eligible secondaries, the primary is suitable.
  * `:nearest` The primary and all secondaries are candidates, but only eligible candidates are suitable.

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

  @primary %{
    mode: :primary,
    tag_sets: [],
    max_staleness_ms: 0
  }

  def primary(map \\ nil)
  def primary(map) when is_map(map) do
    Map.merge(@primary, map)
  end
  def primary(_), do: @primary

  @doc """
  Add read preference to the cmd
  """
  def add_read_preference(cmd, opts) do

    read_preference = opts
                      |> Keyword.get(:read_preference)
                      |> Mongo.ReadPreference.primary()
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

  ##
  # Therefore, when sending queries to a mongos, the following rules apply:
  #
  # For mode 'primary', drivers MUST NOT set the slaveOK wire protocol flag and MUST NOT use $readPreference
  def mongos(%{mode: :primay}) do
    nil
  end
  # For mode 'secondary', drivers MUST set the slaveOK wire protocol flag and MUST also use $readPreference
  def mongos(%{mode: :secondary} = config) do
    transform(config)
  end
  # For mode 'primaryPreferred', drivers MUST set the slaveOK wire protocol flag and MUST also use $readPreference
  def mongos(%{mode: :primary_preferred} = config) do
    transform(config)
  end
  # For mode 'secondaryPreferred', drivers MUST set the slaveOK wire protocol flag. If the read preference contains a
  # non-empty tag_sets parameter, maxStalenessSeconds is a positive integer, or the hedge parameter is non-empty,
  # drivers MUST use $readPreference; otherwise, drivers MUST NOT use $readPreference
  def mongos(%{mode: :secondary_preferred} = config) do
    transform(config)
  end
  # For mode 'nearest', drivers MUST set the slaveOK wire protocol flag and MUST also use $readPreference
  def mongos(%{mode: :nearest} = config) do
    transform(config)
  end



end
