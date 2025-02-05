defmodule Mongo.ReadPreference do
  import Keywords

  @moduledoc ~S"""
  Determines which servers are considered suitable for read operations

  A read preference consists of a mode and optional `tags`, max_staleness_ms, and `hedge`.
  The mode prioritizes between primaries and secondaries to produce either a single suitable server or a list of candidate servers.
  If tags and maxStalenessSeconds are set, they determine which candidate servers are eligible for selection.
  If hedge is set, it configures how server hedged reads are used.

  The default mode is `:primary`.
  The default tags is a list with an empty tag set: [{}].
  The default max_staleness_ms is unset.
  The default hedge is unset.

  ## mode

  * `:primary` Only an available primary is suitable.
  * `:secondary` All secondaries (and only secondaries) are candidates, but only eligible candidates (i.e. after applying tags and maxStalenessSeconds) are suitable.
  * `:primary_preferred` If a primary is available, only the primary is suitable. Otherwise, all secondaries are candidates,
       but only eligible secondaries are suitable.
  * `:secondary_preferred` All secondaries are candidates. If there is at least one eligible secondary, only eligible secondaries are suitable.
       Otherwise, when there are no eligible secondaries, the primary is suitable.
  * `:nearest` The primary and all secondaries are candidates, but only eligible candidates are suitable.

  """

  @primary %{
    mode: :primary,
    tags: [],
    max_staleness_ms: 0
  }

  @doc """
  Merge default values to the read preferences and converts deprecated tag_sets to tags
  """
  def merge_defaults(%{tag_sets: tags} = map) do
    map =
      map
      |> Map.delete(:tag_sets)
      |> Map.put(:tags, tags)

    Map.merge(@primary, map)
  end

  def merge_defaults(map) when is_map(map) do
    Map.merge(@primary, map)
  end

  def merge_defaults(_other) do
    @primary
  end

  @doc """
  Add read preference to the cmd
  """
  def add_read_preference(cmd, opts) do
    case Keyword.get(opts, :read_preference) do
      nil ->
        cmd

      pref ->
        cmd ++ ["$readPreference": pref]
    end
  end

  @doc """
  Converts the preference to the mongodb format for replica sets
  """
  def to_replica_set(%{:mode => :primary}) do
    %{mode: :primary}
  end

  def to_replica_set(config) do
    mode =
      case config[:mode] do
        :primary_preferred ->
          :primaryPreferred

        :secondary_preferred ->
          :secondaryPreferred

        other ->
          other
      end

    case config[:tags] do
      [] ->
        %{mode: mode}

      nil ->
        %{mode: mode}

      tags ->
        %{mode: mode, tags: [tags]}
    end
  end

  @doc """
  Converts the preference to the mongodb format for mongos
  """
  def to_mongos(%{mode: :primary}) do
    nil
  end

  # for the others we should use the read preferences
  def to_mongos(config) do
    mode =
      case config[:mode] do
        :primary_preferred ->
          :primaryPreferred

        :secondary_preferred ->
          :secondaryPreferred

        other ->
          other
      end

    max_staleness_seconds =
      case config[:max_staleness_ms] do
        i when is_integer(i) ->
          div(i, 1000)

        nil ->
          nil
      end

    read_preference =
      case config[:tags] do
        [] ->
          %{mode: mode, maxStalenessSeconds: max_staleness_seconds, hedge: config[:hedge]}

        nil ->
          %{mode: mode, maxStalenessSeconds: max_staleness_seconds, hedge: config[:hedge]}

        tags ->
          %{mode: mode, tags: [tags], maxStalenessSeconds: max_staleness_seconds, hedge: config[:hedge]}
      end

    filter_nils(read_preference)
  end

  def to_topology_single_type({_, %{replica?: true} = _server_description}), do: %{mode: :primaryPreferred}
  def to_topology_single_type(_), do: nil
end
