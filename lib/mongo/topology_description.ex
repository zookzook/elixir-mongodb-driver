defmodule Mongo.TopologyDescription do
  @moduledoc false
  # This acts as a single topology consisting of many connections, built on top
  # of the existing connection API's. It implements the Server Discovery and
  # Monitoring specification, along with the `Mongo.ServerMonitor` module.

  alias Mongo.Version

  @release_2_4_and_before Version.encode(:release_2_4_and_before)
  @resumable_initial_sync Version.encode(:release_2_4_and_before)
  @wire_protocol_range @release_2_4_and_before..@resumable_initial_sync

  alias Mongo.ServerDescription
  alias Mongo.ReadPreference

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#topologydescription
  @type type :: :unknown | :single | :replica_set_no_primary | :replica_set_with_primary | :sharded
  @type t :: %{
          type: type,
          set_name: String.t() | nil,
          max_set_version: non_neg_integer | nil,
          max_election_id: BSON.ObjectId.t(),
          servers: %{String.t() => Mongo.ServerDescription.t()},
          compatible: boolean,
          compatibility_error: String.t() | nil,
          local_threshold_ms: non_neg_integer
        }

  def defaults(map \\ %{}) do
    default_servers = %{"localhost:27017" => ServerDescription.defaults(%{})}

    Map.merge(
      %{
        type: :unknown,
        set_name: nil,
        max_set_version: nil,
        max_election_id: nil,
        servers: default_servers,
        compatible: true,
        compatibility_error: nil,
        local_threshold_ms: 15
      },
      map
    )
  end

  def has_readable_server?(_topology, _read_preference) do
    true
  end

  def max_version(topology, [host]) do
    case Map.get(topology.servers, host) do
      nil -> 0
      server -> server.max_wire_version
    end
  end

  def has_writable_server?(topology) do
    topology.type in [:single, :sharded, :replica_set_with_primary]
  end

  def update(topology, server_description, num_seeds) do
    check_server_supported(topology, server_description, num_seeds)
  end

  @doc """
  Updates the current measured round trip time for the specified server (address)
  """
  def update_rrt(topology, address, round_trip_time) do
    case topology.servers[address] do
      nil ->
        topology

      _other ->
        put_in(topology.servers[address][:round_trip_time], round_trip_time)
    end
  end

  @doc """
  Selects the next possible server from the current topology.
  """
  def select_servers(topology, type, opts \\ [])

  def select_servers(%{:compatible => false}, _type, _opts) do
    {:error, :invalid_wire_version}
  end

  def select_servers(topology, :write, opts) do
    servers =
      case topology.type do
        :single -> topology.servers
        :sharded -> mongos_servers(topology)
        :replica_set_with_primary -> primary_servers(topology)
        _ -> []
      end

    addr =
      servers
      ## only valid servers
      |> Enum.filter(fn {_, %{type: type}} -> type != :unknown end)
      |> Enum.map(fn {server, _} -> server end)
      |> Enum.take_random(1)

    case addr do
      [] -> :empty
      [result] -> {:ok, {result, opts}}
    end
  end

  def select_servers(topology, :read, opts) do
    read_preference =
      opts
      |> Keyword.get(:read_preference)
      |> ReadPreference.primary()

    {servers, read_prefs} =
      case topology.type do
        :unknown -> {[], nil}
        :single -> {topology.servers, nil}
        :sharded -> {mongos_servers(topology), ReadPreference.mongos(read_preference)}
        _ -> {select_replica_set_server(topology, read_preference.mode, read_preference), ReadPreference.slave_ok(read_preference)}
      end

    opts =
      case read_prefs do
        nil -> Keyword.delete(opts, :read_preference)
        prefs -> Keyword.put(opts, :read_preference, prefs)
      end

    addr =
      servers
      |> Enum.map(fn {server, _} -> server end)
      |> Enum.take_random(1)

    # check now three possible cases
    case addr do
      [] -> :empty
      [result] -> {:ok, {result, opts}}
    end
  end

  defp mongos_servers(%{:servers => servers}) do
    Enum.filter(servers, fn {_, server} -> server.type == :mongos end)
  end

  defp primary_servers(%{:servers => servers}) do
    Enum.filter(servers, fn {_, server} -> server.type == :rs_primary end)
  end

  defp secondary_servers(%{:servers => servers}) do
    Enum.filter(servers, fn {_, server} -> server.type == :rs_secondary end)
  end

  ##
  #
  # Select the primary without without tag_sets or maxStalenessSeconds
  #
  defp select_replica_set_server(topology, :primary, _read_preference) do
    primary_servers(topology)
  end

  ##
  #
  # Select the secondary with without tag_sets or maxStalenessSeconds
  #
  defp select_replica_set_server(topology, :secondary, read_preference) do
    topology
    |> secondary_servers()
    |> filter_out_stale(topology, read_preference.max_staleness_ms)
    |> select_tag_sets(read_preference.tag_sets)
    |> filter_latency_window(topology.local_threshold_ms)
  end

  ##
  # From the specs
  #
  # 'primaryPreferred' is equivalent to selecting a server with read preference mode 'primary'
  # (without tag_sets or maxStalenessSeconds), or, if that fails, falling back to selecting with read preference mode
  # 'secondary' (with tag_sets and maxStalenessSeconds, if provided).
  defp select_replica_set_server(topology, :primary_preferred, read_preference) do
    case primary_servers(topology) do
      [] -> select_replica_set_server(topology, :secondary, read_preference)
      primary -> primary
    end
  end

  ##
  # From the specs
  # 'secondaryPreferred' is the inverse: selecting with mode 'secondary' (with tag_sets and maxStalenessSeconds) and
  # falling back to selecting with mode 'primary' (without tag_sets or maxStalenessSeconds).
  #
  defp select_replica_set_server(topology, :secondary_preferred, read_preference) do
    case select_replica_set_server(topology, :secondary, read_preference) do
      [] -> primary_servers(topology)
      secondary -> secondary
    end
  end

  ##
  # From the specs:
  #
  # The term 'nearest' is unfortunate, as it implies a choice based on geographic locality or absolute lowest latency, neither of which are true.
  #
  # Instead, and unlike the other read preference modes, 'nearest' does not favor either primaries or secondaries;
  # instead all servers are candidates and are filtered by tag_sets and maxStalenessSeconds.
  defp select_replica_set_server(%{:servers => servers} = topology, :nearest, read_preference) do
    servers
    |> filter_out_stale(topology, read_preference.max_staleness_ms)
    |> select_tag_sets(read_preference.tag_sets)
    |> filter_latency_window(topology.local_threshold_ms)
  end

  defp filter_out_stale(servers, _topology, nil), do: servers
  defp filter_out_stale(servers, _topology, 0), do: servers

  defp filter_out_stale(servers, topology, max_staleness_ms) do
    {_, primary} =
      case topology.type do
        :replica_set_no_primary -> find_max_secondary(servers)
        :replica_set_with_primary -> find_primary(topology.servers)
      end

    servers
    |> Enum.filter(fn
      {_, %{type: :rs_secondary} = secondary} -> calc_staleness(primary, secondary, topology) < max_staleness_ms
      {_, _other} -> true
    end)
    |> Enum.into(%{})
  end

  ##
  # find the primary server
  #
  defp find_primary(servers) do
    Enum.find(servers, fn {_, %{type: type}} -> type == :rs_primary end)
  end

  ##
  # find server with the max last write date!
  #
  defp find_max_secondary(servers) do
    Enum.reduce(servers, {0, nil}, fn {_, server}, {max, max_server} ->
      case server.last_write_date > max do
        true -> {server.last_write_date, server}
        false -> {max, max_server}
      end
    end)
  end

  ##
  # Don't crash...
  #
  defp calc_staleness(nil, _secondary, _topology) do
    0
  end

  ## When there is no known primary, a secondary S's staleness is estimated with this formula:
  ##
  ## SMax.lastWriteDate - S.lastWriteDate + heartbeatFrequencyMS
  defp calc_staleness(smax, secondary, %{type: :replica_set_no_primary, heartbeat_frequency_ms: freq}) do
    DateTime.diff(smax.last_write_date, secondary.last_write_date, :millisecond) + freq
  end

  ## When there is a known primary, a secondary S's staleness is estimated with this formula:
  ##
  ## (S.lastUpdateTime - S.lastWriteDate) - (P.lastUpdateTime - P.lastWriteDate) + heartbeatFrequencyMS
  ##
  defp calc_staleness(primary, secondary, %{type: :replica_set_with_primary, heartbeat_frequency_ms: freq}) do
    DateTime.diff(secondary.last_update_time, secondary.last_write_date, :millisecond) + DateTime.diff(primary.last_update_time, primary.last_write_date, :millisecond) + freq
  end

  defp select_tag_sets(servers, []) do
    servers
  end

  defp select_tag_sets(servers, tag_sets) do
    tags = MapSet.new(tag_sets |> Enum.map(fn {key, value} -> {to_string(key), value} end))

    Enum.reduce(servers, [], fn
      {address, server}, acc ->
        case MapSet.subset?(tags, MapSet.new(server.tag_set)) do
          true -> [{address, server} | acc]
          false -> acc
        end
    end)
  end

  defp filter_latency_window(servers, local_threshold_ms) do
    if Enum.empty?(servers) do
      servers
    else
      min_server =
        servers
        |> Enum.min_by(fn {_, server} -> server.round_trip_time end)
        |> elem(1)

      latency_window = min_server.round_trip_time + local_threshold_ms

      Enum.filter(servers, fn {_, server} ->
        server.round_trip_time <= latency_window
      end)
    end
  end

  defp check_server_supported(topology, server_description, num_seeds) do
    server_supported_range = server_description.min_wire_version..server_description.max_wire_version
    server_supported? = Enum.any?(server_supported_range, fn version -> version in @wire_protocol_range end)

    if server_supported? do
      check_for_single_topology(topology, server_description, num_seeds)
    else
      topology =
        topology
        |> Map.put(:compatible, false)
        |> Map.put(
          :compatibility_error,
          "Server at #{server_description.address} uses wire protocol " <>
            "versions #{server_description.min_wire_version} through " <>
            "#{server_description.max_wire_version}, but client only " <>
            "supports #{Enum.min(@wire_protocol_range)} through " <>
            "#{Enum.max(@wire_protocol_range)}."
        )

      {[], topology}
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#topologytype-single
  defp check_for_single_topology(topology, server_description, num_seeds) do
    case topology.type do
      :single ->
        previous_description =
          topology.servers
          |> Map.values()
          |> hd()

        {[{previous_description, server_description}], put_in(topology.servers[server_description.address], Map.merge(previous_description, server_description))}

      _ ->
        check_server_in_topology(topology, server_description, num_seeds)
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#other-topologytypes
  defp check_server_in_topology(%{type: type} = topology, %{address: address} = server_description, num_seeds) do
    case topology.servers[address] do
      nil ->
        {[], topology}

      previous_description ->
        server_description = Map.merge(previous_description, server_description)

        {actions, topology} =
          topology
          |> put_in([:servers, address], server_description)
          |> update_topology(type, server_description, num_seeds)

        {[{previous_description, server_description} | actions], topology}
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#topologytype-explanations
  defp update_topology(topology, :unknown, server_description, num_seeds) do
    case server_description.type do
      :unknown ->
        {[], %{topology | type: :unknown}}

      :rs_ghost ->
        {[], %{topology | type: :unknown}}

      :standalone ->
        update_unknown_with_standalone(topology, server_description, num_seeds)

      :mongos ->
        {[], %{topology | type: :sharded}}

      :rs_primary ->
        topology
        |> Map.put(:set_name, server_description.set_name)
        |> update_rs_from_primary(server_description)

      type when type in [:rs_secondary, :rs_arbiter, :rs_other] ->
        topology
        |> Map.put(:set_name, server_description.set_name)
        |> update_rs_without_primary(server_description)

      # don't touch broken states...
      _ ->
        {[], topology}
    end
  end

  defp update_topology(topology, :sharded, server_description, _) do
    case server_description.type do
      type when type in [:unknown, :mongos] ->
        {[], topology}

      type when type in [:rs_ghost, :standalone, :rs_primary, :rs_secondary, :rs_arbiter, :rs_other] ->
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}

      _ ->
        {[], topology}
    end
  end

  defp update_topology(topology, :replica_set_no_primary, server_description, _) do
    case server_description.type do
      type when type in [:unknown, :rs_ghost] ->
        {[], topology}

      type when type in [:standalone, :mongos] ->
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}

      :rs_primary ->
        update_rs_from_primary(topology, server_description)

      type when type in [:rs_secondary, :rs_arbiter, :rs_ghost] ->
        update_rs_without_primary(topology, server_description)

      _ ->
        {[], topology}
    end
  end

  defp update_topology(topology, :replica_set_with_primary, server_description, _) do
    case server_description.type do
      :unknown ->
        topology |> check_if_has_primary

      :rs_ghost ->
        topology |> check_if_has_primary

      type when type in [:standalone, :mongos] ->
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        check_if_has_primary(new_topology)

      :rs_primary ->
        update_rs_from_primary(topology, server_description)

      type when type in [:rs_secondary, :rs_arbiter, :rs_ghost] ->
        update_rs_with_primary_from_member(topology, server_description)

      _ ->
        {[], topology}
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#actions

  defp not_in_servers?(topology, server_description) do
    server_description.address not in Map.keys(topology.servers)
  end

  def invalid_set_name?(topology, server_description) do
    topology.set_name != server_description.set_name and
      topology.set_name != nil
  end

  defp update_unknown_with_standalone(topology, server_description, num_seeds) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      if num_seeds == 1 do
        {[], Map.put(topology, :type, :single)}
      else
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}
      end
    end
  end

  defp update_rs_without_primary(topology, server_description) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      if invalid_set_name?(topology, server_description) do
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}
      else
        {actions, topology} =
          topology
          |> Map.put(:set_name, server_description.set_name)
          |> add_new_servers(server_description)

        if server_description.address != server_description.me do
          {_, topology} = pop_in(topology.servers[server_description.address])
          {actions, topology}
        else
          {actions, topology}
        end
      end
    end
  end

  defp add_new_servers({actions, topology}, server_description) do
    {[], new_topology} = add_new_servers(topology, server_description)
    {actions, new_topology}
  end

  defp add_new_servers(topology, server_description) do
    all_hosts = server_description.hosts ++ server_description.passives ++ server_description.arbiters

    topology =
      Enum.reduce(all_hosts, topology, fn host, topology ->
        case Map.has_key?(topology.servers, host) do
          true ->
            topology

          false ->
            # this is kinda like an "upsert"
            put_in(topology.servers[host], ServerDescription.defaults(%{address: host}))
        end
      end)

    {[], topology}
  end

  defp update_rs_with_primary_from_member(topology, server_description) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      topology =
        if invalid_set_name?(topology, server_description) do
          {_, new_topology} = pop_in(topology.servers[server_description.address])
          new_topology
        else
          topology
        end

      if server_description.address != server_description.me do
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        check_if_has_primary(new_topology)
      else
        if Enum.any?(topology.servers, fn
             {_, server_description} ->
               server_description.type == :rs_primary
           end) do
          {[], topology}
        else
          {[], %{topology | type: :replica_set_no_primary}}
        end
      end
    end
  end

  defp update_rs_from_primary(topology, server_description) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      if invalid_set_name?(topology, server_description) do
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        check_if_has_primary(new_topology)
      else
        topology
        |> Map.put(:set_name, server_description.set_name)
        |> handle_election_id(server_description)
      end
    end
  end

  defp handle_election_id(topology, server_description) do
    # yes, this is really in the spec
    if server_description[:set_version] != nil and
         server_description[:election_id] != nil do
      has_set_version_and_election_id? =
        topology[:max_set_version] != nil and
          topology[:max_election_id] != nil

      newer_set_version? = topology.max_set_version > server_description.set_version
      same_set_version? = topology.max_set_version == server_description.set_version
      greater_election_id? = topology.max_election_id > server_description.election_id

      if has_set_version_and_election_id? and
           (newer_set_version? or (same_set_version? and greater_election_id?)) do
        new_server_description = ServerDescription.defaults(%{address: server_description.address})

        topology
        |> put_in([:servers, new_server_description.address], new_server_description)
        |> check_if_has_primary
      else
        topology
        |> Map.put(:max_election_id, server_description.election_id)
        |> continue(server_description)
      end
    else
      topology
      |> continue(server_description)
    end
  end

  defp continue(topology, server_description) do
    topology
    |> handle_set_version(server_description)
    |> invalidate_stale_primary(server_description)
    |> add_new_servers(server_description)
    |> remove_dead_nodes(server_description)
    |> check_if_has_primary
  end

  defp handle_set_version(topology, server_description) do
    if server_description.set_version != nil and
         (topology.max_set_version == nil or
            server_description.set_version > topology.max_set_version) do
      Map.put(topology, :max_set_version, server_description.set_version)
    else
      topology
    end
  end

  def invalidate_stale_primary(topology, server_description) do
    {actions, new_servers} =
      topology.servers
      |> Enum.reduce({[], %{}}, fn {address, %{type: type} = server}, {acts, servers} ->
        if address != server_description.address and type == :rs_primary do
          {[{:force_check, address} | acts], Map.put(servers, address, ServerDescription.defaults(%{address: address}))}
        else
          {acts, Map.put(servers, address, server)}
        end
      end)

    {actions, Map.put(topology, :servers, new_servers)}
  end

  def remove_dead_nodes({actions, topology}, server_description) do
    all_hosts = server_description.hosts ++ server_description.passives ++ server_description.arbiters

    topology =
      update_in(
        topology.servers,
        &Enum.into(
          Enum.filter(&1, fn {address, _} ->
            address in all_hosts
          end),
          %{}
        )
      )

    {actions, topology}
  end

  defp check_if_has_primary({actions, topology}) do
    {[], new_topology} = check_if_has_primary(topology)
    {actions, new_topology}
  end

  defp check_if_has_primary(topology) do
    any_primary? =
      Enum.any?(topology.servers, fn {_, server_description} ->
        server_description.type == :rs_primary
      end)

    if any_primary? do
      {[], %{topology | type: :replica_set_with_primary}}
    else
      {[], %{topology | type: :replica_set_no_primary}}
    end
  end
end
