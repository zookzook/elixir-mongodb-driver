defmodule Mongo.TopologyDescriptionTest do
  use ExUnit.Case, async: true
  alias Mongo.{ReadPreference, TopologyDescription}
  import Mongo.TopologyTestData

  test "single server selection" do
    single_server = "localhost:27017"

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :secondary})
    ]

    assert {:ok, {^single_server, _}} = TopologyDescription.select_servers(single(), :read, opts)

    assert {:ok, {^single_server, _}} = TopologyDescription.select_servers(single(), :write)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :nearest})
    ]

    assert {:ok, {^single_server, _}} = TopologyDescription.select_servers(single(), :read, opts)
  end

  test "shared server selection" do
    sharded_server = "localhost:27017"

    assert {:ok, {^sharded_server, []}} = TopologyDescription.select_servers(sharded(), :write, [])

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :primary})
    ]

    assert {:ok, {^sharded_server, []}} = TopologyDescription.select_servers(sharded(), :read, opts)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :secondary})
    ]

    assert {:ok, {^sharded_server, [{:read_preference, %{mode: :secondary, maxStalenessSeconds: 0}}]}} = TopologyDescription.select_servers(sharded(), :read, opts)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :primary_preferred})
    ]

    assert {:ok, {^sharded_server, [{:read_preference, %{mode: :primaryPreferred, maxStalenessSeconds: 0}}]}} = TopologyDescription.select_servers(sharded(), :read, opts)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :secondary_preferred})
    ]

    assert {:ok, {^sharded_server, [{:read_preference, %{mode: :secondaryPreferred, maxStalenessSeconds: 0}}]}} = TopologyDescription.select_servers(sharded(), :read, opts)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :nearest})
    ]

    assert {:ok, {^sharded_server, [{:read_preference, %{mode: :nearest, maxStalenessSeconds: 0}}]}} = TopologyDescription.select_servers(sharded(), :read, opts)
  end

  test "replica set server selection" do
    all_hosts = ["localhost:27018", "localhost:27019", "localhost:27020"]
    master = "localhost:27018"
    seconardaries = List.delete(all_hosts, master)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :secondary})
    ]

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    assert Enum.any?(seconardaries, fn sec -> sec == server end)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :primary})
    ]

    assert {:ok, {_master, _}} = TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :primary_preferred})
    ]

    assert {:ok, {_master, _}} = TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :primary_preferred})
    ]

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_no_master(), :read, opts)
    assert Enum.any?(seconardaries, fn sec -> sec == server end)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :nearest})
    ]

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_with_master(), :read, opts)
    assert Enum.any?(all_hosts, fn sec -> sec == server end)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :secondary})
    ]

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_no_master(), :read, opts)
    assert Enum.any?(seconardaries, fn sec -> sec == server end)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :secondary_preferred})
    ]

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_with_master(), :read, opts)
    assert Enum.any?(seconardaries, fn sec -> sec == server end)

    assert {:ok, {^master, _}} = TopologyDescription.select_servers(repl_set_only_master(), :read, opts)

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_no_master(), :read, opts)
    assert Enum.any?(seconardaries, fn sec -> sec == server end)

    opts = [
      read_preference: ReadPreference.merge_defaults(%{mode: :nearest})
    ]

    {:ok, {server, _}} = TopologyDescription.select_servers(repl_set_no_master(), :read, opts)
    assert Enum.any?(all_hosts, fn sec -> sec == server end)
  end

  test "Set topology type to :single when direct_connection option is true" do
    opts = [
      direct_connection: true
    ]

    assert :single = TopologyDescription.get_type(opts)

    opts = [
      type: :unknown,
      direct_connection: true
    ]

    assert :single = TopologyDescription.get_type(opts)
  end

  test "Set read_preference to :primaryPreferred when topology is single and server is replica set" do
    assert {:ok, {_, opts}} = TopologyDescription.select_servers(single(), :read, [])
    assert nil == Keyword.get(opts, :read_preference)

    assert {:ok, {_, opts}} = TopologyDescription.select_servers(single_with_repl_set(), :read, [])
    assert :primaryPreferred = Keyword.get(opts, :read_preference) |> Map.get(:mode)
  end
end
