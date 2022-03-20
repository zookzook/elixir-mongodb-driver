defmodule Mongo.TopologyTestData do
  @moduledoc false

  def single,
    do: %{
      set_name: nil,
      type: :single,
      compatibility_error: nil,
      compatible: true,
      local_threshold_ms: 15,
      max_election_id: nil,
      max_set_version: nil,
      servers: %{
        "localhost:27017" => %{
          address: "localhost:27017",
          arbiters: [],
          election_id: nil,
          error: nil,
          hosts: [],
          last_update_time: nil,
          last_write_date: nil,
          max_wire_version: 4,
          me: nil,
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: nil,
          round_trip_time: 44,
          set_name: nil,
          set_version: nil,
          tag_set: %{},
          type: :standalone
        }
      }
    }

  def sharded(),
    do: %{
      set_name: nil,
      type: :sharded,
      compatibility_error: nil,
      compatible: true,
      local_threshold_ms: 15,
      max_election_id: nil,
      max_set_version: nil,
      servers: %{
        "localhost:27017" => %{
          address: "localhost:27017",
          arbiters: [],
          election_id: nil,
          error: nil,
          hosts: [],
          last_update_time: nil,
          last_write_date: nil,
          max_wire_version: 8,
          me: nil,
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: nil,
          round_trip_time: 44,
          set_name: nil,
          set_version: nil,
          tag_set: %{},
          type: :mongos
        }
      }
    }

  def repl_set_with_master,
    do: %{
      compatibility_error: nil,
      compatible: true,
      local_threshold_ms: 15,
      set_name: "replset1",
      type: :replica_set_with_primary,
      max_election_id: nil,
      max_set_version: 3,
      servers: %{
        "localhost:27018" => %{
          address: "localhost:27018",
          arbiters: [],
          election_id: nil,
          error: nil,
          last_update_time: 1_472_503_386_585,
          last_write_date: nil,
          max_wire_version: 4,
          me: "localhost:27018",
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: "localhost:27018",
          round_trip_time: 16,
          set_name: "replset1",
          set_version: 3,
          tag_set: %{},
          type: :rs_primary,
          hosts: [
            "localhost:27018",
            "localhost:27019",
            "localhost:27020"
          ]
        },
        "localhost:27019" => %{
          address: "localhost:27019",
          arbiters: [],
          election_id: nil,
          last_update_time: 1_472_503_386_582,
          last_write_date: nil,
          max_wire_version: 4,
          me: "localhost:27019",
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: "localhost:27018",
          round_trip_time: 15,
          set_name: "replset1",
          set_version: 3,
          tag_set: %{},
          type: :rs_secondary,
          error: nil,
          hosts: [
            "localhost:27018",
            "localhost:27019",
            "localhost:27020"
          ]
        },
        "localhost:27020" => %{
          address: "localhost:27020",
          arbiters: [],
          election_id: nil,
          last_update_time: 1_472_503_386_583,
          last_write_date: nil,
          max_wire_version: 4,
          me: "localhost:27020",
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: "localhost:27018",
          round_trip_time: 14,
          set_name: "replset1",
          set_version: 3,
          tag_set: %{},
          type: :rs_secondary,
          error: nil,
          hosts: [
            "localhost:27018",
            "localhost:27019",
            "localhost:27020"
          ]
        }
      }
    }

  def repl_set_no_master,
    do: %{
      compatibility_error: nil,
      compatible: true,
      local_threshold_ms: 15,
      set_name: "replset1",
      type: :replica_set_no_primary,
      max_election_id: nil,
      max_set_version: 3,
      servers: %{
        "localhost:27018" => %{
          address: "localhost:27018",
          arbiters: [],
          election_id: nil,
          error: nil,
          last_update_time: nil,
          last_write_date: nil,
          max_wire_version: 0,
          me: nil,
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: nil,
          round_trip_time: 0,
          set_name: nil,
          set_version: nil,
          tag_set: %{},
          type: :unknown,
          hosts: []
        },
        "localhost:27019" => %{
          address: "localhost:27019",
          arbiters: [],
          election_id: nil,
          last_update_time: 1_472_503_386_582,
          last_write_date: nil,
          max_wire_version: 4,
          me: "localhost:27019",
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: "localhost:27018",
          round_trip_time: 15,
          set_name: "replset1",
          set_version: 3,
          tag_set: %{},
          type: :rs_secondary,
          error: nil,
          hosts: [
            "localhost:27018",
            "localhost:27019",
            "localhost:27020"
          ]
        },
        "localhost:27020" => %{
          address: "localhost:27020",
          arbiters: [],
          election_id: nil,
          last_update_time: 1_472_503_386_583,
          last_write_date: nil,
          max_wire_version: 4,
          me: "localhost:27020",
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: "localhost:27018",
          round_trip_time: 14,
          set_name: "replset1",
          set_version: 3,
          tag_set: %{},
          type: :rs_secondary,
          error: nil,
          hosts: [
            "localhost:27018",
            "localhost:27019",
            "localhost:27020"
          ]
        }
      }
    }

  def repl_set_only_master,
    do: %{
      compatibility_error: nil,
      compatible: true,
      local_threshold_ms: 15,
      set_name: "replset1",
      type: :replica_set_with_primary,
      max_election_id: nil,
      max_set_version: 3,
      servers: %{
        "localhost:27018" => %{
          address: "localhost:27018",
          arbiters: [],
          election_id: nil,
          error: nil,
          last_update_time: nil,
          last_write_date: nil,
          max_wire_version: 0,
          me: nil,
          min_wire_version: 0,
          op_time: nil,
          passives: [],
          primary: nil,
          round_trip_time: 15,
          set_name: nil,
          set_version: nil,
          tag_set: %{},
          type: :rs_primary,
          hosts: ["localhost:27018"]
        }
      }
    }
end
