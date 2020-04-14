defmodule Mongo.ServerDescription do
  @moduledoc false

  alias Mongo.Version

  @retryable_wire_version Version.encode(:supports_op_msg)

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#serverdescription
  @type type :: :standalone | :mongos | :possible_primary | :rs_primary |
                :rs_secondary | :rs_arbiter | :rs_other | :rs_ghost | :unknown
  @type t :: %{
    address: String.t | nil,
    error: String.t | nil,
    round_trip_time: non_neg_integer | nil,
    last_write_date: DateTime.t,
    op_time: BSON.ObjectId.t | nil,
    type: type,
    min_wire_version: non_neg_integer,
    max_wire_version: non_neg_integer,
    me: String.t | nil,
    hosts: [String.t],
    passives: [String.t],
    arbiters: [String.t],
    tag_set: %{String.t => String.t},
    set_name: String.t | nil,
    set_version: non_neg_integer | nil,
    election_id: BSON.ObjectId.t | nil,
    primary: String.t | nil,
    last_update_time: non_neg_integer,
    max_bson_object_size: non_neg_integer,
    max_message_size_bytes: non_neg_integer,
    max_write_batch_size: non_neg_integer,
    compression: String.t | nil,
    read_only: boolean(),
    logical_session_timeout: non_neg_integer,
    supports_retryable_writes: boolean()
  }

  def defaults(map \\ %{}) do
    Map.merge(%{
      address: "localhost:27017",
      error: nil,
      round_trip_time: nil,
      last_write_date: nil,
      op_time: nil,
      type: :unknown,
      min_wire_version: 0,
      max_wire_version: 0,
      me: nil,
      hosts: [],
      passives: [],
      arbiters: [],
      tag_set: %{},
      set_name: nil,
      set_version: nil,
      election_id: nil,
      primary: nil,
      last_update_time: 0,
      max_bson_object_size: 16_777_216,
      max_message_size_bytes: 48_000_000,
      max_write_batch_size: 100_000,
      compression: nil,
      read_only: false,
      logical_session_timeout: 30,
      support_retryable_writes: false
    }, map)
  end

  def from_is_master_error(address, error) do
    defaults(%{
      address: address,
      error: error
    })
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#parsing-an-ismaster-response
  def from_is_master(address, rtt, finish_time, is_master_reply) do

    server_type      = determine_server_type(is_master_reply)
    max_wire_version = is_master_reply["maxWireVersion"] || 0
    %{
      address: address,
      round_trip_time: rtt,
      type: server_type,
      last_write_date: get_in(is_master_reply,["lastWrite", "lastWriteDate"]),
      op_time: get_in(is_master_reply, ["lastWrite", "opTime"]),
      last_update_time: finish_time,
      min_wire_version: is_master_reply["minWireVersion"] || 0,
      max_wire_version: is_master_reply["maxWireVersion"] || 0,
      me: is_master_reply["me"],
      hosts: (is_master_reply["hosts"] || []) |> Enum.map(&String.downcase/1),
      passives: (is_master_reply["passives"] || []) |> Enum.map(&String.downcase/1),
      arbiters: (is_master_reply["arbiters"] || []) |> Enum.map(&String.downcase/1),
      tag_set: is_master_reply["tags"] || %{},
      set_name: is_master_reply["setName"],
      set_version: is_master_reply["setVersion"],
      election_id: is_master_reply["electionId"],
      primary: is_master_reply["primary"],
      max_bson_object_size: (is_master_reply["maxBsonObjectSize"] || 16_777_216),
      max_message_size_bytes: (is_master_reply["maxMessageSizeBytes"] || 48_000_000),
      max_write_batch_size: (is_master_reply["maxWriteBatchSize"] || 100_000),
      compression: is_master_reply["compression"],
      read_only: (is_master_reply["readOnly"] || false),
      logical_session_timeout: is_master_reply["logicalSessionTimeoutMinutes"] || 30,
      supports_retryable_writes: server_type != :standalone && max_wire_version >= @retryable_wire_version && is_master_reply["logicalSessionTimeoutMinutes"] != nil
    }
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#type
  defp determine_server_type(%{"ok" => n}) when n != 1, do: :unknown
  defp determine_server_type(%{"msg" => "isdbgrid"}), do: :mongos
  defp determine_server_type(%{"isreplicaset" => true}), do: :rs_ghost
  defp determine_server_type(%{"setName" => set_name} = is_master_reply) when set_name != nil do
    case is_master_reply do
      %{"ismaster" => true}    -> :rs_primary
      %{"secondary" => true}   -> :rs_secondary
      %{"arbiterOnly" => true} -> :rs_arbiter
      _                        -> :rs_other
    end
  end
  defp determine_server_type(_), do: :standalone

end
