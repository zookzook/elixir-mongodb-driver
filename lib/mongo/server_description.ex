defmodule Mongo.ServerDescription do
  @moduledoc false

  alias Mongo.Version

  @retryable_wire_version Version.encode(:supports_op_msg)

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#serverdescription
  @type type :: :standalone | :mongos | :possible_primary | :rs_primary | :rs_secondary | :rs_arbiter | :rs_other | :rs_ghost | :unknown

  @type t :: %{
          address: String.t() | nil,
          error: String.t() | nil,
          round_trip_time: non_neg_integer | nil,
          last_write_date: DateTime.t(),
          op_time: BSON.ObjectId.t() | nil,
          type: type,
          min_wire_version: non_neg_integer,
          max_wire_version: non_neg_integer,
          me: String.t() | nil,
          hosts: [String.t()],
          passives: [String.t()],
          arbiters: [String.t()],
          tag_set: %{String.t() => String.t()},
          set_name: String.t() | nil,
          set_version: non_neg_integer | nil,
          election_id: BSON.ObjectId.t() | nil,
          primary: String.t() | nil,
          last_update_time: non_neg_integer,
          max_bson_object_size: non_neg_integer,
          max_message_size_bytes: non_neg_integer,
          max_write_batch_size: non_neg_integer,
          compression: String.t() | nil,
          read_only: boolean(),
          logical_session_timeout: non_neg_integer,
          supports_retryable_writes: boolean()
        }

  @empty %{
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
  }

  def new() do
    @empty
  end

  def defaults(map \\ %{}) do
    Map.merge(@empty, map)
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#parsing-an-ismaster-response
  def parse_hello_response(address, error) do
    Map.merge(@empty, %{address: address, error: error})
  end

  def parse_hello_response(hello_response) do
    server_type = determine_server_type(hello_response)
    max_wire_version = hello_response["maxWireVersion"] || 0

    supports_retryable_writes =
      server_type != :standalone &&
        max_wire_version >= @retryable_wire_version &&
        hello_response["logicalSessionTimeoutMinutes"] != nil

    %{
      type: server_type,
      last_write_date: get_in(hello_response, ["lastWrite", "lastWriteDate"]),
      op_time: get_in(hello_response, ["lastWrite", "opTime"]),
      min_wire_version: hello_response["minWireVersion"] || 0,
      max_wire_version: max_wire_version,
      me: hello_response["me"],
      hosts: (hello_response["hosts"] || []) |> Enum.map(&String.downcase/1),
      passives: (hello_response["passives"] || []) |> Enum.map(&String.downcase/1),
      arbiters: (hello_response["arbiters"] || []) |> Enum.map(&String.downcase/1),
      tag_set: hello_response["tags"] || %{},
      set_name: hello_response["setName"],
      set_version: hello_response["setVersion"],
      election_id: hello_response["electionId"],
      primary: hello_response["primary"],
      max_bson_object_size: hello_response["maxBsonObjectSize"] || 16_777_216,
      max_message_size_bytes: hello_response["maxMessageSizeBytes"] || 48_000_000,
      max_write_batch_size: hello_response["maxWriteBatchSize"] || 100_000,
      compression: hello_response["compression"],
      read_only: hello_response["readOnly"] || false,
      logical_session_timeout: hello_response["logicalSessionTimeoutMinutes"] || 30,
      supports_retryable_writes: supports_retryable_writes
    }
  end

  def parse_hello_response(address, rtt, last_update_time, hello_response) do
    server_type = determine_server_type(hello_response)
    max_wire_version = hello_response["maxWireVersion"] || 0

    %{
      address: address,
      round_trip_time: rtt,
      type: server_type,
      last_write_date: get_in(hello_response, ["lastWrite", "lastWriteDate"]),
      op_time: get_in(hello_response, ["lastWrite", "opTime"]),
      last_update_time: last_update_time,
      min_wire_version: hello_response["minWireVersion"] || 0,
      max_wire_version: max_wire_version,
      me: hello_response["me"],
      hosts: (hello_response["hosts"] || []) |> Enum.map(&String.downcase/1),
      passives: (hello_response["passives"] || []) |> Enum.map(&String.downcase/1),
      arbiters: (hello_response["arbiters"] || []) |> Enum.map(&String.downcase/1),
      tag_set: hello_response["tags"] || %{},
      set_name: hello_response["setName"],
      set_version: hello_response["setVersion"],
      election_id: hello_response["electionId"],
      primary: hello_response["primary"],
      max_bson_object_size: hello_response["maxBsonObjectSize"] || 16_777_216,
      max_message_size_bytes: hello_response["maxMessageSizeBytes"] || 48_000_000,
      max_write_batch_size: hello_response["maxWriteBatchSize"] || 100_000,
      compression: hello_response["compression"],
      read_only: hello_response["readOnly"] || false,
      logical_session_timeout: hello_response["logicalSessionTimeoutMinutes"] || 30,
      supports_retryable_writes: server_type != :standalone && max_wire_version >= @retryable_wire_version && hello_response["logicalSessionTimeoutMinutes"] != nil
    }
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#type
  defp determine_server_type(%{"ok" => n}) when n != 1, do: :unknown
  defp determine_server_type(%{"msg" => "isdbgrid"}), do: :mongos
  defp determine_server_type(%{"isreplicaset" => true}), do: :rs_ghost

  defp determine_server_type(%{"setName" => set_name} = is_master_reply) when set_name != nil do
    case is_master_reply do
      %{"ismaster" => true} -> :rs_primary
      %{"isWritablePrimary" => true} -> :rs_primary
      %{"secondary" => true} -> :rs_secondary
      %{"arbiterOnly" => true} -> :rs_arbiter
      _ -> :rs_other
    end
  end

  defp determine_server_type(_), do: :standalone
end
