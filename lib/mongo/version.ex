defmodule Mongo.Version do
  @moduledoc """
  This module contains the constant of all wire versions.

  see https://github.com/mongodb/mongo/blob/master/src/mongo/db/wire_version.h
  """

  values = [
    # Everything before we started tracking.
    release_2_4_and_before: 0,
    # The aggregation command may now be requested to return cursors.
    agg_returns_cursors: 1,
    # insert, update, and delete batch command
    batch_commands: 2,
    # support SCRAM-SHA1, listIndexes, listCollections, new explain
    release_2_7_7: 3,
    # Support find and getMore commands, as well as OP_COMMAND in mongod (but not mongos).
    find_command: 4,
    # Supports all write commands take a write concern.
    commands_accept_write_concern: 5,
    # Supports the new OP_MSG wireprotocol (3.6+).
    supports_op_msg: 6,
    # Supports replica set transactions (4.0+).
    replica_set_transactions: 7,
    # Supports sharded transactions (4.2+).
    sharded_transactions: 8,
    # Supports resumable initial sync (4.4+).
    resumable_initial_sync: 9,
    # Supports features available from 4.7 and onwards.
    wire_version_47: 10,
    # Supports features available from 4.8 and onwards.
    wire_version_48: 11,
    # Supports features available from 4.9 and onwards.
    wire_version_49: 12,
    # Supports features available from 5.0 and onwards.
    wire_version_50: 13,
    # Supports features available from 5.1 and onwards.
    wire_version_51: 14,
    # Supports features available from 5.2 and onwards.
    wire_version_52: 15,
    # Supports features available from 5.3 and onwards.
    wire_version_53: 16,
    # Supports features available from 6.0 and onwards.
    wire_version_60: 17,
    # Supports features available from 6.1 and onwards.
    wire_version_61: 18
  ]

  for {key, value} <- values do
    def encode(unquote(key)), do: unquote(value)
    def decode(unquote(value)), do: unquote(key)
  end
end
