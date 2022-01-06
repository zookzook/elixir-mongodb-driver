defmodule Mongo.Version do
  @moduledoc """
  This module contains the constant of all wire versions.

  see https://github.com/mongodb/mongo/blob/master/src/mongo/db/wire_version.h
  """

  values = [
    release_2_4_and_before:        0, # Everything before we started tracking.
    agg_returns_cursors:           1, # The aggregation command may now be requested to return cursors.
    batch_commands:                2, # insert, update, and delete batch command
    release_2_7_7:                 3, # support SCRAM-SHA1, listIndexes, listCollections, new explain
    find_command:                  4, # Support find and getMore commands, as well as OP_COMMAND in mongod (but not mongos).
    commands_accept_write_concern: 5, # Supports all write commands take a write concern.
    supports_op_msg:               6, # Supports the new OP_MSG wireprotocol (3.6+).
    replica_set_transactions:      7, # Supports replica set transactions (4.0+).
    sharded_transactions:          8, # Supports sharded transactions (4.2+).
    resumable_initial_sync:        9, # Supports resumable initial sync (4.4+).
    wire_version_47:              10, # Supports features available from 4.7 and onwards.
    wire_version_48:              11, # Supports features available from 4.8 and onwards.
    wire_version_49:              12, # Supports features available from 4.9 and onwards.
    wire_version_50:              13, # Supports features available from 5.0 and onwards.
    wire_version_51:              14, # Supports features available from 5.1 and onwards.
  ]

  for {key, value} <- values do
    def encode(unquote(key)),   do: unquote(value)
    def decode(unquote(value)), do: unquote(key)
  end

end