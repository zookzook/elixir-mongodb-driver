defmodule Mongo.Version do
  @moduledoc """
  This module contains the constant of all wire versions.
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
  ]

  for {key, value} <- values do
    def encode(unquote(key)),   do: unquote(value)
    def decode(unquote(value)), do: unquote(key)
  end

end