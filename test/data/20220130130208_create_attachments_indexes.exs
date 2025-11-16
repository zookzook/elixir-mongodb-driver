defmodule Test.Migrations.SuccessfulMigration do
  def up(opts) do
    topology = Keyword.get(opts, :topology)
    Mongo.create(topology, "attachments")

    indexes = [
      [key: [uuid: 1], name: "attachments_uuid_index", unique: true]
    ]

    Mongo.create_indexes(topology, "attachments", indexes)
  end

  def down(opts) do
    topology = Keyword.get(opts, :topology)
    Mongo.drop_collection(topology, "attachments")
  end
end
