defmodule Mongo.MigrationTest do
  use CollectionCase
  use Patch

  alias Mongo.Migration

  test "test lock and unlock", %{pid: top} do
    Mongo.drop_collection(top, "migrations")
    Patch.patch(Mongo.Migration, :get_config, fn -> [topology: top, collection: "migrations", path: "mongo/migrations", otp_app: :mongodb_driver] end)
    assert :locked == Migration.lock()
    assert {:error, :already_locked} == Migration.lock()
    assert :unlocked == Migration.unlock()
    assert {:error, :not_locked} == Migration.unlock()
  end
end
