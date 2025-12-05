defmodule Test.Migrations.FailedMigrationDown do
  def up do
    :ok
  end

  def down(opts) do
    Keyword.get(opts, :foo, :foo) + 1
  end
end
