defmodule Mix.Tasks.Mongo do
  @moduledoc """
  Migrations are used to modify your database collection over time.

  This module provides some helpers for migrating the database, allowing developers to use Elixir to alter their storage in a way that is database independent.

  Migrations typically provide two operations: up and down, allowing us to migrate the database forward or roll it back in case of errors.

  In order to manage migrations, the driver creates a collection called `migrations` in the database, which stores all migrations that have already been executed.
  You can configure the name of this collection with the :collection configuration option.

  ## Creating your first migration

  Migrations are defined inside the "priv/mongo/migrations". You can change the path using the configuration key `:path`.

  Each file in the migrations directory has the following structure:

  NUMBER_NAME.exs
  The NUMBER is a unique number that identifies the migration. It is usually the timestamp of when the migration was created.
  The NAME must also be unique and it quickly identifies what the migration does. For example, if you need to track the "weather" in your system,
  you can start a new file at "priv/mongo/migrations/20190417140000_add_weather_index.exs" that will have the following contents:

      defmodule Mongo.Migrations.AddWeatherIndex do
        def up() do
          indexes = [[key: [files_id: 1, n: 1], name: "files_n_index", unique: true]]
          Mongo.create_indexes(:mongo, "weather", indexes)
        end

        def down() do
              Mongo.drop_index(:mongo, "weather", "files_n_index")
        end
      end

  The up/0 function is responsible to migrate your database forward. The down/0 function is executed whenever you want to rollback.
  The down/0 function must always do the opposite of up/0. Inside those functions, we invoke the API defined in this module, you will
  find conveniences for managing collections, indexes, as well as running custom MongoDB commands.

  To run a migration, we generally use Mix tasks. For example, you can run the migration above by going to the root of your project and typing:

        $ mix mongo.migrate

  You can also roll it back by calling:

        $ mix mongo.drop

  In practice, we don't create migration files by hand either, we typically use mix mongo.gen.migration to generate the file with the proper timestamp and then we just fill in its contents:

        $ mix mongo.gen.migration add_weather_index

  ## Configuration

        config :mongodb_driver,
          migration: [
            path: "migrations",
            otp_app: :mongodb_driver,
            topology: :mongo,
            collection: "migrations"
          ]

  The following migration configuration options are available:
    * `:collection` - Version numbers of migrations will be saved in a
      collection named `migrations` by default. You can configure the name of
      the collection via:

        config :mongodb_driver, :migration, collection: "my_migrations"

    * `:path` - the `priv` sub-directory for migrations. `:path` defaults to "migrations" and migrations should be placed at "priv/mongo/migrations"
    * `:otp_app` - the name of the otp_app to resolve the priv folder, defaults to :mongodb_driver. In most cases you use your application name.
    * `:topology` - the topology for running the migrations, :topology defaults to :mongo
  """

  defmodule Migrate do
    @moduledoc "Runs the migrations when calling `mix mongo.migrate`"
    @shortdoc "Runs all migrations"
    @requirements ["app.start"]

    use Mix.Task

    @impl Mix.Task
    def run(_) do
      Mongo.Migration.migrate()
    end
  end

  defmodule Unlock do
    @moduledoc "Unlocks the migration lock"
    @shortdoc "Unlocks the migration lock"
    @requirements ["app.start"]

    use Mix.Task

    @impl Mix.Task
    def run(_) do
      Mongo.Migration.unlock()
    end
  end

  defmodule Reset do
    @moduledoc "Resets the database when calling `mix mongo.reset`"
    @shortdoc "Resets the database"

    @requirements ["app.start"]

    use Mix.Task

    @impl Mix.Task
    def run(_) do
      Mix.Task.run("mongo.drop")
      Mix.Task.run("mongo.migrate")
    end
  end

  defmodule Drop do
    @moduledoc "Drop the migrations when calling `mix mongo.drop`"
    @shortdoc "Drop the migrations"
    @requirements ["app.start"]

    use Mix.Task

    @impl Mix.Task
    def run(_) do
      Mongo.Migration.drop()
    end
  end
end
