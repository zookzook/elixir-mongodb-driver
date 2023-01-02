defmodule Mongo.Migration do
  @moduledoc false
  use Mongo.Collection

  def migrate(opts \\ []) do
    with :locked <- lock(opts) do
      migration_files!(opts)
      |> compile_migrations()
      |> Enum.each(fn {mod, version} -> run_up(version, mod, opts) end)

      unlock(opts)
    end
  rescue
    error ->
      IO.puts("🚨 Error when migrating: #{inspect(error)}")
      unlock(opts)
  end

  def drop(opts \\ []) do
    with :locked <- lock(opts) do
      migration_files!(opts)
      |> compile_migrations()
      |> Enum.reverse()
      |> Enum.each(fn {mod, version} -> run_down(version, mod, opts) end)

      unlock(opts)
    end
  rescue
    error ->
      IO.puts("🚨 Error when dropping: #{inspect(error)}")
      unlock(opts)
  end

  def lock(opts \\ []) do
    topology = get_config(opts)[:topology]
    collection = get_config(opts)[:collection]

    query = %{_id: "lock", used: false}
    set = %{"$set": %{used: true}}

    case Mongo.update_one(topology, collection, query, set, upsert: true) do
      {:ok, %{modified_count: 1}} ->
        IO.puts("🔒 #{collection} locked")
        :locked

      {:ok, %{upserted_ids: ["lock"]}} ->
        IO.puts("🔒 #{collection} locked")
        :locked

      _other ->
        {:error, :already_locked}
    end
  end

  def unlock(opts \\ []) do
    topology = get_config(opts)[:topology]
    collection = get_config(opts)[:collection]
    query = %{_id: "lock", used: true}
    set = %{"$set": %{used: false}}

    case Mongo.update_one(topology, collection, query, set) do
      {:ok, %{modified_count: 1}} ->
        IO.puts("🔓 #{collection} unlocked")
        :unlocked

      _other ->
        {:error, :not_locked}
    end
  end

  defp run_up(version, mod, opts) do
    topology = get_config(opts)[:topology]
    collection = get_config(opts)[:collection]

    case Mongo.find_one(topology, collection, %{version: version}) do
      nil ->
        ## check, if the function supports options

        cond do
          function_exported?(mod, :up, 1) ->
            apply(mod, :up, [opts])

          function_exported?(mod, :up, 0) ->
            apply(mod, :up, [])

          true ->
            raise "The module does not export the up function!"
        end

        Mongo.insert_one(topology, collection, %{version: version})
        IO.puts("⚡️ Successfully migrated #{mod}")

      _other ->
        :noop
    end
  rescue
    e ->
      IO.puts("🚨 Error when migrating #{mod}:")
      IO.puts(Exception.format(:error, e, __STACKTRACE__))
      reraise e, __STACKTRACE__
  end

  defp run_down(version, mod, opts) do
    topology = get_config(opts)[:topology]
    collection = get_config(opts)[:collection]

    case Mongo.find_one(topology, collection, %{version: version}) do
      %{"version" => _version} ->
        ## check, if the function supports options
        cond do
          function_exported?(mod, :down, 1) ->
            apply(mod, :down, [opts])

          function_exported?(mod, :down, 0) ->
            apply(mod, :down, [])

          true ->
            raise "The module does not export the down function!"
        end

        Mongo.delete_one(topology, collection, %{version: version})
        IO.puts("💥 Successfully dropped #{mod}")

      _other ->
        :noop
    end
  rescue
    e ->
      IO.puts("🚨 Error when dropping #{mod}:")
      IO.puts(Exception.format(:error, e, __STACKTRACE__))
      reraise e, __STACKTRACE__
  end

  def get_config(opts \\ []) do
    defaults = [topology: :mongo, collection: "migrations", path: "migrations", otp_app: :mongodb_driver]

    defaults
    |> Keyword.merge(Application.get_env(:mongodb_driver, :migration, []))
    |> Keyword.merge(opts)
  end

  def migration_file_path(opts \\ []) do
    path = get_config(opts)[:path]
    topology = get_config(opts)[:topology]
    otp_app = get_config(opts)[:otp_app]
    Path.join([:code.priv_dir(otp_app), to_string(topology), path])
  end

  def migration_files!(opts) do
    file_path = migration_file_path(opts)

    case File.ls(file_path) do
      {:ok, files} ->
        files
        |> Enum.sort()
        |> Enum.map(fn file_name -> Path.join([file_path, file_name]) end)

      {:error, _error} ->
        raise "Could not find migrations file path #{inspect(file_path)}"
    end
  end

  defp compile_migrations(files) do
    Enum.map(files, fn file ->
      mod =
        file
        |> Code.compile_file()
        |> Enum.map(&elem(&1, 0))
        |> List.first()

      version =
        ~r/[0-9]/
        |> Regex.scan(Path.basename(file))
        |> Enum.join()
        |> String.to_integer()

      {mod, version}
    end)
  end
end
