defmodule CollectionCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  @seeds ["127.0.0.1:27017"]

  setup_all do
    assert {:ok, pid} = Mongo.start_link(database: "mongodb_test", seeds: @seeds, show_sensitive_data_on_connection_error: true)
    Mongo.admin_command(pid, configureFailPoint: "failCommand", mode: "off")
    Mongo.drop_database(pid, nil, w: 3)
    {:ok, [pid: pid]}
  end

  setup do
    {:ok, catcher} = EventCatcher.start_link()
    on_exit(fn -> EventCatcher.stop(catcher) end)
    [catcher: catcher]
  end

  using do
    quote do
      import CollectionCase
    end
  end

  defmacro unique_collection do
    {function, _arity} = __CALLER__.function

    "#{__CALLER__.module}.#{function}"
    |> String.replace(" ", "_")
    |> String.replace(".", "_")
    |> String.replace(":", "_")
    |> String.downcase()
  end
end
