defmodule Mongo.App do
  @moduledoc false

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      {Mongo.IdServer, []},
      {Mongo.PBKDF2Cache, []},
      supervisor(Registry, [:duplicate, :events_registry])
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
