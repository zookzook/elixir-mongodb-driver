defmodule CrudExample.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      worker(Mongo, [[name: :mongo, database: "db-1", pool_size: 3]])
    ]

    opts = [strategy: :one_for_one, name: CrudExample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
