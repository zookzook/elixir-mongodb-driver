defmodule ChangeStream.Application do

  @moduledoc false

  use Application
  import Supervisor.Spec

  def start(_type, _args) do

    children = [
      # this should be a replicat set!
      worker(Mongo, [[name: :mongo, url: "mongodb://localhost:27027/db-1", pool_size: 3]]),
      worker(ChangeStream, [])
    ]

    opts = [strategy: :one_for_one, name: ChangeStream.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
