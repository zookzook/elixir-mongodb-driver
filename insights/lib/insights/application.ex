defmodule Insights.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Mongo, [name: :mongo, url: "mongodb://localhost:27017/insights", timeout: 60_000, pool_size: 1, idle_interval: 10_000]},
      # Start the Telemetry supervisor
      InsightsWeb.Telemetry,
      # Start the PubSub system
      {Phoenix.PubSub, name: Insights.PubSub},
      {Insights.EventHandler, []},
      # Start the Endpoint (http/https)
      InsightsWeb.Endpoint
      # Start a worker by calling: Insights.Worker.start_link(arg)
      # {Insights.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Insights.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    InsightsWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
