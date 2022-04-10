defmodule PromEx.Plugins.MongoDB do
  use PromEx.Plugin

  @impl true
  def event_metrics(opts) do
    Event.build(
      :mongodb_cmd_duration,
      [
        # Capture command execution duration information
        distribution(
          [:mongodb_driver, :execution, :duration, :milliseconds],
          event_name: [:mongodb_driver, :execution],
          measurement: :duration, description: "The execution time for a command",
          reporter_options: [
          buckets: [10, 100, 500, 1_000, 5_000, 10_000, 30_000]
          ],
          #tag_values: [:todo],
          tags: [:collection, :command],
          unit: {:microsecond, :millisecond}
        )
      ]
    )

  end
end
