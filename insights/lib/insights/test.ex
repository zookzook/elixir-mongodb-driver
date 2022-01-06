defmodule Insights.Test do

  require Logger

  @host_unreachable                     6
  @host_not_found                       7
  @network_timeout                      89
  @shutdown_in_progress                 91
  @primary_stepped_down                 189
  @exceeded_time_limit                  262
  @socket_exception                     9001
  @not_master                           10107
  @interrupted_at_shutdown              11600
  @interrupted_due_to_repl_state_change 11602
  @not_master_no_slaveok                13435
  @not_master_or_secondary              13436
  @stale_shard_version                  63
  @stale_epoch                          150
  #@stale_config                         13388
  @retry_change_stream                  234
  @failed_to_satisfy_read_preference    133

  @resumxable [@host_unreachable, @host_not_found, @network_timeout, @shutdown_in_progress, @primary_stepped_down,
    @exceeded_time_limit, @socket_exception, @not_master, @interrupted_at_shutdown, @interrupted_at_shutdown,
    @interrupted_due_to_repl_state_change, @not_master_no_slaveok, @not_master_or_secondary, @stale_shard_version,
    @stale_epoch, @retry_change_stream, @failed_to_satisfy_read_preference] #@stale_config,

  @resumable [@primary_stepped_down ]
  def test() do
    @resumable
    |> Enum.map(fn code ->

      fail_cmd = [
        configureFailPoint: "failCommand",
        mode: %{times: 1},
        data: [errorCode: code, failCommands: ["find"]]
      ]

      {:ok, _} = Mongo.admin_command(:mongo, fail_cmd)
      {:error, msg} = Mongo.find_one(:mongo, "test", %{})
      Logger.info("Error: #{inspect msg}")
    end)
  end
end

