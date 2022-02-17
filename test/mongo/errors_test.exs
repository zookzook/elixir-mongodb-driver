defmodule Mongo.ErrorsTest do
  use ExUnit.Case, async: false

  alias Mongo.Error

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

  @resumable [@host_unreachable, @host_not_found, @network_timeout, @shutdown_in_progress, @primary_stepped_down,
    @exceeded_time_limit, @socket_exception, @not_master, @interrupted_at_shutdown, @interrupted_at_shutdown,
    @interrupted_due_to_repl_state_change, @not_master_no_slaveok, @not_master_or_secondary, @stale_shard_version,
    @stale_epoch, @retry_change_stream, @failed_to_satisfy_read_preference] #@stale_config,

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  test "resumable errors", %{pid: top} do

    @resumable
    |> Enum.map(fn code ->

      fail_cmd = [
        configureFailPoint: "failCommand",
        mode: %{times: 1},
        data: [errorCode: code, failCommands: ["find"]]
      ]

      assert {:ok, _} = Mongo.admin_command(top, fail_cmd)
      assert {:error, msg} = Mongo.find_one(top, "test", %{})
      assert msg.resumable == true
    end)

    fail_cmd = [configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [failCommands: ["find"], errorCode: 2, errorLabels: ["ResumableChangeStreamError"]]
    ]

    assert {:ok, _} = Mongo.admin_command(top, fail_cmd)
    assert {:error, msg} = Mongo.find_one(top, "test", %{})

    assert msg.resumable == true

  end

  test "handle connection error" do
    the_error = %DBConnection.ConnectionError{}
    assert false == Error.not_writable_primary_or_recovering?(the_error, [])
    assert false == Error.should_retry_read(the_error, [ping: 1], [])
  end

end