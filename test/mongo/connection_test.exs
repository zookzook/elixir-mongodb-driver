defmodule Mongo.ConnectionTest do
  use MongoTest.Case, async: true
  alias Mongo

  import ExUnit.CaptureLog

  setup_all do

    assert {:ok, top} = Mongo.start_link(hostname: "localhost", database: "mongodb_test")

    cmd = [
      dropUser: "mongodb_user"
    ]
    Mongo.issue_command(top, cmd, :write, [])
    cmd = [
      dropUser: "mongodb_user2"
    ]
    Mongo.issue_command(top, cmd, :write, [])
    cmd = [
      dropUser: "mongodb_admin_user"
    ]
    Mongo.issue_command(top, cmd, :write, [database: "admin_test"])

    cmd = [
      createUser: "mongodb_user",
      pwd: "mongodb_user",
      roles: []
    ]
    assert {:ok, _} = Mongo.issue_command(top, cmd, :write, [])
    cmd = [
      createUser: "mongodb_user2",
      pwd: "mongodb_admin_user",
      roles: []
    ]
    assert {:ok, _} = Mongo.issue_command(top, cmd, :write, [])

    cmd = [
      createUser: "mongodb_admin_user",
      pwd: "mongodb_admin_user",
      roles: [
        %{role: "readWrite", db: "mongodb_test"},
        %{role: "read", db: "mongodb_test2"}]
    ]
    assert {:ok, _} = Mongo.issue_command(top, cmd, :write, [database: "admin_test"])
  end

  defp connect do
    assert {:ok, pid} = Mongo.start_link(hostname: "localhost", database: "mongodb_test")
    pid
  end

  defp connect_auth do
    assert {:ok, pid} = Mongo.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "mongodb_user", password: "mongodb_user")
    pid
  end

  defp connect_auth_invalid do
    assert {:ok, pid} = Mongo.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "mongodb_user", password: "wrong_password")
    pid
  end

  defp connect_auth_on_db do
    assert {:ok, pid} = Mongo.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "mongodb_admin_user", password: "mongodb_admin_user",
                                 auth_source: "admin_test")
    pid
  end

  defp connect_ssl do
    assert {:ok, pid} =
      Mongo.start_link(hostname: "localhost", database: "mongodb_test", ssl: true, ssl_opts: [ ciphers: ['AES256-GCM-SHA384'], versions: [:"tlsv1.2"] ])
    pid
  end

  defp tcp_count do
    Enum.count(:erlang.ports(), fn port ->
      case :erlang.port_info(port, :name) do
        {:name, 'tcp_inet'} -> true
        _ -> false
      end
    end)
  end

  defp connect_socket_dir do
    assert {:ok, pid} = Mongo.start_link(socket_dir: "/tmp", database: "mongodb_test")
    pid
  end

  defp connect_socket do
    assert {:ok, pid} = Mongo.start_link(socket: "/tmp/mongodb-27017.sock", database: "mongodb_test")
    pid
  end

  @tag :socket
  test "connect socket_dir" do
    pid = connect_socket_dir()
    assert {:ok, %{"ok" => 1.0}} = Mongo.ping(pid)
  end

  @tag :socket
  test "connect socket" do
    pid = connect_socket()
    assert {:ok, %{"ok" => 1.0}} = Mongo.ping(pid)
  end

  test "connect and ping" do
    pid = connect()
    assert {:ok, %{"ok" => 1.0}} = Mongo.ping(pid)
  end

  @tag :ssl
  test "ssl" do
    pid = connect_ssl()
    assert {:ok, %{"ok" => 1.0}} = Mongo.ping(pid)
  end

  test "auth" do
    pid = connect_auth()
    assert {:ok, %{"ok" => 1.0}} =  Mongo.ping(pid)
  end

  test "auth on db" do
    pid = connect_auth_on_db()
    assert {:ok, %{"ok" => 1.0}} = Mongo.ping(pid)
  end

  test "auth wrong" do
    Process.flag(:trap_exit, true)

    opts = [hostname: "localhost", database: "mongodb_test",
            username: "mongodb_user", password: "wrong",
            backoff_type: :stop]

    assert capture_log(fn ->
       {:ok, pid} = Mongo.start_link(opts)
       assert_receive {:EXIT, ^pid, :killed}, 5000
    end)
  end

  test "auth wrong on db" do
    Process.flag(:trap_exit, true)

    opts = [hostname: "localhost", database: "mongodb_test",
            username: "mongodb_admin_user", password: "wrong",
            backoff_type: :stop, auth_source: "admin_test"]

    assert capture_log(fn ->
       {:ok, pid} = Mongo.start_link(opts)
       assert_receive {:EXIT, ^pid, :killed}, 5000
     end) =~ "(Mongo.Error) auth failed for user mongodb_admin_user"
  end

  test "insert_one flags" do
    pid = connect_auth()
    coll = unique_name()

    assert {:ok, _} =
           Mongo.insert_one(pid, coll, %{foo: 42}, [continue_on_error: true])
  end

  def find(pid, coll, query, _select, opts) do
    Mongo.find(pid, coll, query, opts) |> Enum.to_list() |> Enum.map(fn m ->  Map.pop(m, "_id") |> elem(1) end)
  end

  test "find" do
    pid = connect_auth()
    coll = unique_name()
    Mongo.delete_many(pid, coll, %{})

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])

    assert [%{ "foo" => 42}, %{"foo" => 43}] = find(pid, coll, %{}, nil, [])
    assert [%{"foo" => 43}]                  = find(pid, coll, %{}, nil, skip: 1)

  end

  test "big response" do
    pid    = connect_auth()
    coll   = unique_name()
    size   = 1024*1024
    binary = <<0::size(size)>>

    Mongo.delete_many(pid, coll, %{})

    Enum.each(1..10, fn _ ->
      Mongo.insert_one(pid, coll, %{data: binary}, [w: 0])
    end)

    assert 10 = find(pid, coll, %{}, nil, batch_size: 100) |> Enum.count()
  end

  test "auth connection leak" do
    assert capture_log(fn ->
      assert tcp_count() == 6
      Enum.each(1..10, fn _ ->
        connect_auth_invalid()
      end)
      :timer.sleep(1000)
      # there should be 36 connections with connection_type: :monitor
      # 6 of setup, and 10*3 for three nodes
      assert tcp_count() == 36
    end)
  end
end
