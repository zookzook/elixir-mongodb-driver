defmodule Mongo.TransactionRetriesTest do
  use CollectionCase

  alias Mongo.Session

  @tag :rs_required
  test "transaction returns an error", %{pid: top} do
    coll = unique_collection()

    :ok = Mongo.create(top, coll)

    {:ok, session} = Session.start_session(top, :write, [])
    assert :ok = Session.start_transaction(session)

    assert {:ok, _} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 1],
      data: [errorCode: 3, failCommands: ["commitTransaction"]]
    ]

    {:ok, _doc} = Mongo.admin_command(top, cmd)

    {:error, %Mongo.Error{}} = Session.commit_transaction(session)

    assert :ok == Session.abort_transaction(session)
    assert :ok == Session.end_session(top, session)
  end

  @tag :rs_required
  test "transaction retry", %{pid: top, catcher: catcher} do
    coll = unique_collection()

    :ok = Mongo.create(top, coll)

    {:ok, session} = Session.start_session(top, :write, [])
    assert :ok = Session.start_transaction(session)

    assert {:ok, _} = Mongo.insert_one(top, coll, %{name: "Greta"}, session: session)

    cmd = [
      configureFailPoint: "failCommand",
      mode: [times: 3],
      data: [errorCode: 6, failCommands: ["commitTransaction"], errorLabels: ["UnknownTransactionCommitResult"]]
    ]

    {:ok, _doc} = Mongo.admin_command(top, cmd)
    assert :ok == Session.commit_transaction(session)

    assert :ok == Session.end_session(top, session)

    assert [:commitTransaction, :commitTransaction, :commitTransaction] = EventCatcher.failed_events(catcher) |> Enum.map(fn event -> event.command_name end)
    assert [:commitTransaction, :configureFailPoint, :insert, :create] = get_succeeded_events(catcher) |> Enum.reject(fn event -> event == :more_to_come end)
  end

  @tag :rs_required
  test "with_transaction, return an error", %{pid: top} do
    coll = unique_collection()

    :ok = Mongo.create(top, coll)

    assert {:error, %Mongo.Error{}} =
             Session.with_transaction(top, fn opts ->
               {:ok, _} = Mongo.insert_one(top, coll, %{name: "Greta"}, opts)
               {:ok, _} = Mongo.insert_one(top, coll, %{name: "Waldo"}, opts)
               {:ok, _} = Mongo.insert_one(top, coll, %{name: "Tom"}, opts)

               cmd = [
                 configureFailPoint: "failCommand",
                 mode: [times: 1],
                 data: [errorCode: 3, failCommands: ["commitTransaction"]]
               ]

               {:ok, _doc} = Mongo.admin_command(top, cmd)

               {:ok, []}
             end)
  end

  @tag :rs_required
  test "with_transaction, retry commit", %{pid: top} do
    coll = unique_collection()

    :ok = Mongo.create(top, coll)

    assert {:ok, []} =
             Session.with_transaction(top, fn opts ->
               {:ok, _} = Mongo.insert_one(top, coll, %{name: "Greta"}, opts)
               {:ok, _} = Mongo.insert_one(top, coll, %{name: "Waldo"}, opts)
               {:ok, _} = Mongo.insert_one(top, coll, %{name: "Tom"}, opts)

               cmd = [
                 configureFailPoint: "failCommand",
                 mode: [times: 3],
                 data: [errorCode: 6, failCommands: ["commitTransaction"], errorLabels: ["UnknownTransactionCommitResult"]]
               ]

               {:ok, _doc} = Mongo.admin_command(top, cmd)

               {:ok, []}
             end)
  end

  @tag :rs_required
  test "with_transaction, retry commit timeout", %{pid: top, catcher: catcher} do
    coll = unique_collection()

    :ok = Mongo.create(top, coll)

    assert {:error, %Mongo.Error{code: 6, error_labels: ["UnknownTransactionCommitResult"]}} =
             Session.with_transaction(
               top,
               fn opts ->
                 {:ok, _} = Mongo.insert_one(top, coll, %{name: "Greta"}, opts)

                 cmd = [
                   configureFailPoint: "failCommand",
                   mode: "alwaysOn",
                   data: [errorCode: 6, failCommands: ["commitTransaction"], errorLabels: ["UnknownTransactionCommitResult"]]
                 ]

                 {:ok, _doc} = Mongo.admin_command(top, cmd)

                 {:ok, []}
               end,
               transaction_retry_timeout_s: 2
             )

    Mongo.admin_command(top, configureFailPoint: "failCommand", mode: "off")

    assert [:configureFailPoint, :abortTransaction, :configureFailPoint, :insert, :create] = get_succeeded_events(catcher)
  end

  @tag :rs_required
  test "with_transaction, retry transaction timeout", %{pid: top, catcher: catcher} do
    coll = unique_collection()

    :ok = Mongo.create(top, coll)

    cmd = [
      configureFailPoint: "failCommand",
      mode: "alwaysOn",
      data: [errorCode: 6, failCommands: ["commitTransaction"], errorLabels: ["TransientTransactionError"]]
    ]

    {:ok, _doc} = Mongo.admin_command(top, cmd)

    assert {:error, %Mongo.Error{code: 6, error_labels: ["TransientTransactionError"]}} =
             Session.with_transaction(
               top,
               fn opts ->
                 {:ok, _} = Mongo.insert_one(top, coll, %{name: "Greta"}, opts)
                 {:ok, []}
               end,
               transaction_retry_timeout_s: 2
             )

    Mongo.admin_command(top, configureFailPoint: "failCommand", mode: "off")

    assert [:configureFailPoint, :abortTransaction, :insert, :configureFailPoint, :create] = get_succeeded_events(catcher) |> Enum.reject(fn event -> event == :more_to_come end)
  end

  defp get_succeeded_events(catcher) do
    catcher
    |> EventCatcher.succeeded_events()
    |> Enum.map(fn event -> event.command_name end)
    |> Enum.reject(fn event -> event == :isMaster end)
  end
end
