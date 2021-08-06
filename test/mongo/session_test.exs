defmodule Mongo.SessionTest do
  use ExUnit.Case

  alias Mongo.Session

  @moduletag :session

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()

    [pid: pid]
  end

  setup %{pid: pid} do
    table = "tab_#{System.unique_integer([:positive])}"
    assert {:ok, _} = Mongo.command(pid, %{create: table})

    [table: table]
  end

  describe "lifetime" do
    test "session can be created", %{pid: pid} do
      assert {:ok, session} = Mongo.start_session(pid)

      assert is_pid(session)
      assert Process.alive?(session)
    end

    test "created session is not ended", %{pid: pid} do
      assert {:ok, session} = Mongo.start_session(pid)
      refute Session.ended?(session)
    end
  end

  describe "transaction" do
    setup %{pid: pid} do
      assert {:ok, session} = Mongo.start_session(pid)

      [session: session]
    end

    test "can start new transaction", %{session: session} do
      assert :ok = Session.start_transaction(session)
    end

    test "cannot start new transaction when there is transaction in progress", %{session: session} do
      assert :ok = Session.start_transaction(session)
      assert {:error, _} = Session.start_transaction(session)
    end

    test "started session can be commited", %{pid: pid, session: session, table: table} do
      assert :ok = Session.start_transaction(session)
      assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
      assert :ok = Session.commit_transaction(session)
    end

    test "started session can be aborted", %{pid: pid, session: session, table: table} do
      assert :ok = Session.start_transaction(session)
      assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
      assert :ok = Session.abort_transaction(session)
    end

    test "commited transaction cannot be aborted", %{pid: pid, session: session, table: table} do
      assert :ok = Session.start_transaction(session)
      assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
      assert :ok = Session.commit_transaction(session)
      assert {:error, _} = Session.abort_transaction(session)
    end

    test "commited transaction can be commited", %{pid: pid, session: session, table: table} do
      assert :ok = Session.start_transaction(session)
      assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
      assert :ok = Session.commit_transaction(session)
      assert :ok = Session.commit_transaction(session)
    end

    test "data in one transaction aren't visible in other", %{pid: pid, session: s1, table: table} do
      assert {:ok, s2} = Mongo.start_session(pid)

      assert s1 != s2

      assert :ok = Session.start_transaction(s1)
      assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: s1)

      assert nil == Mongo.find_one(pid, table, %{}, session: s2)

      assert :ok = Session.commit_transaction(s1)

      refute nil == Mongo.find_one(pid, table, %{})
    end

    tasks = [:commit, :abort]

    for task <- tasks do
      test "aborted transaction cannot be #{task}ed", %{
        pid: pid,
        session: session,
        table: table
      } do
        assert :ok = Session.start_transaction(session)
        assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
        assert :ok = Session.abort_transaction(session)
        assert {:error, _} = Session.unquote(:"#{task}_transaction")(session)
      end
    end

    test "inserts in commited transactions are visible after commit", %{
      pid: pid,
      session: session,
      table: table
    } do
      assert :ok = Session.start_transaction(session)
      assert {:ok, result} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
      id = result.inserted_id
      assert :ok = Session.commit_transaction(session)
      assert Mongo.find_one(pid, table, _id: id)
    end

    test "inserts in aborted transactions are ignored", %{
      pid: pid,
      session: session,
      table: table
    } do
      assert :ok = Session.start_transaction(session)
      assert {:ok, result} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)
      id = result.inserted_id
      assert :ok = Session.abort_transaction(session)
      refute Mongo.find_one(pid, table, _id: id)
    end
  end

  describe "with_transaction commits changes on exit" do
    setup %{pid: pid} do
      assert {:ok, session} = Mongo.start_session(pid)

      [session: session]
    end

    test "returns passed value", %{session: session} do
      assert {:ok, :ok} == Session.with_transaction(session, fn -> :ok end)
    end

    test "inserts are persisted after transaction end", %{
      pid: pid,
      session: session,
      table: table
    } do
      assert {:ok, result} =
               Session.with_transaction(session, fn ->
                 assert {:ok, result} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)

                 result
               end)

      assert Mongo.find_one(pid, table, _id: result.inserted_id)
    end

    test "aborts session and reraises error that occured within function", %{
      pid: pid,
      session: session,
      table: table
    } do
      assert_raise RuntimeError, "example error", fn ->
        Session.with_transaction(session, fn ->
          assert {:ok, _} = Mongo.insert_one(pid, table, %{foo: 1}, session: session)

          raise "example error"

          :ok
        end)
      end

      assert nil == Mongo.find_one(pid, table, [])
    end
  end
end
