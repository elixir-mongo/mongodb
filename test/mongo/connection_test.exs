defmodule Mongo.ConnectionTest do
  use MongoTest.Case, async: true
  alias Mongo.Connection
  alias Mongo.ReadResult, as: Read
  alias Mongo.WriteResult, as: Write

  defp connect do
    assert {:ok, pid} =
           Connection.start_link(hostname: "localhost", database: "mongodb_test")
    pid
  end

  defp connect_auth do
    assert {:ok, pid} =
           Connection.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "mongodb_user", password: "mongodb_user")
    pid
  end

  test "connect and ping" do
    pid = connect()
    assert %{"ok" => 1.0} = Connection.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "auth" do
    pid = connect_auth()

    assert %{"ok" => 1.0} = Connection.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "auth wrong" do
    Process.flag(:trap_exit, true)

    opts = [hostname: "localhost", database: "mongodb_test",
            username: "mongodb_user", password: "wrong"]

    capture_log fn ->
      assert {:ok, pid} = Connection.start_link(opts)
      assert_receive {:EXIT, ^pid, %Mongo.Error{code: 18}}
    end
  end

  test "change default database" do
    pid = connect()

    assert "mongodb_test" = Connection.database(pid)
    Connection.database(pid, "mongodb_test2")
    assert "mongodb_test2" = Connection.database(pid)
    assert %{"ok" => 1.0} = Connection.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "insert and find_one" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, %Write{type: :insert, num_inserted: 1}} =
           Connection.insert(pid, coll, %{foo: 42, bar: 43})
    assert %{"foo" => 42} = Connection.find_one(pid, coll, %{foo: 42}, nil)
    assert %{"bar" => 43} = Connection.find_one(pid, coll, %{}, %{bar: 43})
  end

  test "insert flags" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} =
           Connection.insert(pid, coll, %{foo: 42}, [continue_on_error: true])
  end

  test "find" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])

    assert {:ok, %Read{cursor_id: 0, from: 0, num: 2, docs: [%{"foo" => 42}, %{"foo" => 43}]}} =
           Connection.find(pid, coll, %{}, nil)
    assert {:ok, %Read{cursor_id: 0, from: 0, num: 1, docs: [%{"foo" => 43}]}} =
           Connection.find(pid, coll, %{}, nil, skip: 1)
  end

  test "find and get_more" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 44}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 45}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 46}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 47}, [])

    assert {:ok, %Read{cursor_id: cursor_id, from: 0, docs: [%{"foo" => 42}, %{"foo" => 43}]}} =
           Connection.find(pid, coll, %{}, nil, batch_size: 2)
    assert {:ok, %Read{cursor_id: ^cursor_id, from: 2, docs: [%{"foo" => 44}, %{"foo" => 45}]}} =
           Connection.get_more(pid, coll, cursor_id, batch_size: 2)
    assert {:ok, %Read{cursor_id: ^cursor_id, from: 4, docs: [%{"foo" => 46}, %{"foo" => 47}]}} =
           Connection.get_more(pid, coll, cursor_id, batch_size: 2)
    assert {:ok, %Read{cursor_id: 0, from: 6, docs: []}} =
           Connection.get_more(pid, coll, cursor_id, batch_size: 2)
  end

  test "kill_cursors" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 44}, [])

    assert {:ok, %Read{cursor_id: cursor_id, num: 2}} =
           Connection.find(pid, coll, %{}, nil, batch_size: 2)
    assert :ok = Connection.kill_cursors(pid, cursor_id)

    assert {:error, %Mongo.Error{code: nil, message: "cursor not found"}} =
           Connection.get_more(pid, coll, cursor_id)
  end

  test "update" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])

    assert {:ok, %Write{type: :update, num_matched: 2}} =
           Connection.update(pid, coll, %{}, %{"$inc": %{foo: 1}}, multi: true)
    assert {:ok, %Read{docs: [%{"foo" => 43}, %{"foo" => 44}]}} =
           Connection.find(pid, coll, %{}, nil)

    assert {:ok, %Write{type: :update, num_matched: 1}} =
           Connection.update(pid, coll, %{}, %{"$inc": %{foo: 1}}, multi: false)
    assert {:ok, %Read{docs: [%{"foo" => 44}, %{"foo" => 44}]}} =
           Connection.find(pid, coll, %{}, nil)

    assert {:ok, %Write{type: :update, num_matched: 1, upserted_id: %BSON.ObjectId{}}} =
           Connection.update(pid, coll, %{foo: 0}, %{bar: 42}, upsert: true)
    assert {:ok, %Read{docs: [%{"bar" => 42}]}} =
           Connection.find(pid, coll, %{bar: 42}, nil)

    assert {:ok, %Write{type: :update, num_matched: 0}} =
           Connection.update(pid, coll, %{foo: 0}, %{bar: 42}, upsert: false)
    assert {:ok, %Read{docs: []}} =
           Connection.find(pid, coll, %{bar: 0}, nil)
  end

  test "remove" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])

    assert {:ok, %Write{num_matched: 1, num_removed: 1}} =
           Connection.remove(pid, coll, %{foo: 42}, multi: false)
    assert {:ok, %Read{num: 2}} = Connection.find(pid, coll, %{foo: 42}, nil)

    assert {:ok, %Write{num_matched: 2, num_removed: 2}} =
           Connection.remove(pid, coll, %{foo: 42}, multi: true)
    assert {:ok, %Read{num: 0}} = Connection.find(pid, coll, %{foo: 42}, nil)

    assert {:ok, %Read{num: 1}} = Connection.find(pid, coll, %{foo: 43}, nil)
  end
end
