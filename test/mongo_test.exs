defmodule MongoTest do
  use MongoTest.Case, async: true
  alias Mongo
  alias Mongo.Connection

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

  defmacrop unique_name do
    {function, _arity} = __CALLER__.function
    "#{__CALLER__.module}.#{function}"
  end

  test "connect and ping" do
    pid = connect()
    assert %{"ok" => 1.0} = Mongo.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "auth" do
    pid = connect_auth()

    assert %{"ok" => 1.0} = Mongo.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "auth wrong" do
    Process.flag(:trap_exit, true)

    opts = [hostname: "localhost", database: "mongodb_test",
            username: "mongodb_user", password: "wrong"]

    capture_log fn ->
      assert {:ok, pid} = Connection.start_link(opts)
      assert_receive {:EXIT, ^pid, %Mongo.Error{code: 18, message: "auth failed" <> _}}
    end
  end

  test "change default database" do
    pid = connect()

    assert "mongodb_test" = Mongo.database(pid)
    Mongo.database(pid, "mongodb_test2")
    assert "mongodb_test2" = Mongo.database(pid)
    assert %{"ok" => 1.0} = Mongo.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "insert and find_one" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, %{_id: id}} = Mongo.insert(pid, coll, %{foo: 42, bar: 43})
    assert %{"_id" => ^id, "foo" => 42} = Mongo.find_one(pid, coll, %{foo: 42}, nil)
    assert %{"_id" => ^id, "bar" => 43} = Mongo.find_one(pid, coll, %{}, %{bar: 43})
  end

  test "insert flags" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, %{_id: _, foo: 42}} =
           Mongo.insert(pid, coll, %{foo: 42}, [continue_on_error: true])
  end

  test "find" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 43}, [])

    assert {:ok, 0, [%{"foo" => 42}, %{"foo" => 43}]} =
           Mongo.find(pid, coll, %{}, nil)
    assert {:ok, _, [%{"foo" => 43}]} =
           Mongo.find(pid, coll, %{}, nil, num_skip: 1)
  end

  test "find and get_more" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 44}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 45}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 46}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 47}, [])

    assert {:ok, cursor_id, [%{"foo" => 42}, %{"foo" => 43}]} =
           Mongo.find(pid, coll, %{}, nil, num_return: 2)
    assert {:ok, ^cursor_id, [%{"foo" => 44}, %{"foo" => 45}]} =
           Mongo.get_more(pid, coll, cursor_id, num_return: 2)
    assert {:ok, ^cursor_id, [%{"foo" => 46}, %{"foo" => 47}]} =
           Mongo.get_more(pid, coll, cursor_id, num_return: 2)
    assert {:ok, 0, []} =
           Mongo.get_more(pid, coll, cursor_id, num_return: 2)
  end

  test "kill_cursors" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 44}, [])

    assert {:ok, cursor_id, [_, _]} =
           Mongo.find(pid, coll, %{}, nil, num_return: 2)
    assert :ok = Mongo.kill_cursors(pid, cursor_id)

    assert {:error, %Mongo.Error{code: nil, message: "cursor not found"}} =
           Mongo.get_more(pid, coll, cursor_id)
  end

  test "update" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 43}, [])

    assert :ok = Mongo.update(pid, coll, %{}, %{"$inc": %{foo: 1}}, multi: true)
    assert {:ok, 0, [%{"foo" => 43}, %{"foo" => 44}]} =
           Mongo.find(pid, coll, %{}, nil)

    assert :ok = Mongo.update(pid, coll, %{}, %{"$inc": %{foo: 1}}, multi: false)
    assert {:ok, 0, [%{"foo" => 44}, %{"foo" => 44}]} =
           Mongo.find(pid, coll, %{}, nil)

    assert :ok = Mongo.update(pid, coll, %{foo: 0}, %{bar: 42}, upsert: true)
    assert {:ok, 0, [%{"bar" => 42}]} = Mongo.find(pid, coll, %{bar: 42}, nil)

    assert :ok = Mongo.update(pid, coll, %{foo: 0}, %{bar: 42}, upsert: false)
    assert {:ok, 0, []} = Mongo.find(pid, coll, %{bar: 0}, nil)
  end

  test "delete" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert(pid, coll, %{foo: 43}, [])

    assert :ok = Mongo.delete(pid, coll, %{foo: 42}, multi: false)
    assert {:ok, 0, [_, _]} = Mongo.find(pid, coll, %{foo: 42}, nil)

    assert :ok = Mongo.delete(pid, coll, %{foo: 42}, multi: true)
    assert {:ok, 0, []} = Mongo.find(pid, coll, %{foo: 42}, nil)

    assert {:ok, 0, [_]} = Mongo.find(pid, coll, %{foo: 43}, nil)
  end
end
