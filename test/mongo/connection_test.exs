defmodule Mongo.ConnectionTest do
  use MongoTest.Case, async: true
  alias Mongo

  defp connect do
    assert {:ok, pid} =
           Mongo.start_link(hostname: "localhost", database: "mongodb_test")
    pid
  end

  defp connect_auth do
    assert {:ok, pid} =
           Mongo.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "mongodb_user", password: "mongodb_user")
    pid
  end

  defp connect_auth_on_db do
    assert {:ok, pid} =
           Mongo.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "mongodb_admin_user", password: "mongodb_admin_user",
                                 auth_source: "admin_test")
    pid
  end

  defp connect_ssl do
    assert {:ok, pid} =
      Mongo.start_link(hostname: "localhost", database: "mongodb_test", ssl: true)
    pid
  end

  test "connect and ping" do
    pid = connect()
    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
           Mongo.raw_find(pid, "$cmd", %{ping: 1}, %{}, [batch_size: 1])
  end

  @tag :ssl
  test "ssl" do
    pid = connect_ssl()
    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
      Mongo.raw_find(pid, "$cmd", %{ping: 1}, %{}, [batch_size: 1])
  end

  test "auth" do
    pid = connect_auth()
    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
           Mongo.raw_find(pid, "$cmd", %{ping: 1}, %{}, [batch_size: 1])
  end

  test "auth on db" do
    pid = connect_auth_on_db()
    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
           Mongo.raw_find(pid, "$cmd", %{ping: 1}, %{}, [batch_size: 1])
  end

  test "auth wrong" do
    Process.flag(:trap_exit, true)

    opts = [hostname: "localhost", database: "mongodb_test",
            username: "mongodb_user", password: "wrong",
            backoff_type: :stop]

    capture_log fn ->
      assert {:ok, pid} = Mongo.start_link(opts)
      assert_receive {:EXIT, ^pid, {%Mongo.Error{code: 18}, _}}
    end
  end

  test "auth wrong on db" do
    Process.flag(:trap_exit, true)

    opts = [hostname: "localhost", database: "mongodb_test",
            username: "mongodb_admin_user", password: "wrong",
            backoff_type: :stop, auth_source: "admin_test"]

    capture_log fn ->
      assert {:ok, pid} = Mongo.start_link(opts)
      assert_receive {:EXIT, ^pid, {%Mongo.Error{code: 18}, _}}
    end
  end

  test "insert_one flags" do
    pid = connect_auth()
    coll = unique_name()

    assert {:ok, _} =
           Mongo.insert_one(pid, coll, %{foo: 42}, [continue_on_error: true])
  end

  test "find" do
    pid = connect_auth()
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])

    assert {:ok, %{cursor_id: 0, from: 0, num: 2, docs: [%{"foo" => 42}, %{"foo" => 43}]}} =
           Mongo.raw_find(pid, coll, %{}, nil, [])
    assert {:ok, %{cursor_id: 0, from: 0, num: 1, docs: [%{"foo" => 43}]}} =
           Mongo.raw_find(pid, coll, %{}, nil, skip: 1)
  end

  test "find and get_more" do
    pid = connect_auth()
    coll = unique_name()


    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 44}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 45}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 46}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 47}, [])

    assert {:ok, %{cursor_id: cursor_id, from: 0, docs: [%{"foo" => 42}, %{"foo" => 43}]}} =
           Mongo.raw_find(pid, coll, %{}, nil, batch_size: 2)
    assert {:ok, %{cursor_id: ^cursor_id, from: 2, docs: [%{"foo" => 44}, %{"foo" => 45}]}} =
           Mongo.get_more(pid, coll, cursor_id, batch_size: 2)
    assert {:ok, %{cursor_id: ^cursor_id, from: 4, docs: [%{"foo" => 46}, %{"foo" => 47}]}} =
           Mongo.get_more(pid, coll, cursor_id, batch_size: 2)
    assert {:ok, %{cursor_id: 0, from: 6, docs: []}} =
           Mongo.get_more(pid, coll, cursor_id, batch_size: 2)
  end

  test "kill_cursors" do
    pid = connect_auth()
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 44}, [])

    assert {:ok, %{cursor_id: cursor_id, num: 2}} =
           Mongo.raw_find(pid, coll, %{}, nil, batch_size: 2)
    assert :ok = Mongo.kill_cursors(pid, [cursor_id], [])

    assert {:error, %Mongo.Error{code: nil, message: "cursor not found"}} =
           Mongo.get_more(pid, coll, cursor_id, [])
  end

  test "big response" do
    pid    = connect_auth()
    coll   = unique_name()
    size   = 1024*1024
    binary = <<0::size(size)>>

    Enum.each(1..10, fn _ ->
      Mongo.insert_one(pid, coll, %{data: binary}, [w: 0])
    end)

    assert {:ok, %{num: 10}} = Mongo.raw_find(pid, coll, %{}, nil, batch_size: 100)
  end
end
