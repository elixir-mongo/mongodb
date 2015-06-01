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

    assert {:ok, %{_id: id}} = Mongo.insert(pid, coll, %{foo: 42})
    assert %{"_id" => ^id, "foo" => 42} = Mongo.find_one(pid, coll, %{foo: 42}, nil)
  end

  test "insert flags" do
    pid = connect_auth()
    coll = unique_name

    assert {:ok, %{_id: _, foo: 42}} =
           Mongo.insert(pid, coll, %{foo: 42}, [continue_on_error: true])
  end
end
