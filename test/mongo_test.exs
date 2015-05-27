defmodule MongoTest do
  # async: true doesn't work
  use ExUnit.Case, async: true
  alias Mongo
  alias Mongo.Connection

  test "connect and ping" do
    assert {:ok, pid} =
           Connection.start_link(hostname: "localhost", database: "mongodb_test")
    assert %{"ok" => 1.0} = Mongo.find_one(pid, "$cmd", %{ping: 1}, %{})
  end

  test "auth" do
    assert {:ok, pid} =
           Connection.start_link(hostname: "localhost", database: "mongodb_test",
                                 username: "test_user", password: "test_user")
    assert %{"ok" => 1.0} = Mongo.find_one(pid, "$cmd", %{ping: 1}, %{})
  end
end
