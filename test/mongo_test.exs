defmodule MongoTest do
  use ExUnit.Case
  alias Mongo.Connection

  test "connect" do
    Connection.start_link(hostname: "localhost", database: "mongodb_test",
                          username: "test_user", password: "test_user")
    :timer.sleep(1000)
  end
end
