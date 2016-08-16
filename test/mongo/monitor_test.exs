defmodule Mongo.MonitorTest do
  use MongoTest.Case, async: true

  test "Properly retrieve wire version" do
    {:ok, pid} = Mongo.start_link([database: "mongodb", name: MyMongo.Pool])
    Mongo.command!(pid, %{ping: true})

    assert is_integer(Mongo.Monitor.wire_version(MyMongo.Pool))
    assert is_integer(Mongo.Monitor.wire_version(pid))
  end
end
