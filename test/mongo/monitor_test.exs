defmodule Mongo.MonitorTest do
  use MongoTest.Case, async: true

  test "Properly retrieve wire version" do
    {:ok, pid} = Mongo.start_link([database: "mongodb", name: MyMongo.Pool])

    Mongo.Monitor.add_conn(pid, MyMongo.Pool, 4)
    assert Mongo.Monitor.wire_version(MyMongo.Pool) == 4
    assert Mongo.Monitor.wire_version(pid) == 4
  end
end
