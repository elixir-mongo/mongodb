defmodule Mongo.TopologyTest do
  use MongoTest.Case # DO NOT MAKE ASYNCHRONOUS
  alias Mongoman.{ReplicaSet, ReplicaSetConfig}

  @replset "thetestset"

  setup_all do
    {:ok, rs_pid} = ReplicaSet.start_link(ReplicaSetConfig.make(@replset, 3))
    nodes = ReplicaSet.nodes(rs_pid)
    {:ok, mongo_pid} = Mongo.start_link(database: "test", seeds: nodes)

    %{pid: mongo_pid}
  end

  test "replica set selection", %{pid: mongo_pid} do
    assert {:ok, %Mongo.InsertOneResult{inserted_id: new_id}} =
             Mongo.insert_one(mongo_pid, "test", %{topology_test: 1}, w: 3)

    rp = Mongo.ReadPreference.defaults(%{mode: :secondary})
    assert [%{"_id" => new_id, "topology_test" => 1}] =
             mongo_pid
             |> Mongo.find("test", %{_id: new_id}, read_preference: rp)
             |> Enum.to_list

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} =
             Mongo.delete_one(mongo_pid, "test", %{_id: new_id})
  end

  # TODO: test sharded replica set selection
end
