defmodule Mongo.TopologyTest do
  use MongoTest.Case # DO NOT MAKE ASYNCHRONOUS
  alias Mongoman.{ReplicaSet, ReplicaSetConfig}

  @replset "thetestset"

  setup_all do
    config = ReplicaSetConfig.make(@replset, 3)
    {:ok, rs_pid} = ReplicaSet.start_link(config)
    on_exit fn -> ReplicaSet.delete_config(config) end

    nodes = ReplicaSet.nodes(rs_pid)
    {:ok, mongo_pid} = Mongo.start_link(database: "test", seeds: nodes)

    %{pid: mongo_pid}
  end

  @modes [:secondary, :secondary_preferred, :primary] #, :primary_preferred]
  test "replica set selection", %{pid: mongo_pid} do
    assert {:ok, %Mongo.InsertOneResult{inserted_id: new_id}} =
             Mongo.insert_one(mongo_pid, "test", %{topology_test: 1}, w: 3)

    for mode <- @modes do
      rp = Mongo.ReadPreference.defaults(%{mode: mode})
      assert [%{"_id" => ^new_id, "topology_test" => 1}] =
               mongo_pid
               |> Mongo.find("test", %{_id: new_id}, read_preference: rp)
               |> Enum.to_list
    end

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} =
             Mongo.delete_one(mongo_pid, "test", %{_id: new_id})
  end
end
