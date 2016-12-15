defmodule Mongo.TopologyTest do
  use ExUnit.Case # DO NOT MAKE ASYNCHRONOUS
  alias Mongoman.{ReplicaSet, ReplicaSetConfig}

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    %{pid: pid}
  end

  @modes [:secondary, :secondary_preferred, :primary, :primary_preferred]
  test "replica set selection", %{pid: mongo_pid} do
    for mode <- @modes do
      assert {:ok, %Mongo.InsertOneResult{inserted_id: new_id}} =
               Mongo.insert_one(mongo_pid, "test", %{topology_test: 1}, w: 3)

      rp = Mongo.ReadPreference.defaults(%{mode: mode})
      assert [%{"_id" => ^new_id, "topology_test" => 1}] =
               mongo_pid
               |> Mongo.find("test", %{_id: new_id}, read_preference: rp)
               |> Enum.to_list

      assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} =
               Mongo.delete_one(mongo_pid, "test", %{_id: new_id})
    end
  end
end
