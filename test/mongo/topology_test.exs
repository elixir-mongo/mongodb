defmodule Mongo.TopologyTest do
  # DO NOT MAKE ASYNCHRONOUS
  use ExUnit.Case

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()
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
               |> Enum.to_list()

      assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} =
               Mongo.delete_one(mongo_pid, "test", %{_id: new_id})
    end
  end

  test "remove old connection_pool", %{pid: mongo_pid} do
    :erlang.trace(mongo_pid, true, [:receive])
    %{monitors: %{"127.0.0.1:27001" => monitor_pid}} = state = :sys.get_state(mongo_pid)

    case state do
      %{connection_pools: %{"127.0.0.1:27001" => connection_pid}} ->
        GenServer.cast(mongo_pid, {:connected, monitor_pid})

        %{connection_pools: %{"127.0.0.1:27001" => new_connection_pid}} =
          :sys.get_state(mongo_pid)

        assert connection_pid != new_connection_pid

      _ ->
        assert_receive {:trace, ^mongo_pid, :receive, {:"$gen_cast", {:connected, ^monitor_pid}}}
        %{connection_pools: %{"127.0.0.1:27001" => connection_pid}} = :sys.get_state(mongo_pid)
        ref = Process.monitor(connection_pid)
        GenServer.cast(mongo_pid, {:connected, monitor_pid})
        assert_receive {:DOWN, ^ref, :process, ^connection_pid, :normal}

        %{connection_pools: %{"127.0.0.1:27001" => new_connection_pid}} =
          :sys.get_state(mongo_pid)

        assert connection_pid != new_connection_pid
    end
  end
end
