defmodule Mongo.Test do
  use MongoTest.Case, async: true
  alias Mongo.Connection

  defmodule Pool do
    use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy
  end

  setup_all do
    assert {:ok, _} = Pool.start_link(database: "mongodb_test")
    :ok
  end

  test "aggregate" do
    coll = unique_name

    Pool.transaction(fn pid->
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 44}, [])
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 45}, [])
    end)

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}, %{"foo" => 45}] =
           Mongo.aggregate(Pool, coll, []) |> Enum.to_list

    assert []               = Mongo.aggregate(Pool, coll, []) |> Enum.take(0)
    assert []               = Mongo.aggregate(Pool, coll, []) |> Enum.drop(4)
    assert [%{"foo" => 42}] = Mongo.aggregate(Pool, coll, []) |> Enum.take(1)
    assert [%{"foo" => 45}] = Mongo.aggregate(Pool, coll, []) |> Enum.drop(3)

    assert []               = Mongo.aggregate(Pool, coll, [], use_cursor: false) |> Enum.take(0)
    assert []               = Mongo.aggregate(Pool, coll, [], use_cursor: false) |> Enum.drop(4)
    assert [%{"foo" => 42}] = Mongo.aggregate(Pool, coll, [], use_cursor: false) |> Enum.take(1)
    assert [%{"foo" => 45}] = Mongo.aggregate(Pool, coll, [], use_cursor: false) |> Enum.drop(3)

    assert []               = Mongo.aggregate(Pool, coll, [], batch_size: 1) |> Enum.take(0)
    assert []               = Mongo.aggregate(Pool, coll, [], batch_size: 1) |> Enum.drop(4)
    assert [%{"foo" => 42}] = Mongo.aggregate(Pool, coll, [], batch_size: 1) |> Enum.take(1)
    assert [%{"foo" => 45}] = Mongo.aggregate(Pool, coll, [], batch_size: 1) |> Enum.drop(3)
  end

  test "count" do
    coll = unique_name

    assert 0 = Mongo.count(Pool, coll, [])

    Pool.transaction(fn pid->
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])
    end)

    assert 2 = Mongo.count(Pool, coll, %{})
    assert 1 = Mongo.count(Pool, coll, %{foo: 42})
  end

  test "distinct" do
    coll = unique_name

    assert [] = Mongo.distinct(Pool, coll, "foo", %{})

    Pool.transaction(fn pid->
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 42}, [])
      assert {:ok, _} = Connection.insert(pid, coll, %{foo: 43}, [])
    end)

    assert [42, 43] = Mongo.distinct(Pool, coll, "foo", %{})
    assert [42]     = Mongo.distinct(Pool, coll, "foo", %{foo: 42})
  end
end
