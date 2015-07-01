defmodule Mongo.Test do
  use MongoTest.Case, async: true

  defmodule Pool do
    use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy
  end

  setup_all do
    assert {:ok, _} = Pool.start_link(database: "mongodb_test")
    :ok
  end

  test "aggregate" do
    coll = unique_name

    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 43})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 44})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 45})

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

    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 43})

    assert 2 = Mongo.count(Pool, coll, %{})
    assert 1 = Mongo.count(Pool, coll, %{foo: 42})
  end

  test "distinct" do
    coll = unique_name

    assert [] = Mongo.distinct(Pool, coll, "foo", %{})

    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 43})

    assert [42, 43] = Mongo.distinct(Pool, coll, "foo", %{})
    assert [42]     = Mongo.distinct(Pool, coll, "foo", %{foo: 42})
  end

  test "find" do
    coll = unique_name

    assert [] = Mongo.find(Pool, coll, %{}) |> Enum.to_list

    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42, bar: 1})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 43, bar: 2})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 44, bar: 3})

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}] =
           Mongo.find(Pool, coll, %{}) |> Enum.to_list

    # Mongo is weird with batch_size=1
    assert [%{"foo" => 42}] = Mongo.find(Pool, coll, %{}, batch_size: 1) |> Enum.to_list

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}] =
           Mongo.find(Pool, coll, %{}, batch_size: 2) |> Enum.to_list

    assert [%{"foo" => 42}, %{"foo" => 43}] =
           Mongo.find(Pool, coll, %{}, limit: 2) |> Enum.to_list

    assert [%{"foo" => 42}, %{"foo" => 43}] =
           Mongo.find(Pool, coll, %{}, batch_size: 2, limit: 2) |> Enum.to_list

    assert [%{"foo" => 42}] =
           Mongo.find(Pool, coll, %{bar: 1}) |> Enum.to_list

    assert [%{"bar" => 1}, %{"bar" => 2}, %{"bar" => 3}] =
           Mongo.find(Pool, coll, %{}, projection: %{bar: 1}) |> Enum.to_list

    assert [%{"bar" => 1}] =
           Mongo.find(Pool, coll, %{"$query": %{foo: 42}}, projection: %{bar: 1}) |> Enum.to_list
  end

  test "insert_one" do
    coll = unique_name

    assert_raise FunctionClauseError, fn ->
      Mongo.insert_one(Pool, coll, [%{foo: 42, bar: 1}])
    end

    assert {:ok, result} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert %Mongo.InsertOneResult{inserted_id: id} = result

    assert [%{"_id" => ^id, "foo" => 42}] = Mongo.find(Pool, coll, %{_id: id}) |> Enum.to_list

    assert :ok = Mongo.insert_one(Pool, coll, %{}, w: 0)
  end

  test "insert_many" do
    coll = unique_name

    assert_raise FunctionClauseError, fn ->
      Mongo.insert_many(Pool, coll, %{foo: 42, bar: 1})
    end

    assert {:ok, result} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 43}])
    assert %Mongo.InsertManyResult{inserted_ids: %{0 => id0, 1 => id1}} = result

    assert [%{"_id" => ^id0, "foo" => 42}] = Mongo.find(Pool, coll, %{_id: id0}) |> Enum.to_list
    assert [%{"_id" => ^id1, "foo" => 43}] = Mongo.find(Pool, coll, %{_id: id1}) |> Enum.to_list

    assert :ok = Mongo.insert_many(Pool, coll, [%{}], w: 0)
  end
end
