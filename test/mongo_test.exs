defmodule Mongo.Test do
  use MongoTest.Case, async: true

  defmodule Pool do
    use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy
  end

  defmodule LoggingPool do
    use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy

    def log(return, _queue_time, _query_time, fun, args) do
      Process.put(:last_log, {fun, args})
      return
    end
  end

  setup_all do
    assert {:ok, _} = Pool.start_link(database: "mongodb_test")
    assert {:ok, _} = LoggingPool.start_link(database: "mongodb_test")
    :ok
  end

  test "run_command" do
    assert {:ok, %{"ok" => 1.0}} = Mongo.run_command(Pool, %{ping: true})
    assert {:error, %Mongo.Error{}} =
      Mongo.run_command(Pool, %{ drop: "unexisting-database" })
  end

  test "run_command!" do
    assert %{"ok" => 1.0} = Mongo.run_command!(Pool, %{ping: true})
    assert_raise Mongo.Error, fn ->
      Mongo.run_command!(Pool, %{ drop: "unexisting-database" })
    end
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

    assert {:ok, 0} = Mongo.count(Pool, coll, [])

    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 43})

    assert {:ok, 2} = Mongo.count(Pool, coll, %{})
    assert {:ok, 1} = Mongo.count(Pool, coll, %{foo: 42})
  end

  test "count!" do
    coll = unique_name

    assert 0 = Mongo.count!(Pool, coll, %{foo: 43})
  end

  test "distinct" do
    coll = unique_name

    assert {:ok, []} = Mongo.distinct(Pool, coll, "foo", %{})

    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(Pool, coll, %{foo: 43})

    assert {:ok, [42, 43]} = Mongo.distinct(Pool, coll, "foo", %{})
    assert {:ok, [42]}     = Mongo.distinct(Pool, coll, "foo", %{foo: 42})
  end

  test "distinct!" do
    coll = unique_name

    assert [] = Mongo.distinct!(Pool, coll, "foo", %{})
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

    assert [%{"foo" => 44}, %{"foo" => 43}] =
      Mongo.find(Pool, coll, %{}, sort: [foo: -1], batch_size: 2, limit: 2) |> Enum.to_list
  end

  test "insert_one" do
    coll = unique_name

    assert_raise ArgumentError, fn ->
      Mongo.insert_one(Pool, coll, [%{foo: 42, bar: 1}])
    end

    assert {:ok, result} = Mongo.insert_one(Pool, coll, %{foo: 42})
    assert %Mongo.InsertOneResult{inserted_id: id} = result

    assert [%{"_id" => ^id, "foo" => 42}] = Mongo.find(Pool, coll, %{_id: id}) |> Enum.to_list

    assert :ok = Mongo.insert_one(Pool, coll, %{}, w: 0)
  end

  test "insert_one!" do
    coll = unique_name

    assert %Mongo.InsertOneResult{} = Mongo.insert_one!(Pool, coll, %{"_id" => 1})
    assert nil == Mongo.insert_one!(Pool, coll, %{}, w: 0)

    assert_raise Mongo.Error, fn ->
      Mongo.insert_one!(Pool, coll, %{_id: 1})
    end
  end

  test "insert_many" do
    coll = unique_name

    assert_raise ArgumentError, fn ->
      Mongo.insert_many(Pool, coll, %{foo: 42, bar: 1})
    end

    assert {:ok, result} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 43}])
    assert %Mongo.InsertManyResult{inserted_ids: %{0 => id0, 1 => id1},
                                   inserted_count: 2} = result

    assert [%{"_id" => ^id0, "foo" => 42}] = Mongo.find(Pool, coll, %{_id: id0}) |> Enum.to_list
    assert [%{"_id" => ^id1, "foo" => 43}] = Mongo.find(Pool, coll, %{_id: id1}) |> Enum.to_list

    assert :ok = Mongo.insert_many(Pool, coll, [%{}], w: 0)
  end

  test "insert_many!" do
    coll = unique_name

    docs = [%{foo: 42}, %{foo: 43}]
    assert %Mongo.InsertManyResult{} = Mongo.insert_many!(Pool, coll, docs)

    assert nil == Mongo.insert_many!(Pool, coll, [%{}], w: 0)

    assert_raise Mongo.Error, fn ->
      Mongo.insert_many!(Pool, coll, [%{_id: 1}, %{_id: 1}])
    end
  end

  test "delete_one" do
    coll = unique_name

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} = Mongo.delete_one(Pool, coll, %{foo: 42})
    assert [%{"foo" => 42}] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} = Mongo.delete_one(Pool, coll, %{foo: 42})
    assert [] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.DeleteResult{deleted_count: 0}} = Mongo.delete_one(Pool, coll, %{foo: 42})
    assert [%{"foo" => 43}] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
  end

  test "delete_one!" do
    coll = unique_name

    assert %Mongo.DeleteResult{deleted_count: 0} = Mongo.delete_one!(Pool, coll, %{foo: 42})

    assert nil == Mongo.delete_one!(Pool, coll, %{}, w: 0)
  end

  test "delete_many" do
    coll = unique_name

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.DeleteResult{deleted_count: 2}} = Mongo.delete_many(Pool, coll, %{foo: 42})
    assert [] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.DeleteResult{deleted_count: 0}} = Mongo.delete_one(Pool, coll, %{foo: 42})
    assert [%{"foo" => 43}] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
  end

  test "delete_many!" do
    coll = unique_name

    assert %Mongo.DeleteResult{deleted_count: 0} = Mongo.delete_many!(Pool, coll, %{foo: 42})

    assert nil == Mongo.delete_many!(Pool, coll, %{}, w: 0)
  end

  test "replace_one" do
    coll = unique_name

    assert_raise ArgumentError, fn ->
      Mongo.replace_one(Pool, coll, %{foo: 42}, %{"$set": %{foo: 0}})
    end

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} =
           Mongo.replace_one(Pool, coll, %{foo: 42}, %{foo: 0})

    assert [_] = Mongo.find(Pool, coll, %{foo: 0}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.UpdateResult{matched_count: 0, modified_count: 1, upserted_id: id}} =
           Mongo.replace_one(Pool, coll, %{foo: 50}, %{foo: 0}, upsert: true)
    assert [_] = Mongo.find(Pool, coll, %{_id: id}) |> Enum.to_list

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} =
           Mongo.replace_one(Pool, coll, %{foo: 43}, %{foo: 1}, upsert: true)
    assert [] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 1}) |> Enum.to_list
  end

  test "replace_one!" do
    coll = unique_name

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{matched_count: 0, modified_count: 0, upserted_id: nil} =
      Mongo.replace_one!(Pool, coll, %{foo: 43}, %{foo: 0})

    assert nil == Mongo.replace_one!(Pool, coll, %{foo: 45}, %{foo: 0}, w: 0)

    assert_raise Mongo.Error, fn ->
      Mongo.replace_one!(Pool, coll, %{foo: 42}, %{_id: 1})
    end
  end

  test "update_one" do
    coll = unique_name

    assert_raise ArgumentError, fn ->
      Mongo.update_one(Pool, coll, %{foo: 42}, %{foo: 0})
    end

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} =
           Mongo.update_one(Pool, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert [_] = Mongo.find(Pool, coll, %{foo: 0}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.UpdateResult{matched_count: 0, modified_count: 1, upserted_id: id}} =
           Mongo.update_one(Pool, coll, %{foo: 50}, %{"$set": %{foo: 0}}, upsert: true)
    assert [_] = Mongo.find(Pool, coll, %{_id: id}) |> Enum.to_list

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} =
           Mongo.update_one(Pool, coll, %{foo: 43}, %{"$set": %{foo: 1}}, upsert: true)
    assert [] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 1}) |> Enum.to_list
  end

  test "update_one!" do
    coll = unique_name

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil} =
      Mongo.update_one!(Pool, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert nil == Mongo.update_one!(Pool, coll, %{foo: 42}, %{}, w: 0)

    assert_raise Mongo.Error, fn ->
      Mongo.update_one!(Pool, coll, %{foo: 0}, %{"$set": %{_id: 0}})
    end
  end

  test "update_many" do
    coll = unique_name

    assert_raise ArgumentError, fn ->
      Mongo.update_many(Pool, coll, %{foo: 42}, %{foo: 0})
    end

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{matched_count: 2, modified_count: 2, upserted_id: nil}} =
           Mongo.update_many(Pool, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert [_, _] = Mongo.find(Pool, coll, %{foo: 0}) |> Enum.to_list
    assert [] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.UpdateResult{matched_count: 0, modified_count: 1, upserted_id: id}} =
           Mongo.update_many(Pool, coll, %{foo: 50}, %{"$set": %{foo: 0}}, upsert: true)
    assert [_] = Mongo.find(Pool, coll, %{_id: id}) |> Enum.to_list

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_id: nil}} =
           Mongo.update_many(Pool, coll, %{foo: 43}, %{"$set": %{foo: 1}}, upsert: true)
    assert [] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 1}) |> Enum.to_list
  end

  test "update_many!" do
    coll = unique_name

    assert {:ok, _} = Mongo.insert_many(Pool, coll, [%{foo: 42}, %{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{matched_count: 2, modified_count: 2, upserted_id: nil} =
      Mongo.update_many!(Pool, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert nil == Mongo.update_many!(Pool, coll, %{foo: 0}, %{}, w: 0)

    assert_raise Mongo.Error, fn ->
      Mongo.update_many!(Pool, coll, %{foo: 0}, %{"$set": %{_id: 1}})
    end
  end

  test "save_one" do
    coll = unique_name
    id = Mongo.IdServer.new

    assert {:ok, %Mongo.SaveOneResult{matched_count: 0, modified_count: 0, upserted_id: %BSON.ObjectId{}}} =
           Mongo.save_one(Pool, coll, %{foo: 42})
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.SaveOneResult{matched_count: 0, modified_count: 0, upserted_id: %BSON.ObjectId{}}} =
           Mongo.save_one(Pool, coll, %{foo: 42})
    assert [_, _] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert {:ok, %Mongo.SaveOneResult{matched_count: 0, modified_count: 1, upserted_id: %BSON.ObjectId{}}} =
           Mongo.save_one(Pool, coll, %{_id: id, foo: 43})
    assert [_] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list

    assert {:ok, %Mongo.SaveOneResult{matched_count: 1, modified_count: 1, upserted_id: nil}} =
           Mongo.save_one(Pool, coll, %{_id: id, foo: 44})
    assert [] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
  end

  test "save_one!" do
    coll = unique_name

    assert %Mongo.SaveOneResult{matched_count: 0, modified_count: 0, upserted_id: %BSON.ObjectId{} = id} =
      Mongo.save_one!(Pool, coll, %{foo: 42})

    assert nil == Mongo.save_one!(Pool, coll, %{_id: id, foo: 43}, w: 0)
  end

  test "save_many! ordered single" do
    coll = unique_name
    id = Mongo.IdServer.new

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 0, upserted_ids: %{0 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 42}])
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 0, upserted_ids: %{0 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 42}])
    assert [_, _] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 1, upserted_ids: %{0 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{_id: id, foo: 43}])
    assert [_] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 1, modified_count: 1, upserted_ids: %{}} =
      Mongo.save_many!(Pool, coll, [%{_id: id, foo: 44}])
    assert [] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
  end

  test "save_many! ordered multi" do
    coll = unique_name
    id1 = Mongo.IdServer.new
    id2 = Mongo.IdServer.new
    id3 = Mongo.IdServer.new
    id4 = Mongo.IdServer.new
    id5 = Mongo.IdServer.new

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 0,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 42}, %{foo: 43}])
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 2,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{_id: id1, foo: 44}, %{_id: id2, foo: 45}])
    assert [_] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 45}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 2, modified_count: 2, upserted_ids: %{}} =
      Mongo.save_many!(Pool, coll, [%{_id: id1, foo: 46}, %{_id: id1, foo: 46}])
    assert [] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 46}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 1,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}, 2 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 47}, %{_id: id3, foo: 48}, %{foo: 49}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 47}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 48}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 49}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 2,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}, 2 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{_id: id4, foo: 50}, %{foo: 51}, %{_id: id5, foo: 52}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 50}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 51}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 52}) |> Enum.to_list
  end

  test "save_many! unordered single" do
    coll = unique_name
    id = Mongo.IdServer.new

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 0, upserted_ids: %{0 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 42}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 0, upserted_ids: %{0 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 42}], ordered: false)
    assert [_, _] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 1, upserted_ids: %{0 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{_id: id, foo: 43}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 1, modified_count: 1, upserted_ids: %{}} =
      Mongo.save_many!(Pool, coll, [%{_id: id, foo: 44}], ordered: false)
    assert [] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
  end

  test "save_many! unordered multi" do
    coll = unique_name
    id1 = Mongo.IdServer.new
    id2 = Mongo.IdServer.new
    id3 = Mongo.IdServer.new
    id4 = Mongo.IdServer.new
    id5 = Mongo.IdServer.new

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 0,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 42}, %{foo: 43}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 42}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 43}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 2,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{_id: id1, foo: 44}, %{_id: id2, foo: 45}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 45}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 2, modified_count: 2, upserted_ids: %{}} =
      Mongo.save_many!(Pool, coll, [%{_id: id1, foo: 46}, %{_id: id1, foo: 46}], ordered: false)
    assert [] = Mongo.find(Pool, coll, %{foo: 44}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 46}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 1,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}, 2 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{foo: 47}, %{_id: id3, foo: 48}, %{foo: 49}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 47}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 48}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 49}) |> Enum.to_list

    assert %Mongo.SaveManyResult{matched_count: 0, modified_count: 2,
                                 upserted_ids: %{0 => %BSON.ObjectId{}, 1 => %BSON.ObjectId{}, 2 => %BSON.ObjectId{}}} =
      Mongo.save_many!(Pool, coll, [%{_id: id4, foo: 50}, %{foo: 51}, %{_id: id5, foo: 52}], ordered: false)
    assert [_] = Mongo.find(Pool, coll, %{foo: 50}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 51}) |> Enum.to_list
    assert [_] = Mongo.find(Pool, coll, %{foo: 52}) |> Enum.to_list
  end

  test "logging" do
    coll = unique_name

    Mongo.find(LoggingPool, coll, %{}, log: false) |> Enum.to_list
    refute Process.get(:last_log)

    Mongo.find(LoggingPool, coll, %{}) |> Enum.to_list
    assert Process.get(:last_log) == {:find, [coll, %{}, nil, [batch_size: 1000]]}
  end

  # issue #19
  test "correctly pass options to cursor" do
    assert %Mongo.Cursor{coll: "coll", opts: [no_cursor_timeout: true, skip: 10]} =
           Mongo.find(Pool, "coll", %{}, skip: 10, cursor_timeout: false)
  end
end
