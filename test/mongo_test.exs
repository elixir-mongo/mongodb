defmodule Mongo.Test do
  use ExUnit.Case

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()

    {:ok, [pid: pid]}
  end

  defmacro unique_name do
    {function, _arity} = __CALLER__.function
    "#{__CALLER__.module}.#{function}.#{System.unique_integer([:positive])}"
  end

  test "object_id" do
    assert %BSON.ObjectId{value: <<_::96>>} = Mongo.object_id()
  end

  test "command", c do
    assert {:ok, %{"ok" => 1.0}} = Mongo.command(c.pid, %{ping: true})
    assert {:error, %Mongo.Error{}} = Mongo.command(c.pid, %{drop: "unexisting-database"})
  end

  test "command!", c do
    assert %{"ok" => 1.0} = Mongo.command!(c.pid, %{ping: true})

    assert_raise Mongo.Error, fn ->
      Mongo.command!(c.pid, %{drop: "unexisting-database"})
    end
  end

  test "show_collections", c do
    coll_1 = unique_name() <> "_1"
    coll_2 = unique_name() <> "_2"

    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 1})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 2})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_2, %{foo: 3})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_2, %{foo: 4})

    assert {:ok, _} =
             Mongo.create_indexes(c.pid, coll_1, [[key: [foo: 1], name: "not-a-collection"]])

    assert {:ok, _} =
             Mongo.create_indexes(c.pid, coll_2, [
               [key: [foo: 1, bar: 1], name: "not-a-collection"]
             ])

    colls =
      c.pid
      |> Mongo.show_collections()
      |> Enum.to_list()

    assert Enum.member?(colls, coll_1)
    assert Enum.member?(colls, coll_2)
    assert not Enum.member?(colls, "not-a-collection")
  end

  test "create_indexes", c do
    coll_1 = unique_name()

    valid_index_spec = [[key: [foo: 1], name: "foo"]]

    # Create the index
    assert {:ok,
            %Mongo.CreateIndexesResult{
              commit_quorum: "votingMembers",
              created_collection_automatically: true,
              num_indexes_after: 2,
              num_indexes_before: 1
            }} = Mongo.create_indexes(c.pid, coll_1, valid_index_spec)

    # Repeated calls to create the index should be ok / idemptodent
    assert {:ok,
            %Mongo.CreateIndexesResult{
              commit_quorum: nil,
              created_collection_automatically: false,
              num_indexes_after: 2,
              num_indexes_before: 2
            }} = Mongo.create_indexes(c.pid, coll_1, valid_index_spec)

    # Subsequent conflicting indexes should result in an error
    conflicting_index_spec = [[key: [foo_bar: 1], name: "foo"]]

    assert {:error, %Mongo.Error{code: 86}} =
             Mongo.create_indexes(c.pid, coll_1, conflicting_index_spec)

    # Bad index specification should result in an error
    bad_index_spec = [[key: [foo_bar: 1]]]
    assert {:error, %Mongo.Error{code: 9}} = Mongo.create_indexes(c.pid, coll_1, bad_index_spec)
  end

  test "list_indexes", c do
    coll_1 = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 1})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 2})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 3})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 4})

    assert {:ok, _} = Mongo.create_indexes(c.pid, coll_1, [[key: [foo: 1], name: "foo"]])

    assert {:ok, _} =
             Mongo.create_indexes(c.pid, coll_1, [[key: [foo: 1, bar: 1], name: "foo-bar"]])

    indexes =
      c.pid
      |> Mongo.list_index_names(coll_1)
      |> Enum.to_list()

    assert Enum.count(indexes) == 3
    assert Enum.member?(indexes, "_id_")
    assert Enum.member?(indexes, "foo")
    assert Enum.member?(indexes, "foo-bar")
  end

  test "drop_index", c do
    coll_1 = unique_name()

    name = "foo"
    spec = [[key: [foo: 1], name: name]]

    # Create the index
    assert {:ok, %Mongo.CreateIndexesResult{num_indexes_before: 1, num_indexes_after: 2}} =
             Mongo.create_indexes(c.pid, coll_1, spec)

    indexes =
      c.pid
      |> Mongo.list_index_names(coll_1)
      |> Enum.to_list()

    assert Enum.member?(indexes, name)

    assert {:ok, %Mongo.DropIndexResult{num_indexes_was: 2}} =
             Mongo.drop_index(c.pid, coll_1, name)

    indexes =
      c.pid
      |> Mongo.list_index_names(coll_1)
      |> Enum.to_list()

    refute Enum.member?(indexes, name)

    assert {:error, %Mongo.Error{}} = Mongo.drop_index(c.pid, coll_1, name)

    # Drop index with multiple indexes
    spec = [[key: [foo: 1], name: "foo"], [key: [foo_bar: 1], name: "foo_bar"]]

    assert {:ok, %Mongo.CreateIndexesResult{num_indexes_before: 1, num_indexes_after: 3}} =
             Mongo.create_indexes(c.pid, coll_1, spec)

    # Drop all with wildcard
    assert {:ok, %Mongo.DropIndexResult{num_indexes_was: 3}} =
             Mongo.drop_index(c.pid, coll_1, "*")

    assert {:ok, %Mongo.CreateIndexesResult{num_indexes_before: 1, num_indexes_after: 3}} =
             Mongo.create_indexes(c.pid, coll_1, spec)

    # Drop all with list of indexes
    assert {:ok, %Mongo.DropIndexResult{num_indexes_was: 3}} =
             Mongo.drop_index(c.pid, coll_1, ["foo", "foo_bar"])
  end

  test "aggregate", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 45})

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}, %{"foo" => 45}] =
             c.pid |> Mongo.aggregate(coll, []) |> Enum.to_list()

    query = [
      %{
        "$match" => %{
          "foo" => %{"$gt" => 43}
        }
      },
      %{
        "$group" => %{
          "_id" => "foo",
          "total" => %{"$sum" => "$foo"}
        }
      }
    ]

    assert [%{"_id" => "foo", "total" => 89}] =
             c.pid |> Mongo.aggregate(coll, query) |> Enum.to_list()

    assert [] = c.pid |> Mongo.aggregate(coll, []) |> Enum.take(0)
    assert [] = c.pid |> Mongo.aggregate(coll, []) |> Enum.drop(4)
    assert [%{"foo" => 42}] = c.pid |> Mongo.aggregate(coll, []) |> Enum.take(1)
    assert [%{"foo" => 45}] = c.pid |> Mongo.aggregate(coll, []) |> Enum.drop(3)

    assert [] = Mongo.aggregate(c.pid, coll, [], use_cursor: false) |> Enum.take(0)
    assert [] = Mongo.aggregate(c.pid, coll, [], use_cursor: false) |> Enum.drop(4)

    assert [%{"foo" => 42}] =
             c.pid |> Mongo.aggregate(coll, [], use_cursor: false) |> Enum.take(1)

    assert [%{"foo" => 45}] =
             c.pid |> Mongo.aggregate(coll, [], use_cursor: false) |> Enum.drop(3)

    assert [] = Mongo.aggregate(c.pid, coll, [], batch_size: 1) |> Enum.take(0)
    assert [] = Mongo.aggregate(c.pid, coll, [], batch_size: 1) |> Enum.drop(4)
    assert [%{"foo" => 42}] = c.pid |> Mongo.aggregate(coll, [], batch_size: 1) |> Enum.take(1)
    assert [%{"foo" => 45}] = c.pid |> Mongo.aggregate(coll, [], batch_size: 1) |> Enum.drop(3)
  end

  test "count", c do
    coll = unique_name()

    assert {:ok, 0} = Mongo.count(c.pid, coll, [])

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})

    assert {:ok, 2} = Mongo.count(c.pid, coll, %{})
    assert {:ok, 1} = Mongo.count(c.pid, coll, %{foo: 42})
  end

  test "count!", c do
    coll = unique_name()

    assert 0 = Mongo.count!(c.pid, coll, %{foo: 43})
  end

  test "distinct", c do
    coll = unique_name()

    assert {:ok, []} = Mongo.distinct(c.pid, coll, "foo", %{})

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})

    assert {:ok, [42, 43]} = Mongo.distinct(c.pid, coll, "foo", %{})
    assert {:ok, [42]} = Mongo.distinct(c.pid, coll, "foo", %{foo: 42})
  end

  test "distinct!", c do
    coll = unique_name()

    assert [] = Mongo.distinct!(c.pid, coll, "foo", %{})
  end

  test "find", c do
    coll = unique_name()

    assert [] = c.pid |> Mongo.find(coll, %{}) |> Enum.to_list()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43, bar: 2})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44, bar: 3})

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}] =
             c.pid |> Mongo.find(coll, %{}) |> Enum.to_list()

    # Mongo is weird with batch_size=1
    assert [%{"foo" => 42}] = c.pid |> Mongo.find(coll, %{}, batch_size: 1) |> Enum.to_list()

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}] =
             c.pid |> Mongo.find(coll, %{}, batch_size: 2) |> Enum.to_list()

    assert [%{"foo" => 42}, %{"foo" => 43}] =
             c.pid |> Mongo.find(coll, %{}, limit: 2) |> Enum.to_list()

    assert [%{"foo" => 42}, %{"foo" => 43}] =
             c.pid |> Mongo.find(coll, %{}, batch_size: 2, limit: 2) |> Enum.to_list()

    assert [%{"foo" => 42}] = c.pid |> Mongo.find(coll, %{bar: 1}) |> Enum.to_list()

    assert [%{"bar" => 1}, %{"bar" => 2}, %{"bar" => 3}] =
             c.pid |> Mongo.find(coll, %{}, projection: %{bar: 1}) |> Enum.to_list()

    assert [%{"bar" => 1}] =
             c.pid
             |> Mongo.find(coll, %{"$query": %{foo: 42}}, projection: %{bar: 1})
             |> Enum.to_list()

    assert [%{"foo" => 44}, %{"foo" => 43}] =
             c.pid
             |> Mongo.find(coll, %{}, sort: [foo: -1], batch_size: 2, limit: 2)
             |> Enum.to_list()
  end

  test "find_one", c do
    coll = unique_name()

    assert [] = c.pid |> Mongo.find(coll, %{}) |> Enum.to_list()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    assert nil == Mongo.find_one(c.pid, coll, %{foo: 43})
    assert %{"foo" => 42} = Mongo.find_one(c.pid, coll, %{})

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})

    assert %{"foo" => 42} = Mongo.find_one(c.pid, coll, %{})
    # should return the first one so the next test fails
    assert %{"foo" => 43} != Mongo.find_one(c.pid, coll, %{})
  end

  test "find_one_and_update", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    # defaults
    assert {:ok, value} =
             Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 2}})

    assert %{"bar" => 1} = value, "Should return original document by default"

    # should raise if we don't have atomic operators
    assert_raise ArgumentError, fn ->
      Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{bar: 3})
    end

    # return_document = :after
    assert {:ok, value} =
             Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 3}},
               return_document: :after
             )

    assert %{"bar" => 3} = value, "Should return modified doc"

    # projection
    assert {:ok, value} =
             Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 3}},
               projection: %{"bar" => 1}
             )

    assert Map.get(value, "foo") == nil, "Should respect the projection"

    # sort
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 10})

    assert {:ok, value} =
             Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{baz: 1}},
               sort: %{"bar" => -1},
               return_document: :after
             )

    assert %{"bar" => 10, "baz" => 1} = value, "Should respect the sort"

    # upsert
    assert {:ok, value} =
             Mongo.find_one_and_update(c.pid, coll, %{"foo" => 43}, %{"$set" => %{baz: 1}},
               upsert: true,
               return_document: :after
             )

    assert %{"foo" => 43, "baz" => 1} = value, "Should upsert"

    # don't find return {:ok, nil}
    assert {:ok, nil} ==
             Mongo.find_one_and_update(c.pid, coll, %{"number" => 666}, %{
               "$set" => %{title: "the number of the beast"}
             })

    assert {:ok, nil} ==
             Mongo.find_one_and_update(c.pid, "coll_that_doesnt_exist", %{"number" => 666}, %{
               "$set" => %{title: "the number of the beast"}
             })

    # wrong parameter
    assert {:error, %Mongo.Error{}} =
             Mongo.find_one_and_update(c.pid, 2, %{"number" => 666}, %{
               "$set" => %{title: "the number of the beast"}
             })
  end

  test "find_one_and_replace", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    assert_raise ArgumentError, fn ->
      Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 3}})
    end

    # defaults
    assert {:ok, value} = Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 42}, %{bar: 2})
    assert %{"foo" => 42, "bar" => 1} = value, "Should return original document by default"

    # return_document = :after
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43, bar: 1})

    assert {:ok, value} =
             Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 43}, %{bar: 3},
               return_document: :after
             )

    assert %{"bar" => 3} = value, "Should return modified doc"
    assert match?(%{"foo" => 43}, value) == false, "Should replace document"

    # projection
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44, bar: 1})

    assert {:ok, value} =
             Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 44}, %{foo: 44, bar: 3},
               return_document: :after,
               projection: %{bar: 1}
             )

    assert Map.get(value, "foo") == nil, "Should respect the projection"

    # sort
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 1, note: "keep"})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 2, note: "replace"})

    assert {:ok, _} =
             Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 50}, %{foo: 50, bar: 3},
               sort: %{bar: -1}
             )

    assert [doc] = c.pid |> Mongo.find(coll, %{note: "keep"}) |> Enum.to_list()

    assert %{"bar" => 1, "note" => "keep"} = doc,
           "Replaced the correct document according to the sort"

    # upsert
    assert [] = c.pid |> Mongo.find(coll, %{upsertedDocument: true}) |> Enum.to_list()

    assert {:ok, value} =
             Mongo.find_one_and_replace(
               c.pid,
               coll,
               %{"upsertedDocument" => true},
               %{"upsertedDocument" => true},
               upsert: true,
               return_document: :after
             )

    assert %{"upsertedDocument" => true} = value, "Should upsert"

    assert [%{"upsertedDocument" => true}] =
             c.pid |> Mongo.find(coll, %{upsertedDocument: true}) |> Enum.to_list()
  end

  test "find_one_and_delete", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    # default
    assert {:ok, %{"foo" => 42, "bar" => 1}} = Mongo.find_one_and_delete(c.pid, coll, %{foo: 42})
    assert [] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    # projection
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    assert {:ok, value} =
             Mongo.find_one_and_delete(c.pid, coll, %{foo: 42}, projection: %{bar: 1})

    assert Map.get(value, "foo") == nil, "Should respect the projection"

    # sort
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 1, note: "keep"})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 2, note: "delete"})

    assert {:ok, %{"note" => "delete"}} =
             Mongo.find_one_and_delete(c.pid, coll, %{foo: 50}, sort: %{bar: -1})

    assert [%{"note" => "keep"}] = c.pid |> Mongo.find(coll, %{note: "keep"}) |> Enum.to_list()
  end

  test "insert_one", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.insert_one(c.pid, coll, [%{foo: 42, bar: 1}])
    end

    assert {:ok, result} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert %Mongo.InsertOneResult{acknowledged: true, inserted_id: id} = result

    assert [%{"_id" => ^id, "foo" => 42}] =
             c.pid |> Mongo.find(coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.InsertOneResult{acknowledged: false, inserted_id: nil}} =
             Mongo.insert_one(c.pid, coll, %{}, w: 0)
  end

  test "insert_one!", c do
    coll = unique_name()

    assert %Mongo.InsertOneResult{} = Mongo.insert_one!(c.pid, coll, %{"_id" => 1})

    assert %Mongo.InsertOneResult{acknowledged: false, inserted_id: nil} ==
             Mongo.insert_one!(c.pid, coll, %{}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.insert_one!(c.pid, coll, %{_id: 1})
    end
  end

  test "insert_many", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.insert_many(c.pid, coll, %{foo: 42, bar: 1})
    end

    assert {:ok, result} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 43}])
    assert %Mongo.InsertManyResult{inserted_ids: [id0, id1]} = result

    assert [%{"_id" => ^id0, "foo" => 42}] =
             c.pid |> Mongo.find(coll, %{_id: id0}) |> Enum.to_list()

    assert [%{"_id" => ^id1, "foo" => 43}] =
             c.pid |> Mongo.find(coll, %{_id: id1}) |> Enum.to_list()

    assert {:ok, %Mongo.InsertManyResult{acknowledged: false, inserted_ids: nil}} =
             Mongo.insert_many(c.pid, coll, [%{}], w: 0)
  end

  test "insert_many!", c do
    coll = unique_name()

    docs = [%{foo: 42}, %{foo: 43}]
    assert %Mongo.InsertManyResult{} = Mongo.insert_many!(c.pid, coll, docs)

    assert %Mongo.InsertManyResult{acknowledged: false, inserted_ids: nil} ==
             Mongo.insert_many!(c.pid, coll, [%{}], w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.insert_many!(c.pid, coll, [%{_id: 1}, %{_id: 1}])
    end
  end

  test "delete_one", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} =
             Mongo.delete_one(c.pid, coll, %{foo: 42})

    assert [%{"foo" => 42}] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} =
             Mongo.delete_one(c.pid, coll, %{foo: 42})

    assert [] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.DeleteResult{deleted_count: 0}} =
             Mongo.delete_one(c.pid, coll, %{foo: 42})

    assert [%{"foo" => 43}] = c.pid |> Mongo.find(coll, %{foo: 43}) |> Enum.to_list()
  end

  test "delete_one!", c do
    coll = unique_name()

    assert %Mongo.DeleteResult{deleted_count: 0} = Mongo.delete_one!(c.pid, coll, %{foo: 42})

    assert %Mongo.DeleteResult{acknowledged: false} == Mongo.delete_one!(c.pid, coll, %{}, w: 0)
  end

  test "delete_many", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.DeleteResult{deleted_count: 2}} =
             Mongo.delete_many(c.pid, coll, %{foo: 42})

    assert [] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.DeleteResult{deleted_count: 0}} =
             Mongo.delete_one(c.pid, coll, %{foo: 42})

    assert [%{"foo" => 43}] = c.pid |> Mongo.find(coll, %{foo: 43}) |> Enum.to_list()
  end

  test "delete_many!", c do
    coll = unique_name()

    assert %Mongo.DeleteResult{deleted_count: 0} = Mongo.delete_many!(c.pid, coll, %{foo: 42})

    assert %Mongo.DeleteResult{acknowledged: false} == Mongo.delete_many!(c.pid, coll, %{}, w: 0)
  end

  test "replace_one", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.replace_one(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})
    end

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_ids: nil}} =
             Mongo.replace_one(c.pid, coll, %{foo: 42}, %{foo: 0})

    assert [_] = c.pid |> Mongo.find(coll, %{foo: 0}) |> Enum.to_list()
    assert [_] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 0, upserted_ids: [id]}} =
             Mongo.replace_one(c.pid, coll, %{foo: 50}, %{foo: 0}, upsert: true)

    assert [_] = c.pid |> Mongo.find(coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_ids: nil}} =
             Mongo.replace_one(c.pid, coll, %{foo: 43}, %{foo: 1}, upsert: true)

    assert [] = c.pid |> Mongo.find(coll, %{foo: 43}) |> Enum.to_list()
    assert [_] = c.pid |> Mongo.find(coll, %{foo: 1}) |> Enum.to_list()
  end

  test "replace_one!", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{matched_count: 0, modified_count: 0, upserted_ids: nil} =
             Mongo.replace_one!(c.pid, coll, %{foo: 43}, %{foo: 0})

    assert %Mongo.UpdateResult{acknowledged: false} ==
             Mongo.replace_one!(c.pid, coll, %{foo: 45}, %{foo: 0}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.replace_one!(c.pid, coll, %{foo: 42}, %{_id: 1})
    end
  end

  test "update_one", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.update_one(c.pid, coll, %{foo: 42}, %{foo: 0})
    end

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_ids: nil}} =
             Mongo.update_one(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert [_] = c.pid |> Mongo.find(coll, %{foo: 0}) |> Enum.to_list()
    assert [_] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 0, upserted_ids: [id]}} =
             Mongo.update_one(c.pid, coll, %{foo: 50}, %{"$set": %{foo: 0}}, upsert: true)

    assert [_] = c.pid |> Mongo.find(coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_ids: nil}} =
             Mongo.update_one(c.pid, coll, %{foo: 43}, %{"$set": %{foo: 1}}, upsert: true)

    assert [] = c.pid |> Mongo.find(coll, %{foo: 43}) |> Enum.to_list()
    assert [_] = c.pid |> Mongo.find(coll, %{foo: 1}) |> Enum.to_list()
  end

  test "update_one!", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_ids: nil} =
             Mongo.update_one!(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert %Mongo.UpdateResult{acknowledged: false} ==
             Mongo.update_one!(c.pid, coll, %{foo: 42}, %{}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.update_one!(c.pid, coll, %{foo: 0}, %{"$set": %{_id: 0}})
    end
  end

  test "update_many", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.update_many(c.pid, coll, %{foo: 42}, %{foo: 0})
    end

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{matched_count: 2, modified_count: 2, upserted_ids: nil}} =
             Mongo.update_many(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert [_, _] = c.pid |> Mongo.find(coll, %{foo: 0}) |> Enum.to_list()
    assert [] = c.pid |> Mongo.find(coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 0, upserted_ids: [id]}} =
             Mongo.update_many(c.pid, coll, %{foo: 50}, %{"$set": %{foo: 0}}, upsert: true)

    assert [_] = c.pid |> Mongo.find(coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{matched_count: 1, modified_count: 1, upserted_ids: nil}} =
             Mongo.update_many(c.pid, coll, %{foo: 43}, %{"$set": %{foo: 1}}, upsert: true)

    assert [] = c.pid |> Mongo.find(coll, %{foo: 43}) |> Enum.to_list()
    assert [_] = c.pid |> Mongo.find(coll, %{foo: 1}) |> Enum.to_list()
  end

  test "update_many!", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{matched_count: 2, modified_count: 2, upserted_ids: nil} =
             Mongo.update_many!(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert %Mongo.UpdateResult{acknowledged: false} ==
             Mongo.update_many!(c.pid, coll, %{foo: 0}, %{}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.update_many!(c.pid, coll, %{foo: 0}, %{"$set": %{_id: 1}})
    end
  end

  # issue #19
  test "correctly pass options to cursor", c do
    assert %Mongo.Cursor{opts: [slave_ok: true, no_cursor_timeout: true, skip: 10], coll: "coll"} =
             Mongo.find(c.pid, "coll", %{}, skip: 10, cursor_timeout: false)
  end

  # issue #220
  @tag :mongo_3_4
  test "correctly query NumberDecimal", c do
    coll = "number_decimal_test"

    Mongo.command(
      c.pid,
      %{
        eval: "db.#{coll}.insert({number: NumberDecimal('123.456')})"
      }
    )

    assert %{"number" => %Decimal{coef: 123_456, exp: -3}} =
             Mongo.find(c.pid, coll, %{}, limit: 1) |> Enum.to_list() |> List.first()
  end

  test "access multiple databases", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42}, database: "mongodb_test2")

    assert {:ok, 1} = Mongo.count(c.pid, coll, [], database: "mongodb_test2")
    assert {:ok, 0} = Mongo.count(c.pid, coll, [])
  end
end
