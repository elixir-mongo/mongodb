defmodule Mongo.CursorTest do
  use MongoTest.Case, async: true

  @name __MODULE__

  setup_all do
    assert {:ok, pid} = Mongo.start_link(name: @name, database: "mongodb_test", pool: DBConnection.Poolboy)
    {:ok, [pid: pid]}
  end

  # issue #94
  test "correctly pass options to kill_cursors" do
    coll = unique_name()

    docs = Stream.cycle([%{foo: 42}]) |> Enum.take(100)

    assert {:ok, _} = Mongo.insert_many(@name, coll, docs, pool: DBConnection.Poolboy)
    assert [%{"foo" => 42}, %{"foo" => 42}] = Mongo.find(@name, coll, %{}, limit: 2, pool: DBConnection.Poolboy) |> Enum.to_list
  end
end
