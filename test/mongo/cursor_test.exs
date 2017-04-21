defmodule Mongo.CursorTest do
  use ExUnit.Case

  defmacro unique_name do
    {function, _arity} = __CALLER__.function
    "#{__CALLER__.module}.#{function}"
  end

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  # issue #94
  test "correctly pass options to kill_cursors", c do
    coll = unique_name()

    docs = Stream.cycle([%{foo: 42}]) |> Enum.take(100)

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, docs)
    assert [%{"foo" => 42}, %{"foo" => 42}] = Mongo.find(c.pid, coll, %{}, limit: 2) |> Enum.to_list
  end
end
