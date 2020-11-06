defmodule Mongo.EncoderTest do
  use MongoTest.Case, async: false
  alias Mongo

  defp connect_auth do
    assert {:ok, pid} =
             Mongo.start_link(
               hostname: "localhost",
               database: "mongodb_test",
               username: "mongodb_user",
               password: "mongodb_user"
             )

    pid
  end

  defmodule CustomStructWithoutProtocol do
    @fields [:a, :b, :c, :id]
    @enforce_keys @fields
    defstruct @fields
  end

  defmodule CustomStruct do
    @fields [:a, :b, :c, :id]
    @enforce_keys @fields
    defstruct @fields

    defimpl Mongo.Encoder do
      def encode(%{a: a, b: b, id: id}) do
        %{
          _id: id,
          a: a,
          b: b,
          custom_encoded: true
        }
      end
    end
  end

  test "insert encoded struct with protocol" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :write)

    struct_to_insert = %CustomStruct{a: 10, b: 20, c: 30, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} = Mongo.insert_one(pid, coll, struct_to_insert, [])

    assert {:ok,
            %{
              cursor_id: 0,
              from: 0,
              num: 1,
              docs: [
                %{
                  "a" => 10,
                  "b" => 20,
                  "custom_encoded" => true,
                  "_id" => "5ef27e73d2a57d358f812001"
                }
              ]
            }} = Mongo.raw_find(conn, coll, %{}, nil, skip: 0)
  end

  test "insert encoded struct without protocol" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, _conn, _, _} = Mongo.select_server(pid, :write)

    struct_to_insert = %CustomStructWithoutProtocol{a: 10, b: 20, c: 30, id: "x"}

    assert_raise Protocol.UndefinedError, fn ->
      Mongo.insert_one(pid, coll, struct_to_insert, [])
    end
  end

  defimpl Mongo.Encoder, for: Function do
    def encode(_), do: %{fun: true, _id: "5ef27e73d2a57d358f812002"}
  end

  test "insert encoded function to db" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :write)

    fun_to_insert = & &1

    assert {:ok, _} = Mongo.insert_one(pid, coll, fun_to_insert, [])

    assert {:ok,
            %{
              cursor_id: 0,
              from: 0,
              num: 1,
              docs: [%{"fun" => true, "_id" => "5ef27e73d2a57d358f812002"}]
            }} = Mongo.raw_find(conn, coll, %{}, nil, skip: 0)
  end

  test "update with encoded struct in db with protocol" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :write)

    struct_to_insert = %CustomStruct{a: 10, b: 20, c: 30, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} = Mongo.insert_one(pid, coll, struct_to_insert, [])

    struct_to_change = %CustomStruct{a: 100, b: 200, c: 300, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} =
             Mongo.update_one(pid, coll, %{_id: "5ef27e73d2a57d358f812001"}, %{
               "$set": struct_to_change
             })

    assert {:ok,
            %{
              cursor_id: 0,
              from: 0,
              num: 1,
              docs: [
                %{
                  "a" => 100,
                  "b" => 200,
                  "custom_encoded" => true,
                  "_id" => "5ef27e73d2a57d358f812001"
                }
              ]
            }} = Mongo.raw_find(conn, coll, %{}, nil, skip: 0)
  end

  test "upsert with encoded struct in db with protocol" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :write)

    struct_to_change = %CustomStruct{a: 100, b: 200, c: 300, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} =
             Mongo.update_one(
               pid,
               coll,
               %{_id: "5ef27e73d2a57d358f812001"},
               %{"$set": struct_to_change},
               upsert: true
             )

    assert {:ok,
            %{
              cursor_id: 0,
              from: 0,
              num: 1,
              docs: [
                %{
                  "a" => 100,
                  "b" => 200,
                  "custom_encoded" => true,
                  "_id" => "5ef27e73d2a57d358f812001"
                }
              ]
            }} = Mongo.raw_find(conn, coll, %{}, nil, skip: 0)
  end
end
