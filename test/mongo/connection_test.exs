defmodule Mongo.ConnectionTest do
  use MongoTest.Case, async: false
  import ExUnit.CaptureLog
  alias Mongo

  defp connect do
    assert {:ok, pid} = Mongo.start_link(hostname: "localhost", database: "mongodb_test")
    pid
  end

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

  defp connect_auth_invalid do
    assert {:ok, pid} =
             Mongo.start_link(
               hostname: "localhost",
               database: "mongodb_test",
               username: "mongodb_user",
               password: "wrong_password"
             )

    pid
  end

  defp connect_auth_on_db do
    assert {:ok, pid} =
             Mongo.start_link(
               hostname: "localhost",
               database: "mongodb_test",
               username: "mongodb_admin_user",
               password: "mongodb_admin_user",
               auth_source: "admin_test"
             )

    pid
  end

  defp connect_ssl do
    assert {:ok, pid} =
             Mongo.start_link(hostname: "localhost", database: "mongodb_test", ssl: true)

    pid
  end

  defp connect_socket_dir do
    assert {:ok, pid} = Mongo.start_link(socket_dir: "/tmp", database: "mongodb_test")

    pid
  end

  defp connect_socket do
    assert {:ok, pid} =
             Mongo.start_link(socket: "/tmp/mongodb-27017.sock", database: "mongodb_test")

    pid
  end

  defp tcp_count do
    Enum.count(:erlang.ports(), fn port ->
      case :erlang.port_info(port, :name) do
        {:name, 'tcp_inet'} -> true
        _ -> false
      end
    end)
  end

  test "connect and ping" do
    pid = connect()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
             Mongo.raw_find(conn, "$cmd", %{ping: 1}, %{}, batch_size: 1)
  end

  @tag :ssl
  test "ssl" do
    pid = connect_ssl()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
             Mongo.raw_find(conn, "$cmd", %{ping: 1}, %{}, batch_size: 1)
  end

  test "auth" do
    pid = connect_auth()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
             Mongo.raw_find(conn, "$cmd", %{ping: 1}, %{}, batch_size: 1)
  end

  test "auth on db" do
    pid = connect_auth_on_db()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
             Mongo.raw_find(conn, "$cmd", %{ping: 1}, %{}, batch_size: 1)
  end

  test "auth wrong" do
    Process.flag(:trap_exit, true)

    opts = [
      hostname: "localhost",
      database: "mongodb_test",
      username: "mongodb_user",
      password: "wrong",
      backoff_type: :stop
    ]

    assert capture_log(fn ->
             assert {:ok, pid} = Mongo.start_link(opts)
             assert_receive {:EXIT, ^pid, :killed}, 5000
           end) =~ "(Mongo.Error) auth failed for user mongodb_user"
  end

  test "auth wrong on db" do
    Process.flag(:trap_exit, true)

    opts = [
      hostname: "localhost",
      database: "mongodb_test",
      username: "mongodb_admin_user",
      password: "wrong",
      backoff_type: :stop,
      auth_source: "admin_test"
    ]

    assert capture_log(fn ->
             assert {:ok, pid} = Mongo.start_link(opts)
             assert_receive {:EXIT, ^pid, :killed}, 5000
           end) =~ "(Mongo.Error) auth failed for user mongodb_admin_user"
  end

  test "insert_one flags" do
    pid = connect_auth()
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, continue_on_error: true)
  end

  test "find" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])

    assert {:ok, %{cursor_id: 0, from: 0, num: 2, docs: [%{"foo" => 42}, %{"foo" => 43}]}} =
             Mongo.raw_find(conn, coll, %{}, nil, [])

    assert {:ok, %{cursor_id: 0, from: 0, num: 1, docs: [%{"foo" => 43}]}} =
             Mongo.raw_find(conn, coll, %{}, nil, skip: 1)
  end

  test "find and get_more" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 44}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 45}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 46}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 47}, [])

    assert {:ok, %{cursor_id: cursor_id, from: 0, docs: [%{"foo" => 42}, %{"foo" => 43}]}} =
             Mongo.raw_find(conn, coll, %{}, nil, batch_size: 2)

    assert {:ok,
            %{"cursor" => %{"id" => ^cursor_id, "nextBatch" => [%{"foo" => 44}, %{"foo" => 45}]}}} =
             Mongo.get_more(conn, coll, cursor_id, batch_size: 2)

    assert {:ok,
            %{"cursor" => %{"id" => ^cursor_id, "nextBatch" => [%{"foo" => 46}, %{"foo" => 47}]}}} =
             Mongo.get_more(conn, coll, cursor_id, batch_size: 2)

    assert {:ok, %{"cursor" => %{"id" => 0, "nextBatch" => []}}} =
             Mongo.get_more(conn, coll, cursor_id, batch_size: 2)
  end

  test "kill_cursors" do
    pid = connect_auth()
    coll = unique_name()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 42}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 43}, [])
    assert {:ok, _} = Mongo.insert_one(pid, coll, %{foo: 44}, [])

    assert {:ok, %{cursor_id: cursor_id, num: 2}} =
             Mongo.raw_find(conn, coll, %{}, nil, batch_size: 2)

    assert :ok = Mongo.kill_cursors(conn, [cursor_id], [])

    assert {:error, %Mongo.Error{code: 43, host: nil, message: message}} =
             Mongo.get_more(conn, coll, cursor_id, [])

    assert Regex.match?(~r/#{cursor_id}/, message)
  end

  test "big response" do
    pid = connect_auth()
    coll = unique_name()
    size = 1024 * 1024
    binary = <<0::size(size)>>
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    Enum.each(1..10, fn _ ->
      Mongo.insert_one(pid, coll, %{data: binary}, w: 0)
    end)

    assert {:ok, %{num: 10}} = Mongo.raw_find(conn, coll, %{}, nil, batch_size: 100)
  end

  test "auth connection leak" do
    capture_log(fn ->
      # sometimes the function tcp_count() returns 1, so the test fails.
      # maybe it is a good idea to wait a second before counting
      # :timer.sleep(1000)
      assert tcp_count() == 0

      Enum.each(1..10, fn _ ->
        connect_auth_invalid()
      end)

      :timer.sleep(1000)
      # there should be 10 connections with connection_type: :monitor
      assert tcp_count() == 10
    end)
  end

  @tag :socket
  test "connect socket_dir" do
    pid = connect_socket_dir()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
             Mongo.raw_find(conn, "$cmd", %{ping: 1}, %{}, batch_size: 1)
  end

  @tag :socket
  test "connect socket" do
    pid = connect_socket()
    {:ok, conn, _, _} = Mongo.select_server(pid, :read)

    assert {:ok, %{docs: [%{"ok" => 1.0}]}} =
             Mongo.raw_find(conn, "$cmd", %{ping: 1}, %{}, batch_size: 1)
  end
end
