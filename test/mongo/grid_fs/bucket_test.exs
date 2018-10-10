defmodule Mongo.GridFs.BucketTest do
  use ExUnit.Case

  alias Mongo.GridFs.Bucket

  setup_all do
    #assert {:ok, pid} = Mongo.TestConnection.connect
    #{:ok, [pid: pid]}
    {:ok, pid} = Mongo.start_link(url: "mongodb://localhost:27017/grid-test")
    {:ok, [pid: pid]}
  end

  test "checks implementation of the protocols Inspect and String.Chars", c do
    b = Bucket.new( c.pid )
    inspect b
    assert inspect b == to_string(b)
  end

  test "checks if the name can be overridden", c do
    new_name = "my_fs"
    %Bucket{ name: fs } = Bucket.new( c.pid, name: "my_fs" )
    assert fs == new_name
  end

  test "checks if the chunk_size can be overridden", c do
    new_chunk_size = 30*1024
    %Bucket{ chunk_size: chunk_size } = Bucket.new( c.pid, chunk_size: new_chunk_size )
    assert chunk_size == new_chunk_size
  end

end
