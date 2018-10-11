defmodule Mongo.GridFs.UploadTest do
  use ExUnit.Case

  alias Mongo.GridFs.Bucket
  alias Mongo.GridFs.Upload

  setup_all do
    #assert {:ok, pid} = Mongo.TestConnection.connect
    #{:ok, [pid: pid]}
    {:ok, pid} = Mongo.start_link(url: "mongodb://localhost:27017/grid-test")
    {:ok, [pid: pid]}
  end

  def calc_checksum(path) do
    File.stream!(path,[],2048)
    |> Enum.reduce(:crypto.hash_init(:sha256),fn(line, acc) -> :crypto.hash_update(acc,line) end )
    |> :crypto.hash_final
    |> Base.encode16
  end

  test "uploads a jpeg file, checks download, length and checksum", c do
    b = Bucket.new( c.pid )
    upload_stream = Upload.open_upload_stream( b )

    src_filename = "./test/data/test.jpg"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    dest_filename = "/tmp/my-test-file.jps"

    with {:ok, stream} <- b |> Mongo.GridFs.Download.open_download_stream(file_id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

    %{size: dest_size} = File.stat!(dest_filename)
    %{size: src_size}  = File.stat!(src_filename)
    assert dest_size == src_size

    assert calc_checksum(dest_filename) == calc_checksum(src_filename)
  end

  test "uploads a text file, checks download, length and checksum", c do
    b = Bucket.new( c.pid )
    upload_stream = Upload.open_upload_stream( b )

    src_filename = "./test/data/test.txt"
    File.stream!(src_filename, [], 512) |> Stream.into( upload_stream ) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    dest_filename = "/tmp/my-test-file.txt"

    with {:ok, stream} <- b |> Mongo.GridFs.Download.open_download_stream(file_id) do
      stream
      |> Stream.into( File.stream!(dest_filename) )
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

    %{size: dest_size} = File.stat!(dest_filename)
    %{size: src_size} = File.stat!(src_filename)
    assert dest_size == src_size

    assert calc_checksum(dest_filename) == calc_checksum(src_filename)
  end
end
