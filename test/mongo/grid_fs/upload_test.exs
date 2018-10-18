defmodule Mongo.GridFs.UploadTest do
  use ExUnit.Case

  alias Mongo.GridFs.Bucket
  alias Mongo.GridFs.Upload

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  def calc_checksum(path) do
    File.stream!(path, [], 2048)
    |> Enum.reduce(:crypto.hash_init(:sha256), fn(line, acc) -> :crypto.hash_update(acc, line) end)
    |> :crypto.hash_final
    |> Base.encode16
  end

  test "upload a jpeg file, check download, length and checksum", c do
    b = Bucket.new(c.pid)
    upload_stream = Upload.open_upload_stream(b, "test.jpg", nil, j: true, w: :majority)

    src_filename = "./test/data/test.jpg"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    dest_filename = "/tmp/my-test-file.jps"

    with {:ok, stream} <- Mongo.GridFs.Download.open_download_stream(b, file_id) do
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

  test "upload a text file, check download, length and checksum", c do

    b = Bucket.new(c.pid)
    upload_stream = Upload.open_upload_stream(b, "my-example-file.txt", meta: %{tag: "checked"}, j: true, w: :majority)

    src_filename = "./test/data/test.txt"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    dest_filename = "/tmp/my-test-file.txt"

    with {:ok, stream} <- Mongo.GridFs.Download.open_download_stream(b, file_id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

    %{size: dest_size} = File.stat!(dest_filename)
    %{size: src_size} = File.stat!(src_filename)
    assert dest_size == src_size

    assert calc_checksum(dest_filename) == calc_checksum(src_filename)
  end

  test "upload a text file, check download, length, meta-data and checksum", c do

    src_filename  = "./test/data/test.txt"
    bucket        = Bucket.new(c.pid)
    chksum        = calc_checksum(src_filename)
    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt", %{tag: "checked", chk_sum: chksum}, j: true, w: :majority)

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    %{"metadata" => %{"tag" => "checked", "chk_sum" => x}} = Mongo.find_one(c.pid, Bucket.files_collection_name(bucket), %{_id: file_id})
    assert x == chksum
  end

end
