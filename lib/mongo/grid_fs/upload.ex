defmodule Mongo.GridFs.Upload do
  @moduledoc """
  The main entry point for uploading files into the grid-fs specified by the bucket struct.
  """

  alias Mongo.GridFs.UploadStream

  @doc """
  Opens a stream that the application can write the contents of the file to.
  The driver generates the file id.

  User data for the 'metadata' field of the files collection document.
  """
  @spec open_upload_stream(Mongo.GridFs.Bucket.t, String.t, BSON.document | nil, Keyword.t) :: UploadStream.t
  def open_upload_stream(bucket, filename, meta \\ nil, opts \\ []) do
    UploadStream.new(bucket, filename, meta, opts)
  end

end
