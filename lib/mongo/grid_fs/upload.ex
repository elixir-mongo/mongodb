defmodule Mongo.GridFs.Upload do
  @moduledoc false

  alias Mongo.GridFs.UploadStream

  def open_upload_stream(bucket, filename, opts \\ []) do
    UploadStream.new(bucket, filename, opts)
  end
end
