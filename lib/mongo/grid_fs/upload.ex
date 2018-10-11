defmodule Mongo.GridFs.Upload do
  @moduledoc false

  alias Mongo.GridFs.UploadStream

  def open_upload_stream(bucket, opts \\ []) do
    UploadStream.new(bucket)
  end
end
