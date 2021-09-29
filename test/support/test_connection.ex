defmodule Mongo.TestConnection do
  @seeds ["127.0.0.1:27001", "127.0.0.1:27002", "127.0.0.1:27003"]

  import ExUnit.Callbacks, only: [start_supervised: 1]

  def connect() do
    with {_, 0} <- System.cmd("bash", ["./start_mongo.bash"]) do
      start_supervised({Mongo, database: "mongodb_test", seeds: @seeds})
    else
      {error, exit_code} ->
        {:error, {exit_code, error}}
    end
  end
end
