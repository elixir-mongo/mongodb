defmodule Mongo.UrlParserTest do
  @moduledoc false

  use ExUnit.Case, async: false
  alias Mongo.UrlParser

  describe ".parse_url" do
    test "basic url" do
      assert UrlParser.parse_url([url: "mongodb://localhost:27017"]) == [seeds: ["localhost:27017"]]
    end
    test "cluster url" do
      url = "mongodb://user:password@seed1.domain.com:27017,seed2.domain.com:27017,seed3.domain.com:27017/db_name?ssl=true&replicaSet=set-name&authSource=admin"
      assert UrlParser.parse_url([url: url]) == [
        username: "user",
        password: "password",
        database: "db_name",
        auth_source: "admin",
        set_name: "set-name",
        ssl: true,
        seeds: ["seed1.domain.com:27017", "seed2.domain.com:27017", "seed3.domain.com:27017"]
      ]
    end
    test "merge options" do
      assert UrlParser.parse_url([url: "mongodb://localhost:27017", name: :test, seeds: ["1234"]]) == [seeds: ["localhost:27017"], name: :test]
    end
  end
end
