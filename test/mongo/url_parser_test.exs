defmodule Mongo.UrlParserTest do
  @moduledoc false

  use ExUnit.Case, async: false
  alias Mongo.UrlParser

  describe ".parse_url" do
    test "basic url" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017") == [seeds: ["localhost:27017"]]
    end

    test "percent encoded password" do
      url =
        "mongodb://myusername:2yPK%7DBzj%7CqE%28%24%5E1JDdk4J42%2A%264lLgV%25C@mymongodbserver:27017/admin"

      opts = UrlParser.parse_url(url: url)
      password = Keyword.get(opts, :password)
      assert password == "2yPK}Bzj|qE($^1JDdk4J42*&4lLgV%C"
    end

    test "cluster url" do
      url =
        "mongodb://user:password@seed1.domain.com:27017,seed2.domain.com:27017,seed3.domain.com:27017/db_name?ssl=true&replicaSet=set-name&authSource=admin&maxPoolSize=5"

      assert UrlParser.parse_url(url: url) == [
               username: "user",
               password: "password",
               database: "db_name",
               pool_size: 5,
               auth_source: "admin",
               set_name: "set-name",
               ssl: true,
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017",
                 "seed3.domain.com:27017"
               ]
             ]
    end

    test "merge options" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017", name: :test, seeds: ["1234"]) ==
               [seeds: ["localhost:27017"], name: :test]
    end

    test "url srv" do
      assert UrlParser.parse_url(url: "mongodb+srv://test5.test.build.10gen.cc") ==
               [
                 ssl: true,
                 auth_source: "thisDB",
                 set_name: "repl0",
                 seeds: [
                   "localhost.test.build.10gen.cc:27017"
                 ]
               ]
    end

    test "url srv with user" do
      assert UrlParser.parse_url(url: "mongodb+srv://user:password@test5.test.build.10gen.cc") ==
               [
                 username: "user",
                 password: "password",
                 ssl: true,
                 auth_source: "thisDB",
                 set_name: "repl0",
                 seeds: [
                   "localhost.test.build.10gen.cc:27017"
                 ]
               ]
    end

    test "write concern" do
      for w <- [2, "majority"] do
        assert UrlParser.parse_url(
                 url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?w=#{w}"
               ) == [
                 database: "db_name",
                 w: w,
                 seeds: [
                   "seed1.domain.com:27017",
                   "seed2.domain.com:27017"
                 ]
               ]
      end
    end

    test "appName option" do
      url = "mongodb://USERNAME:PASSWORD@HOST:PORT/DATABASE?ssl=true&replicaSet=globaldb&appName=APP"
      assert UrlParser.parse_url(url: url) == [
        username: "USERNAME",
        password: "PASSWORD",
        database: "DATABASE",
        app_name: "APP",
        set_name: "globaldb",
        ssl: true,
        seeds: ["HOST:PORT"]
      ]
    end
  end
end
