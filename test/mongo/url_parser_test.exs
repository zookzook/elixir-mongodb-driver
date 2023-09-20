defmodule Mongo.UrlParserTest do
  @moduledoc false

  use ExUnit.Case, async: false
  alias Mongo.UrlParser

  describe ".parse_url" do
    test "basic url" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017") == [seeds: ["localhost:27017"]]
    end

    test "cluster url with ssl" do
      url = "mongodb://user:password@seed1.domain.com:27017,seed2.domain.com:27017,seed3.domain.com:27017/db_name?ssl=true&replicaSet=set-name&authSource=admin&maxPoolSize=5"

      assert UrlParser.parse_url(url: url) |> Keyword.drop([:pw_safe]) == [
               password: "*****",
               username: "user",
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

    test "cluster url with tls" do
      url = "mongodb://user:password@seed1.domain.com:27017,seed2.domain.com:27017,seed3.domain.com:27017/db_name?tls=true&replicaSet=set-name&authSource=admin&maxPoolSize=5"

      assert UrlParser.parse_url(url: url) |> Keyword.drop([:pw_safe]) == [
               password: "*****",
               username: "user",
               database: "db_name",
               pool_size: 5,
               auth_source: "admin",
               set_name: "set-name",
               tls: true,
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

    test "url srv with auth source" do
      assert UrlParser.parse_url(url: "mongodb+srv://test10.test.build.10gen.cc/db?replicaSet=r1&authSource=admin") ==
               [
                 database: "db",
                 ssl: true,
                 socket_timeout_ms: 500,
                 auth_source: "admin",
                 set_name: "r1",
                 seeds: [
                   "localhost.test.build.10gen.cc:27017"
                 ]
               ]
    end

    test "url srv with user" do
      assert UrlParser.parse_url(url: "mongodb+srv://user:password@test5.test.build.10gen.cc") |> Keyword.drop([:pw_safe]) ==
               [
                 password: "*****",
                 username: "user",
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
        assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?w=#{w}") == [
                 database: "db_name",
                 w: w,
                 seeds: [
                   "seed1.domain.com:27017",
                   "seed2.domain.com:27017"
                 ]
               ]
      end
    end

    test "encoded user" do
      real_username = "@:/skøl:@/"
      real_password = "@æœ{}%e()}@"

      encoded_username = URI.encode_www_form(real_username)
      encoded_password = URI.encode_www_form(real_password)
      url = "mongodb://#{encoded_username}:#{encoded_password}@mymongodbserver:27017/admin"
      opts = UrlParser.parse_url(url: url)
      username = Keyword.get(opts, :username)
      assert username == real_username
    end

    test "external auth source " do
      encoded_external_auth_source = URI.encode_www_form("$external")
      url = "mongodb://user:password@seed1.domain.com:27017,seed2.domain.com:27017,seed3.domain.com:27017/db_name?replicaSet=set-name&authMechanism=PLAIN&authSource=#{encoded_external_auth_source}&tls=true"

      assert UrlParser.parse_url(url: url) |> Keyword.drop([:pw_safe]) == [
               password: "*****",
               username: "user",
               database: "db_name",
               tls: true,
               auth_source: "$external",
               auth_mechanism: :plain,
               set_name: "set-name",
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017",
                 "seed3.domain.com:27017"
               ]
             ]
    end
  end
end
