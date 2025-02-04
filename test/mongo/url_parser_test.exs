defmodule Mongo.UrlParserTest do
  @moduledoc false

  use ExUnit.Case, async: false
  alias Mongo.UrlParser

  describe ".parse_url" do
    test "basic url" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017") == [seeds: ["localhost:27017"]]
    end

    test "basic url and trailing slash" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017/") == [seeds: ["localhost:27017"]]
    end

    test "basic url and trailing slash and options" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017/?replicaSet=set-name&authSource=admin&maxPoolSize=5") == [
               pool_size: 5,
               auth_source: "admin",
               set_name: "set-name",
               seeds: ["localhost:27017"]
             ]
    end

    test "basic url, trailing slash and options" do
      assert UrlParser.parse_url(url: "mongodb://localhost:27017/") == [seeds: ["localhost:27017"]]
    end

    test "Missing delimiting slash between hosts and options" do
      assert UrlParser.parse_url(url: "mongodb://example.com?w=1") == [url: "mongodb://example.com?w=1"]
    end

    test "Incomplete key value pair for option" do
      assert UrlParser.parse_url(url: "mongodb://example.com/test?w") == [url: "mongodb://example.com/test?w"]
    end

    test "User info for single IPv4 host without database" do
      assert UrlParser.parse_url(url: "mongodb://alice:foo@127.0.0.1") |> Keyword.drop([:pw_safe]) == [password: "*****", username: "alice", seeds: ["127.0.0.1"]]
    end

    test "User info for single IPv4 host with database" do
      assert UrlParser.parse_url(url: "mongodb://alice:foo@127.0.0.1/test") |> Keyword.drop([:pw_safe]) == [
               password: "*****",
               username: "alice",
               database: "test",
               seeds: ["127.0.0.1"]
             ]
    end

    test "User info for single hostname without database" do
      assert UrlParser.parse_url(url: "mongodb://eve:baz@example.com") |> Keyword.drop([:pw_safe]) == [
               password: "*****",
               username: "eve",
               seeds: ["example.com"]
             ]
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

    test "write read preferences" do
      assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?readPreference=secondary&readPreferenceTags=dc:ny,rack:r&maxStalenessSeconds=30") == [
               database: "db_name",
               read_preference: %{mode: :secondary, tags: [dc: "ny", rack: "r"], max_staleness_ms: 30_000},
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017"
               ]
             ]

      assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?readPreference=secondary&readPreferenceTags=dc::ny,rack:r&maxStalenessSeconds=30") == [
               database: "db_name",
               read_preference: %{mode: :secondary, tags: [rack: "r"], max_staleness_ms: 30_000},
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017"
               ]
             ]

      assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?readPreference=secondary&maxStalenessSeconds=30") == [
               database: "db_name",
               read_preference: %{mode: :secondary, max_staleness_ms: 30_000},
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017"
               ]
             ]

      assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?readPreference=weird&readPreferenceTags=dc:ny,rack:r&maxStalenessSeconds=30") == [
               database: "db_name",
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017"
               ]
             ]

      assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017,seed2.domain.com:27017/db_name?readPreference=primaryPreferred&maxStalenessSeconds=30") == [
               database: "db_name",
               read_preference: %{mode: :primary_preferred, max_staleness_ms: 30_000},
               seeds: [
                 "seed1.domain.com:27017",
                 "seed2.domain.com:27017"
               ]
             ]
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

    test "url with directConnection" do
      for direct_connection <- ["true", "false"] do
        assert UrlParser.parse_url(url: "mongodb://seed1.domain.com:27017/db_name?directConnection=#{direct_connection}") == [
                 database: "db_name",
                 direct_connection: String.to_atom(direct_connection),
                 seeds: [
                   "seed1.domain.com:27017"
                 ]
               ]
      end
    end
  end
end
