defmodule Mongo.MongoDBConnection.UtilsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Mongo.MongoDBConnection.Utils

  @tls13 :"tlsv1.3"
  @tls13_supported @tls13 in :ssl.versions()[:supported]
  @tls13_tag if @tls13_supported,
               do: [requires_tls13: true],
               else: [skip: "OTP build does not advertise TLS 1.3; cannot exercise fallback path"]

  describe "maybe_restrict_versions/1" do
    test "returns opts unchanged when no :ciphers are given" do
      opts = [verify: :verify_peer]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end

    test "returns opts unchanged when :ciphers is nil" do
      opts = [ciphers: nil, verify: :verify_peer]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end

    test "returns opts unchanged when :versions is already set" do
      opts = [ciphers: [~c"AES256-SHA"], versions: [:"tlsv1.2"]]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end

    @tag @tls13_tag
    test "restricts versions to [:\"tlsv1.2\"] for a TLS-1.2-only cipher (charlist)" do
      opts = [ciphers: [~c"AES256-SHA"]]

      {result, log} = with_log(fn -> Utils.maybe_restrict_versions(opts) end)

      assert result[:versions] == [:"tlsv1.2"]
      assert log =~ "restricting TLS versions"
    end

    @tag @tls13_tag
    test "restricts versions for a TLS-1.2-only cipher given as atom" do
      opts = [ciphers: [:"AES256-SHA"]]

      {result, log} = with_log(fn -> Utils.maybe_restrict_versions(opts) end)

      assert result[:versions] == [:"tlsv1.2"]
      assert log =~ "restricting TLS versions"
    end

    test "returns opts unchanged when :ciphers is an empty list" do
      opts = [ciphers: [], verify: :verify_peer]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end

    @tag @tls13_tag
    test "leaves opts untouched when a TLS 1.3 cipher (map form) is present" do
      [tls13_suite | _] = :ssl.cipher_suites(:exclusive, @tls13)
      opts = [ciphers: [tls13_suite]]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end

    @tag @tls13_tag
    test "leaves opts untouched when a TLS 1.3 cipher is given as a binary IANA name" do
      [tls13_suite | _] = :ssl.cipher_suites(:exclusive, @tls13)
      iana_binary = tls13_suite |> :ssl.suite_to_str() |> to_string()
      opts = [ciphers: [iana_binary]]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end

    @tag @tls13_tag
    test "leaves opts untouched when ciphers mix TLS 1.2 and TLS 1.3 suites" do
      [tls13_suite | _] = :ssl.cipher_suites(:exclusive, @tls13)
      opts = [ciphers: [~c"AES256-SHA", tls13_suite]]

      log =
        capture_log(fn ->
          assert Utils.maybe_restrict_versions(opts) == opts
        end)

      refute log =~ "restricting"
    end
  end
end
