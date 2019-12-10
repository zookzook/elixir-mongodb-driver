defmodule Mongo.PasswordSafeTest do
  @moduledoc false

  use ExUnit.Case, async: false
  alias Mongo.UrlParser
  alias Mongo.PasswordSafe

  #
  # When the sasl logger is activated like  `--logger-sasl-reports true` then the supervisor reports all parameters when it starts a process. So, the password should not
  # used in the options
  #
  describe "parse_url and hide the password in options" do
    test "encoded password" do
      url = "mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/admin"
      opts = UrlParser.parse_url([url: url])

      assert "*****" == Keyword.get(opts, :password)
      assert "D1fficultP@ssw0rd" == PasswordSafe.get_pasword()
    end
  end

end
