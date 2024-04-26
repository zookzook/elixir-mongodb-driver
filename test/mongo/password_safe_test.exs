defmodule Mongo.PasswordSafeTest do
  @moduledoc false

  use ExUnit.Case, async: false
  alias Mongo.UrlParser
  alias Mongo.PasswordSafe

  test "encrypted password" do
    pw = "my-secret-password"
    {:ok, pid} = PasswordSafe.start_link()
    PasswordSafe.set_password(pid, pw)
    %{key: _key, pw: enc_pw} = :sys.get_state(pid)
    assert enc_pw != pw
    assert pw == PasswordSafe.get_password(pid)
  end

  #
  # When the sasl logger is activated like  `--logger-sasl-reports true` then the supervisor reports all parameters when it starts a process. So, the password should not
  # used in the options
  #
  test "encoded password" do
    url = "mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/admin"
    opts = UrlParser.parse_url(url: url)
    assert "*****" == Keyword.get(opts, :password)
    assert "D1fficultP@ssw0rd" == PasswordSafe.get_password(Keyword.get(opts, :pw_safe))
  end
end
