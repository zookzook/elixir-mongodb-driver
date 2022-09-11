defmodule Mongo.StableVersion do
  @moduledoc false

  defmodule ServerAPI do
    @moduledoc false

    defstruct version: "1",
              strict: false,
              deprecation_errors: false
  end

  def merge_stable_api(command, %{version: version, strict: strict, deprecation_errors: deprecation_errors}) do
    command
    |> Keyword.put(:apiVersion, version)
    |> Keyword.put(:apiStrict, strict)
    |> Keyword.put(:apiDeprecationErrors, deprecation_errors)
  end

  def merge_stable_api(command, _other) do
    command
  end
end
