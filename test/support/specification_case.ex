defmodule Mongo.SpecificationCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      @crud_tests Path.wildcard("test/support/crud_tests/**/*.json")

      import MongoTest.Case
      import Mongo.SpecificationCase
    end
  end
end
