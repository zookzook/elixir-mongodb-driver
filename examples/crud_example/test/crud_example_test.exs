defmodule CrudExampleTest do
  use ExUnit.Case
  doctest CrudExample

  test "greets the world" do
    assert CrudExample.hello() == :world
  end
end
