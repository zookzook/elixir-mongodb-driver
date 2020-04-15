# Do not run the SSL tests on Travis

{string, 0} = System.cmd("mongod", ~w'--version')
["db version v" <> version, _] = String.split(string, "\n", parts: 2)

IO.puts "[mongod v#{version}]"

version =
  version
  |> String.split(".")
  |> Enum.map(&elem(Integer.parse(&1), 0))
  |> List.to_tuple

options = [ssl: true, socket: true]
options = if System.get_env("CI") do [tag_sets: true] ++ options else options end
options = if version < {3, 4, 0} do [mongo_3_4: true] ++ options else options end
options = if version < {3, 6, 0} do [mongo_3_6: true] ++ options else options end
options = if version < {4, 2, 0} do [mongo_4_2: true] ++ options else options end
options = if version < {4, 3, 0} do [mongo_4_3: true] ++ options else options end

ExUnit.configure exclude: options
ExUnit.start()

defmodule MongoTest.Case do
  use ExUnit.CaseTemplate

  using do
    quote do
      import MongoTest.Case
    end
  end

  defmacro unique_collection do
    {function, _arity} = __CALLER__.function
    "#{__CALLER__.module}.#{function}"
    |> String.replace(" ", "_")
    |> String.replace(".", "_")
    |> String.downcase()
  end
end
