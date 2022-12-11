# Do not run the SSL tests on Travis
ExUnit.configure(exclude: [ssl: true, socket: true])
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
