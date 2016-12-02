ExUnit.start()

defmodule MongoTest.Case do
  use ExUnit.CaseTemplate

  using do
    quote do
      import MongoTest.Case
    end
  end

  def capture_log(fun) do
    Logger.remove_backend(:console)
    fun.()
    Logger.add_backend(:console, flush: true)
  end

  defmacro unique_name do
    {function, _arity} = __CALLER__.function
    "#{__CALLER__.module}.#{function}"
  end
end
