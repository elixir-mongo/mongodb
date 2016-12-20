# Do not run the SSL tests on Travis
if System.get_env("CI") do
  ExUnit.configure exclude: [ssl: true]
end

ExUnit.start()

{string, 0} = System.cmd("mongod", ~w'--version')
["db version v" <> version, _] = String.split(string, "\n", parts: 2)

IO.puts "[mongod v#{version}]"

version =
  version
  |> String.split(".")
  |> Enum.map(&elem(Integer.parse(&1), 0))
  |> List.to_tuple

{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.dropDatabase()')
{_, 0} = System.cmd("mongo", ~w'mongodb_test2 --eval db.dropDatabase()')
{_, 0} = System.cmd("mongo", ~w'admin_test --eval db.dropDatabase()')

if version < {2, 6, 0} do
  {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.addUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})')
  {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.addUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})')
  {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.addUser({user:"mongodb_admin_user",pwd:"mongodb_admin_user",roles:[]})')
else
  {_, _} = System.cmd("mongo", ~w'mongodb_test --eval db.dropUser("mongodb_user")')
  {_, _} = System.cmd("mongo", ~w'mongodb_test --eval db.dropUser("mongodb_user2")')
  {_, _} = System.cmd("mongo", ~w'admin_test --eval db.dropUser("mongodb_admin_user")')
  {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.createUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})')
  {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.createUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})')
  {_, 0} = System.cmd("mongo", ~w'admin_test --eval db.createUser({user:"mongodb_admin_user",pwd:"mongodb_admin_user",roles:[{role:"readWrite",db:"mongodb_test"},{role:"read",db:"mongodb_test2"}]})')
end

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
