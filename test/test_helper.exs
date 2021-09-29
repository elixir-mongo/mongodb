{string, 0} = System.cmd("mongod", ~w'--version')
["db version v" <> version, _] = String.split(string, "\n", parts: 2)

IO.puts("[mongod v#{version}]")

version = Version.parse!(version)

excluded = []

# Do not run the SSL tests on Travis
excluded =
  if System.get_env("CI") do
    [ssl: true, socket: true] ++ excluded
  else
    excluded
  end

excluded =
  if Version.match?(version, "< 3.4.0") do
    [mongo_3_4: true] ++ excluded
  else
    excluded
  end

excluded =
  if Version.match?(version, "< 3.6.0") do
    [session: true] ++ excluded
  else
    excluded
  end

ExUnit.configure(exclude: excluded)
ExUnit.start()

{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.dropDatabase() --port 27001')
{_, 0} = System.cmd("mongo", ~w'mongodb_test2 --eval db.dropDatabase() --port 27001')

{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.dropDatabase()')
{_, 0} = System.cmd("mongo", ~w'mongodb_test2 --eval db.dropDatabase()')
{_, 0} = System.cmd("mongo", ~w'admin_test --eval db.dropDatabase()')

if Version.match?(version, "< 2.6.0") do
  {_, 0} =
    System.cmd(
      "mongo",
      ~w'mongodb_test --eval db.addUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})'
    )

  {_, 0} =
    System.cmd(
      "mongo",
      ~w'mongodb_test --eval db.addUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})'
    )

  {_, 0} =
    System.cmd(
      "mongo",
      ~w'mongodb_test --eval db.addUser({user:"mongodb_admin_user",pwd:"mongodb_admin_user",roles:[]})'
    )
else
  {_, _} = System.cmd("mongo", ~w'mongodb_test --eval db.dropUser("mongodb_user")')
  {_, _} = System.cmd("mongo", ~w'mongodb_test --eval db.dropUser("mongodb_user2")')
  {_, _} = System.cmd("mongo", ~w'admin_test --eval db.dropUser("mongodb_admin_user")')

  {_, 0} =
    System.cmd(
      "mongo",
      ~w'mongodb_test --eval db.createUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})'
    )

  {_, 0} =
    System.cmd(
      "mongo",
      ~w'mongodb_test --eval db.createUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})'
    )

  {_, 0} =
    System.cmd(
      "mongo",
      ~w'admin_test --eval db.createUser({user:"mongodb_admin_user",pwd:"mongodb_admin_user",roles:[{role:"readWrite",db:"mongodb_test"},{role:"read",db:"mongodb_test2"}]})'
    )
end

defmodule MongoTest.Case do
  use ExUnit.CaseTemplate

  using do
    quote do
      import MongoTest.Case
    end
  end

  defmacro unique_name do
    {function, _arity} = __CALLER__.function
    "#{__CALLER__.module}.#{function}"
  end
end
