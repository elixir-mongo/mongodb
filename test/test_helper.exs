{string, 0} = System.cmd("mongod", ~w'--version')
["db version v" <> version, _] = String.split(string, "\n", parts: 2)
mongodb_uri = System.get_env("MONGODB_URI")

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

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'mongodb_test\'); db.dropDatabase()"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'mongodb_test2\'); db.dropDatabase()"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'admin_test\'); db.dropDatabase()"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'admin_test\'); db.dropUser("mongodb_user")"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'admin_test\'); db.dropUser("mongodb_user2")"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'admin_test\'); db.dropUser("mongodb_admin_user")"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'mongodb_test\'); db.createUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'mongodb_test\'); db.createUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})"'
  )

{_, 0} =
  System.cmd(
    "mongo",
    ~w'#{mongodb_uri} --eval "db=db.getSiblingDB(\'admin_test\'); db.createUser({user:"mongodb_admin_user",pwd:"mongodb_admin_user",roles:[{role:"readWrite",db:"mongodb_test"},{role:"read",db:"mongodb_test2"}]})"'
  )

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

  defmacro mongodb_uri do
    quote do
      System.get_env("MONGODB_URI")
    end
  end
end
