# Do not run the SSL tests on Travis
if System.get_env("CI") do
  ExUnit.configure exclude: [ssl: true]
end

ExUnit.start()

{string, 0} = System.cmd("mongod", ~w'--version')
["db version v" <> version, _] = String.split(string, "\n", parts: 2)

IO.puts "[mongod v#{version}]"

{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.dropDatabase() --port 27001')
{_, 0} = System.cmd("mongo", ~w'mongodb_test2 --eval db.dropDatabase() --port 27001')
