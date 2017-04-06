# Do not run the SSL tests on Travis
if System.get_env("CI") do
  ExUnit.configure exclude: [ssl: true]
end

ExUnit.start()
