ExUnit.start()

{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.dropDatabase()')
{_, 0} = System.cmd("mongo", ~w'mongodb_test2 --eval db.dropDatabase()')

# 2.4
{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.addUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})')
{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.addUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})')

# >2.6
# {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.createUser({user:"mongodb_user",pwd:"mongodb_user",roles:[]})')
# {_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.createUser({user:"mongodb_user2",pwd:"mongodb_user2",roles:[]})')
