ExUnit.start()

{_, 0} = System.cmd("mongo", ~w'mongodb_test --eval db.dropDatabase()')
System.cmd("mongo", ~w'mongodb_test --eval db.createUser({user:"test_user",pwd:"test_user",roles:[]})')
