#!/bin/bash

# stop any existing mongod servers and delete data directories

. ./stop_mongo.bash

# start the mongod servers of the replica set

for i in $(seq 1 3); do
  mkdir -p tmp/db$i
  mongod --fork --dbpath tmp/db$i --logpath tmp/db$i/log --port 2700$i --bind_ip 127.0.0.1 \
    --replSet mongodb_test &>/dev/null

  if [ $? -ne 0 ]; then
    . ./stop_mongo.bash
    echo "failed to start mongo servers... try killing any zombies and restarting"
    exit 1
  fi
done

sleep 1
# initiate the replica set

host1='"127.0.0.1:27001"'
host2='"127.0.0.1:27002"'
host3='"127.0.0.1:27003"'
members="[{_id: 0, host: $host1}, {_id: 1, host: $host2}, {_id: 2, host: $host3}]"
mongo --quiet --port 27001 --eval \
  "JSON.stringify(rs.initiate({_id: \"mongodb_test\", members: $members}))" >/dev/null
if [ $? -ne 0 ]; then
  echo "failed to configure replica set"
  exit 1
fi

repl_set_url='mongodb_test/127.0.0.1:27001,127.0.0.1:27002,127.0.0.1:27003'

# wait for replica set election
mongo --port 27001 --quiet --eval 'typeof db.createUser === "function"' >tmp/over24
if [ $? -ne 0 ]; then
  echo "failed while waiting for replica set election"
  exit 1
else
  if [ "$(tail -n1 tmp/over24)" == "true" ]; then
    createUser="createUser"
  else
    createUser="addUser"
    sleep 60
  fi

  mongo mongodb_test --host $repl_set_url --quiet --eval \
    "db.${createUser}({user:'mongodb_user',pwd:'mongodb_user',roles:[]})" &>/dev/null && \
  mongo mongodb_test --host $repl_set_url --quiet --eval \
    "db.${createUser}({user:'mongodb_user2',pwd:'mongodb_user2',roles:[]})" &>/dev/null && \
  mongo admin_test --host $repl_set_url --quiet --eval \
    "db.${createUser}({user:'mongodb_admin_user',pwd:'mongodb_admin_user',roles:[ \
      {role:'readWrite',db:'mongodb_test'},{role:'read',db:'mongodb_test2'}]})" &>/dev/null
fi
