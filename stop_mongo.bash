#!/usr/bin/env bash

for i in $(seq 1 3); do
  pid="$(cat tmp/db$i/mongod.lock 2>/dev/null)"

  if [ -n "$pid" ]; then
    kill -9 $pid
  fi
done

rm -rf tmp
