#!/usr/bin/env bash

find tmp -name mongod.lock -exec cat '{}' + | xargs kill -9 && rm -rf tmp
