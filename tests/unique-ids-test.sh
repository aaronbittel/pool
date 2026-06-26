#!/usr/bin/env bash

set -xe

./maelstrom/maelstrom test -w unique-ids --bin ./bin/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
