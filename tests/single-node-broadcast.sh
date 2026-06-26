#!/usr/bin/env bash

set -xe

./maelstrom/maelstrom test -w broadcast --bin ./bin/broadcast --node-count 1 --time-limit 20 --rate 10
