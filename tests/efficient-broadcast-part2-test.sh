#!/usr/bin/env bash

set -xe

./maelstrom/maelstrom test -w broadcast --bin ./bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
