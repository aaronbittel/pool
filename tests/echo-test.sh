#!/usr/bin/env bash

set -xe

./maelstrom/maelstrom test -w echo --bin ./bin/echo --time-limit 5
