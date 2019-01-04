#!/usr/bin/env bash

TASK_ID="produce_bench_$RANDOM"
TASK_SPEC=`cat $1`;
TASK_SPEC=${TASK_SPEC/TASK_ID/$TASK_ID};
./bin/trogdor.sh local --spec "${TASK_SPEC}"