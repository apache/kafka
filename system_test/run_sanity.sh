#!/bin/bash

my_ts=`date +"%s"`

cp testcase_to_run.json testcase_to_run.json_${my_ts}
cp testcase_to_run_sanity.json testcase_to_run.json

python -B system_test_runner.py


