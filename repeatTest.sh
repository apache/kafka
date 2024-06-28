for i in $(seq 1 2);
do
    TC_PATHS="tests/kafkatest/tests/streams/streams_standby_replica_test.py" bash tests/docker/run_tests.sh
done
