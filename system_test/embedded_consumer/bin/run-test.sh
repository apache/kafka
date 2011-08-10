#!/bin/bash

readonly num_messages=400000
readonly message_size=400
readonly action_on_fail="proceed"

readonly test_start_time="$(date +%s)"

readonly base_dir=$(dirname $0)/..

info() {
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") $*"
}

kill_child_processes() {
    isTopmost=$1
    curPid=$2
    childPids=$(ps a -o pid= -o ppid= | grep "${curPid}$" | awk '{print $1;}')
    for childPid in $childPids
    do
        kill_child_processes 0 $childPid
    done
    if [ $isTopmost -eq 0 ]; then
        kill -15 $curPid 2> /dev/null
    fi
}

cleanup() {
    info "cleaning up"

    pid_zk_source=
    pid_zk_target=
    pid_kafka_source1=
    pid_kafka_source2=
    pid_kafka_source3=
    pid_kafka_target1=
    pid_kafka_target2=
    pid_producer=

    rm -rf /tmp/zookeeper_source
    rm -rf /tmp/zookeeper_target

    rm -rf /tmp/kafka-source{1..3}-logs
    # mkdir -p /tmp/kafka-source{1..3}-logs/test0{1..3}-0
    # touch /tmp/kafka-source{1..3}-logs/test0{1..3}-0/00000000000000000000.kafka

    rm -rf /tmp/kafka-target{1..2}-logs
}

begin_timer() {
    t_begin=$(date +%s)
}

end_timer() {
    t_end=$(date +%s)
}

start_zk() {
    info "starting zookeepers"
    $base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_source.properties 2>&1 > $base_dir/zookeeper_source.log &
    pid_zk_source=$!
    $base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_target.properties 2>&1 > $base_dir/zookeeper_target.log &
    pid_zk_target=$!
}

start_source_servers() {
    info "starting source cluster"
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source1.properties 2>&1 > $base_dir/kafka_source1.log &
    pid_kafka_source1=$!
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source2.properties 2>&1 > $base_dir/kafka_source2.log &
    pid_kafka_source2=$!
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source3.properties 2>&1 > $base_dir/kafka_source3.log &
    pid_kafka_source3=$!
}

start_target_servers_for_whitelist_test() {
    echo "starting mirror cluster"
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target1.properties $base_dir/config/whitelisttest.consumer.properties $base_dir/config/mirror_producer.properties 2>&1 > $base_dir/kafka_target1.log &
    pid_kafka_target1=$!
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target2.properties $base_dir/config/whitelisttest.consumer.properties $base_dir/config/mirror_producer.properties 2>&1 > $base_dir/kafka_target2.log &
    pid_kafka_target2=$!
}

start_target_servers_for_blacklist_test() {
    echo "starting mirror cluster"
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target1.properties $base_dir/config/blacklisttest.consumer.properties $base_dir/config/mirror_producer.properties 2>&1 > $base_dir/kafka_target1.log &
    pid_kafka_target1=$!
    $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target2.properties $base_dir/config/blacklisttest.consumer.properties $base_dir/config/mirror_producer.properties 2>&1 > $base_dir/kafka_target2.log &
    pid_kafka_target2=$!
}

shutdown_servers() {
    info "stopping producer"
    if [ "x${pid_producer}" != "x" ]; then kill_child_processes 0 ${pid_producer}; fi

    info "shutting down target servers"
    if [ "x${pid_kafka_target1}" != "x" ]; then kill_child_processes 0 ${pid_kafka_target1}; fi
    if [ "x${pid_kafka_target2}" != "x" ]; then kill_child_processes 0 ${pid_kafka_target2}; fi
    sleep 2

    info "shutting down source servers"
    if [ "x${pid_kafka_source1}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source1}; fi
    if [ "x${pid_kafka_source2}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source2}; fi
    if [ "x${pid_kafka_source3}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source3}; fi

    info "shutting down zookeeper servers"
    if [ "x${pid_zk_target}" != "x" ]; then kill_child_processes 0 ${pid_zk_target}; fi
    if [ "x${pid_zk_source}" != "x" ]; then kill_child_processes 0 ${pid_zk_source}; fi
}

start_producer() {
    topic=$1
    info "start producing messages for topic $topic ..."
    $base_dir/../../bin/kafka-run-class.sh kafka.tools.ProducerPerformance --brokerinfo zk.connect=localhost:2181 --topic $topic --messages $num_messages --message-size $message_size --batch-size 200 --vary-message-size --threads 1 --reporting-interval $num_messages --async --delay-btw-batch-ms 10 2>&1 > $base_dir/producer_performance.log &
    pid_producer=$!
}

# In case the consumer does not consume, the test may exit prematurely (i.e.,
# shut down the kafka brokers, and ProducerPerformance will start throwing ugly
# exceptions. So, wait for the producer to finish before shutting down. If it
# takes too long, the user can just hit Ctrl-c which is trapped to kill child
# processes.
# Usage: wait_partition_done ([kafka-server] [topic] [partition-id])+
wait_partition_done() {
    n_tuples=$(($# / 3))

    i=1
    while (($#)); do
        kafka_server[i]=$1
        topic[i]=$2
        partitionid[i]=$3
        prev_offset[i]=0
        info "\twaiting for partition on server ${kafka_server[i]}, topic ${topic[i]}, partition ${partitionid[i]}"
        i=$((i+1))
        shift 3
    done

    all_done=0

    # set -x
    while [[ $all_done != 1 ]]; do
        sleep 4
        i=$n_tuples
        all_done=1
        for ((i=1; i <= $n_tuples; i++)); do
            cur_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server ${kafka_server[i]} --topic ${topic[i]} --partition ${partitionid[i]} --time -1 --offsets 1 | tail -1)
            if [ "x$cur_size" != "x${prev_offset[i]}" ]; then
                all_done=0
                prev_offset[i]=$cur_size
            fi
        done
    done

}

cmp_logs() {
    topic=$1
    info "comparing source and target logs for topic $topic"
    source_part0_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9092 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    source_part1_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9091 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    source_part2_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9090 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    target_part0_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    target_part1_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9094 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    if [ "x$target_part0_size" == "x" ]; then target_part0_size=0; fi
    if [ "x$target_part1_size" == "x" ]; then target_part1_size=0; fi
    expected_size=$(($source_part0_size + $source_part1_size + $source_part2_size))
    actual_size=$(($target_part0_size + $target_part1_size))
    if [ "x$expected_size" != "x$actual_size" ]
    then
        info "source size: $expected_size target size: $actual_size"
        return 1
    else
        return 0
    fi
}

take_fail_snapshot() {
    snapshot_dir="$base_dir/failed-${snapshot_prefix}-${test_start_time}"
    mkdir $snapshot_dir
    for dir in /tmp/zookeeper_source /tmp/zookeeper_target /tmp/kafka-source{1..3}-logs /tmp/kafka-target{1..2}-logs; do
        if [ -d $dir ]; then
            cp -r $dir $snapshot_dir
        fi
    done
}

# Usage: process_test_result <result> <action_on_fail>
# result: last test result
# action_on_fail: (exit|wait|proceed)
# ("wait" is useful if you want to troubleshoot using zookeeper)
process_test_result() {
    result=$1
    if [ $1 -eq 0 ]; then
        info "test passed"
    else
        info "test failed"
        case "$2" in
            "wait") info "waiting: hit Ctrl-c to quit"
                wait
                ;;
            "exit") shutdown_servers
                take_fail_snapshot
                exit $result
                ;;
            *) shutdown_servers
                take_fail_snapshot
                info "proceeding"
                ;;
        esac
    fi
}

test_whitelists() {
    info "### Testing whitelists"
    snapshot_prefix="whitelist-test"

    cleanup
    start_zk
    start_source_servers
    start_target_servers_for_whitelist_test
    sleep 4

    begin_timer

    start_producer test01
    info "waiting for producer to finish producing ..."
    wait_partition_done kafka://localhost:9090 test01 0 kafka://localhost:9091 test01 0 kafka://localhost:9092 test01 0

    info "waiting for consumer to finish consuming ..."
    wait_partition_done kafka://localhost:9093 test01 0 kafka://localhost:9094 test01 0

    end_timer
    info "embedded consumer took $((t_end - t_begin)) seconds"

    sleep 2

    cmp_logs test01
    result=$?

    return $result
}

test_blacklists() {
    info "### Testing blacklists"
    snapshot_prefix="blacklist-test"
    cleanup
    start_zk
    start_source_servers
    start_target_servers_for_blacklist_test
    sleep 4

    start_producer test02
    info "waiting for producer to finish producing test02 ..."
    wait_partition_done kafka://localhost:9090 test02 0 kafka://localhost:9091 test02 0 kafka://localhost:9092 test02 0

    # start_producer test03
    # info "waiting for producer to finish producing test03 ..."
    # wait_partition_done kafka://localhost:9090 test03 0 kafka://localhost:9091 test03 0 kafka://localhost:9092 test03 0

    begin_timer

    start_producer test01
    info "waiting for producer to finish producing ..."
    wait_partition_done kafka://localhost:9090 test01 0 kafka://localhost:9091 test01 0 kafka://localhost:9092 test01 0

    info "waiting for consumer to finish consuming ..."
    wait_partition_done kafka://localhost:9093 test01 0 kafka://localhost:9094 test01 0

    end_timer

    info "embedded consumer took $((t_end - t_begin)) seconds"

    sleep 2

    cmp_logs test02
    result1=$?
    # cmp_logs test03
    # result2=$?
    # if [[ "x$result1" == "x0" || "x$result2" == "x0" ]]; then
    if [[ "x$result1" == "x0" ]]; then
        result=1
    else
        cmp_logs test01
        result=$?
    fi

    return $result
}

# main test begins

echo "Test-$test_start_time"

# Ctrl-c trap. Catches INT signal
trap "shutdown_servers; exit 0" INT

test_whitelists
result=$?

process_test_result $result $action_on_fail

shutdown_servers

sleep 2

test_blacklists
result=$?

process_test_result $result $action_on_fail

shutdown_servers

exit $result

