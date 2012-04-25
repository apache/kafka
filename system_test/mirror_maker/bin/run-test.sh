#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

readonly num_messages=10000
readonly message_size=100
readonly action_on_fail="proceed"
# readonly action_on_fail="wait"

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

    pid_zk_source1=
    pid_zk_source2=
    pid_zk_target=
    pid_kafka_source_1_1=
    pid_kafka_source_1_2=
    pid_kafka_source_2_1=
    pid_kafka_source_2_2=
    pid_kafka_target_1_1=
    pid_kafka_target_1_2=
    pid_producer=
    pid_mirrormaker_1=
    pid_mirrormaker_2=

    rm -rf /tmp/zookeeper*

    rm -rf /tmp/kafka*
}

begin_timer() {
    t_begin=$(date +%s)
}

end_timer() {
    t_end=$(date +%s)
}

start_zk() {
    info "starting zookeepers"
    $base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_source_1.properties 2>&1 > $base_dir/zookeeper_source-1.log &
    pid_zk_source1=$!
    $base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_source_2.properties 2>&1 > $base_dir/zookeeper_source-2.log &
    pid_zk_source2=$!
    $base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_target.properties 2>&1 > $base_dir/zookeeper_target.log &
    pid_zk_target=$!
}

start_source_servers() {
    info "starting source cluster"

    JMX_PORT=1111 $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source_1_1.properties 2>&1 > $base_dir/kafka_source-1-1.log &
    pid_kafka_source_1_1=$!
    JMX_PORT=2222 $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source_1_2.properties 2>&1 > $base_dir/kafka_source-1-2.log &
    pid_kafka_source_1_2=$!
    JMX_PORT=3333 $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source_2_1.properties 2>&1 > $base_dir/kafka_source-2-1.log &
    pid_kafka_source_2_1=$!
    JMX_PORT=4444 $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source_2_2.properties 2>&1 > $base_dir/kafka_source-2-2.log &
    pid_kafka_source_2_2=$!
}

start_target_servers() {
    info "starting mirror cluster"
    JMX_PORT=5555 $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target_1_1.properties 2>&1 > $base_dir/kafka_target-1-1.log &
    pid_kafka_target_1_1=$!
    JMX_PORT=6666 $base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target_1_2.properties 2>&1 > $base_dir/kafka_target-1-2.log &
    pid_kafka_target_1_2=$!
}

shutdown_servers() {
    info "stopping mirror-maker"
    if [ "x${pid_mirrormaker_1}" != "x" ]; then kill_child_processes 0 ${pid_mirrormaker_1}; fi
    # sleep to avoid rebalancing during shutdown
    sleep 2
    if [ "x${pid_mirrormaker_2}" != "x" ]; then kill_child_processes 0 ${pid_mirrormaker_2}; fi

    info "stopping producer"
    if [ "x${pid_producer}" != "x" ]; then kill_child_processes 0 ${pid_producer}; fi

    info "shutting down target servers"
    if [ "x${pid_kafka_target_1_1}" != "x" ]; then kill_child_processes 0 ${pid_kafka_target_1_1}; fi
    if [ "x${pid_kafka_target_1_2}" != "x" ]; then kill_child_processes 0 ${pid_kafka_target_1_2}; fi
    sleep 2

    info "shutting down source servers"
    if [ "x${pid_kafka_source_1_1}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source_1_1}; fi
    if [ "x${pid_kafka_source_1_2}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source_1_2}; fi
    if [ "x${pid_kafka_source_2_1}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source_2_1}; fi
    if [ "x${pid_kafka_source_2_2}" != "x" ]; then kill_child_processes 0 ${pid_kafka_source_2_2}; fi

    info "shutting down zookeeper servers"
    if [ "x${pid_zk_target}" != "x" ]; then kill_child_processes 0 ${pid_zk_target}; fi
    if [ "x${pid_zk_source1}" != "x" ]; then kill_child_processes 0 ${pid_zk_source1}; fi
    if [ "x${pid_zk_source2}" != "x" ]; then kill_child_processes 0 ${pid_zk_source2}; fi
}

start_producer() {
    topic=$1
    zk=$2
    info "start producing messages for topic $topic to zookeeper $zk ..."
    $base_dir/../../bin/kafka-run-class.sh kafka.perf.ProducerPerformance --brokerinfo zk.connect=$zk --topic $topic --messages $num_messages --message-size $message_size --batch-size 200 --vary-message-size --threads 1 --reporting-interval $num_messages --async 2>&1 > $base_dir/producer_performance.log &
    pid_producer=$!
}

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
    source_part0_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9090 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    source_part1_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9091 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    source_part2_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9092 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    source_part3_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    target_part0_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9094 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    target_part1_size=$($base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9095 --topic $topic --partition 0 --time -1 --offsets 1 | tail -1)
    if [ "x$source_part0_size" == "x" ]; then source_part0_size=0; fi
    if [ "x$source_part1_size" == "x" ]; then source_part1_size=0; fi
    if [ "x$source_part2_size" == "x" ]; then source_part2_size=0; fi
    if [ "x$source_part3_size" == "x" ]; then source_part3_size=0; fi
    if [ "x$target_part0_size" == "x" ]; then target_part0_size=0; fi
    if [ "x$target_part1_size" == "x" ]; then target_part1_size=0; fi
    expected_size=$(($source_part0_size + $source_part1_size + $source_part2_size + $source_part3_size))
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
    for dir in /tmp/zookeeper_source{1..2} /tmp/zookeeper_target /tmp/kafka-source-{1..2}-{1..2}-logs /tmp/kafka-target{1..2}-logs; do
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
    start_target_servers
    sleep 4

    info "starting mirror makers"
    JMX_PORT=7777 $base_dir/../../bin/kafka-run-class.sh kafka.tools.MirrorMaker --consumer.config $base_dir/config/whitelisttest_1.consumer.properties --consumer.config $base_dir/config/whitelisttest_2.consumer.properties --producer.config $base_dir/config/mirror_producer.properties --whitelist="white.*" --num.streams 2 2>&1 > $base_dir/kafka_mirrormaker_1.log &
    pid_mirrormaker_1=$!
    JMX_PORT=8888 $base_dir/../../bin/kafka-run-class.sh kafka.tools.MirrorMaker --consumer.config $base_dir/config/whitelisttest_1.consumer.properties --consumer.config $base_dir/config/whitelisttest_2.consumer.properties --producer.config $base_dir/config/mirror_producer.properties --whitelist="white.*" --num.streams 2 2>&1 > $base_dir/kafka_mirrormaker_2.log &
    pid_mirrormaker_2=$!

    begin_timer

    start_producer whitetopic01 localhost:2181
    start_producer whitetopic01 localhost:2182
    info "waiting for whitetopic01 producers to finish producing ..."
    wait_partition_done kafka://localhost:9090 whitetopic01 0 kafka://localhost:9091 whitetopic01 0 kafka://localhost:9092 whitetopic01 0 kafka://localhost:9093 whitetopic01 0

    start_producer whitetopic02 localhost:2181
    start_producer whitetopic03 localhost:2181
    start_producer whitetopic04 localhost:2182
    info "waiting for whitetopic02,whitetopic03,whitetopic04 producers to finish producing ..."
    wait_partition_done kafka://localhost:9090 whitetopic02 0 kafka://localhost:9091 whitetopic02 0 kafka://localhost:9090 whitetopic03 0 kafka://localhost:9091 whitetopic03 0 kafka://localhost:9092 whitetopic04 0 kafka://localhost:9093 whitetopic04 0

    start_producer blacktopic01 localhost:2182
    info "waiting for blacktopic01 producer to finish producing ..."
    wait_partition_done kafka://localhost:9092 blacktopic01 0 kafka://localhost:9093 blacktopic01 0

    info "waiting for consumer to finish consuming ..."

    wait_partition_done kafka://localhost:9094 whitetopic01 0 kafka://localhost:9095 whitetopic01 0 kafka://localhost:9094 whitetopic02 0 kafka://localhost:9095 whitetopic02 0 kafka://localhost:9094 whitetopic03 0 kafka://localhost:9095 whitetopic03 0 kafka://localhost:9094 whitetopic04 0 kafka://localhost:9095 whitetopic04 0

    end_timer
    info "embedded consumer took $((t_end - t_begin)) seconds"

    sleep 2

    # if [[ -d /tmp/kafka-target-1-1-logs/blacktopic01 || /tmp/kafka-target-1-2-logs/blacktopic01 ]]; then
    #     echo "blacktopic01 found on target cluster"
    #     result=1
    # else
    #     cmp_logs whitetopic01 && cmp_logs whitetopic02 && cmp_logs whitetopic03 && cmp_logs whitetopic04
    #     result=$?
    # fi

    cmp_logs blacktopic01

    cmp_logs whitetopic01 && cmp_logs whitetopic02 && cmp_logs whitetopic03 && cmp_logs whitetopic04
    result=$?

    return $result
}

test_blacklists() {
    info "### Testing blacklists"
    snapshot_prefix="blacklist-test"
    cleanup
    start_zk
    start_source_servers
    start_target_servers
    sleep 4

    info "starting mirror maker"
    $base_dir/../../bin/kafka-run-class.sh kafka.tools.MirrorMaker --consumer.config $base_dir/config/blacklisttest.consumer.properties --producer.config $base_dir/config/mirror_producer.properties --blacklist="black.*" --num.streams 2 2>&1 > $base_dir/kafka_mirrormaker_1.log &
    pid_mirrormaker_1=$!

    start_producer blacktopic01 localhost:2181
    start_producer blacktopic02 localhost:2181
    info "waiting for producer to finish producing blacktopic01,blacktopic02 ..."
    wait_partition_done kafka://localhost:9090 blacktopic01 0 kafka://localhost:9091 blacktopic01 0 kafka://localhost:9090 blacktopic02 0 kafka://localhost:9091 blacktopic02 0

    begin_timer

    start_producer whitetopic01 localhost:2181
    info "waiting for producer to finish producing whitetopic01 ..."
    wait_partition_done kafka://localhost:9090 whitetopic01 0 kafka://localhost:9091 whitetopic01 0

    info "waiting for consumer to finish consuming ..."
    wait_partition_done kafka://localhost:9094 whitetopic01 0 kafka://localhost:9095 whitetopic01 0

    end_timer

    info "embedded consumer took $((t_end - t_begin)) seconds"

    sleep 2

    cmp_logs blacktopic01 || cmp_logs blacktopic02
    if [ $? -eq 0 ]; then
        return 1
    fi
    
    cmp_logs whitetopic01
    return $?
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

