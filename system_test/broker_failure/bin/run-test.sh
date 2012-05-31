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

# =================================================================
# run-test.sh
# ===========
# 
# This script performs broker failure tests with the following
# setup in a single local machine:
# 
# 1. A cluster of Kafka source brokers
# 2. A cluster of Kafka mirror brokers with embedded consumers in 
#    point-to-point mode
# 3. An independent ConsoleConsumer in publish/subcribe mode to
#    consume messages from the SOURCE brokers cluster
# 4. An independent ConsoleConsumer in publish/subcribe mode to
#    consume messages from the MIRROR brokers cluster
# 5. A producer produces batches of messages to the SOURCE brokers
# 6. One of the Kafka SOURCE or MIRROR brokers in the cluster will
#    be randomly terminated and waiting for the consumer to catch up.
# 7. Repeat Step 4 & 5 as many times as specified in the script
# 
# Expected results:
# ==================
# There should not be any discrepancies by comparing the unique 
# message checksums from the source ConsoleConsumer and the 
# mirror ConsoleConsumer.
# 
# Notes:
# ==================
# The number of Kafka SOURCE brokers can be increased as follows:
# 1. Update the value of $num_kafka_source_server in this script
# 2. Make sure that there are corresponding number of prop files:
#    $base_dir/config/server_source{1..4}.properties
# 
# The number of Kafka TARGET brokers can be increased as follows:
# 1. Update the value of $num_kafka_target_server in this script
# 2. Make sure that there are corresponding number of prop files:
#    $base_dir/config/server_target{1..3}.properties
# 
# Quick Start:
# ==================
# Execute this script as follows:
#   <kafka home>/system_test/broker_failure $ bin/run-test.sh
# 
# The expected output is given in bin/expected.out.
# 
# In the event of failure, by default the brokers and zookeepers
# remain running to make it easier to debug the issue - hit Ctrl-C
# to shut them down. 
# =================================================================

readonly base_dir=$(dirname $0)/..
readonly test_start_time="$(date +%s)"

readonly num_msg_per_batch=500
readonly batches_per_iteration=5
readonly num_iterations=5

readonly zk_source_port=2181
readonly zk_mirror_port=2182

readonly topic_prefix=test
readonly max_topic_id=2
readonly unbalanced_start_id=2
readonly consumer_grp=group1
readonly source_console_consumer_grp=source
readonly mirror_console_consumer_grp=mirror
readonly message_size=5000

# sleep time between each batch of messages published
# from producer - it will be randomly generated
# within the range of sleep_min & sleep_max
readonly sleep_min=3
readonly sleep_max=3

# requires same no. of property files such as:
# $base_dir/config/server_source{1..4}.properties
readonly num_kafka_source_server=4

# requires same no. of property files such as:
# $base_dir/config/server_target{1..3}.properties
readonly num_kafka_target_server=3

readonly wait_time_after_killing_broker=0
readonly wait_time_after_restarting_broker=5

readonly producer_4_brokerinfo_str="broker.list=1:localhost:9091,2:localhost:9092,3:localhost:9093,4:localhost:9094"
readonly producer_3_brokerinfo_str="broker.list=1:localhost:9091,2:localhost:9092,3:localhost:9093"

background_producer_pid_1=
background_producer_pid_2=

no_bouncing=$#

iter=1
abort_test=false

pid_zk_source=
pid_zk_target=

kafka_source_pids=
kafka_source_prop_files=
kafka_source_log_files=

kafka_target_pids=
kafka_target_prop_files=
kafka_target_log_files=
mirror_producer_prop_files=

console_consumer_source_pid=
console_consumer_mirror_pid=

console_consumer_source_log=$base_dir/console_consumer_source.log
console_consumer_mirror_log=$base_dir/console_consumer_mirror.log
producer_performance_log=$base_dir/producer_performance.log

console_consumer_source_crc_log=$base_dir/console_consumer_source_crc.log
console_consumer_source_crc_sorted_log=$base_dir/console_consumer_source_crc_sorted.log
console_consumer_source_crc_sorted_uniq_log=$base_dir/console_consumer_source_crc_sorted_uniq.log

console_consumer_mirror_crc_log=$base_dir/console_consumer_mirror_crc.log
console_consumer_mirror_crc_sorted_log=$base_dir/console_consumer_mirror_crc_sorted.log
console_consumer_mirror_crc_sorted_uniq_log=$base_dir/console_consumer_mirror_crc_sorted_uniq.log

producer_performance_crc_log=$base_dir/producer_performance_crc.log
producer_performance_crc_sorted_log=$base_dir/producer_performance_crc_sorted.log
producer_performance_crc_sorted_uniq_log=$base_dir/producer_performance_crc_sorted_uniq.log

consumer_rebalancing_log=$base_dir/consumer_rebalancing_verification.log

consumer_prop_file=$base_dir/config/whitelisttest.consumer.properties
checksum_diff_log=$base_dir/checksum_diff.log

info() {
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") $*"
}

info_no_newline() {
    echo -e -n "$(date +"%Y-%m-%d %H:%M:%S") $*"
}

initialize() {
    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        kafka_target_prop_files[${i}]=$base_dir/config/server_target${i}.properties
        kafka_target_log_files[${i}]=$base_dir/kafka_target${i}.log
        mirror_producer_prop_files[${i}]=$base_dir/config/mirror_producer${i}.properties
    done

    for ((i=1; i<=$num_kafka_source_server; i++))
    do
        kafka_source_prop_files[${i}]=$base_dir/config/server_source${i}.properties
        kafka_source_log_files[${i}]=$base_dir/kafka_source${i}.log
    done
}

# =========================================
# get_random_range - return a random number
#     between the lower & upper bounds
# usage:
#     get_random_range $lower $upper
#     random_no=$?
# =========================================
get_random_range() {
    lo=$1
    up=$2
    range=$(($up - $lo + 1))

    return $(($(($RANDOM % range)) + $lo))
}

verify_consumer_rebalancing() {

    info "Verifying consumer rebalancing operation"

    CONSUMER_REBALANCING_RESULT=`$base_dir/bin/kafka-run-class.sh \
                                 kafka.tools.VerifyConsumerRebalance \
                                 --zk.connect=localhost:2181 \
                                 --group $consumer_grp`
    echo "$CONSUMER_REBALANCING_RESULT" >> $consumer_rebalancing_log

    REBALANCE_STATUS_LINE=`grep "Rebalance operation" $consumer_rebalancing_log | tail -1`
    # info "REBALANCE_STATUS_LINE: $REBALANCE_STATUS_LINE"
    REBALANCE_STATUS=`echo $REBALANCE_STATUS_LINE | grep "Rebalance operation successful" || echo -n "Rebalance operation failed"`
    info "REBALANCE_STATUS: $REBALANCE_STATUS"

    if [ "${REBALANCE_STATUS}_x" == "Rebalance operation failed_x" ]; then
        info "setting abort_test to true due to Rebalance operation failed"
        abort_test="true"
    fi
}

wait_for_zero_consumer_lags() {

    topic_id=$1

    # no of times to check for zero lagging
    no_of_zero_to_verify=3

    while [ 'x' == 'x' ]
    do
        TOTAL_LAG=0
        CONSUMER_LAGS=`$base_dir/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker \
                       --group $consumer_grp --zkconnect localhost:$zk_source_port \
                       --topic ${topic_prefix}_${topic_id} | grep "Consumer lag" | tr -d ' ' | cut -f2 -d '='`

        for lag in $CONSUMER_LAGS;
        do
            TOTAL_LAG=$(($TOTAL_LAG + $lag))
        done

        info "mirror TOTAL_LAG = $TOTAL_LAG"
        if [ $TOTAL_LAG -eq 0 ]; then
            if [ $no_of_zero_to_verify -eq 0 ]; then
                echo
                return 0
            fi
            no_of_zero_to_verify=$(($no_of_zero_to_verify - 1))
        fi
        sleep 1
    done
}

wait_for_zero_source_console_consumer_lags() {

    topic_id=$1

    # no of times to check for zero lagging
    no_of_zero_to_verify=3

    while [ 'x' == 'x' ]
    do
        TOTAL_LAG=0
        CONSUMER_LAGS=`$base_dir/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker \
                       --group $source_console_consumer_grp --zkconnect localhost:$zk_source_port \
                       --topic ${topic_prefix}_${topic_id} | grep "Consumer lag" | tr -d ' ' | cut -f2 -d '='`

        for lag in $CONSUMER_LAGS;
        do
            TOTAL_LAG=$(($TOTAL_LAG + $lag))
        done

        info "source console consumer TOTAL_LAG = $TOTAL_LAG"
        if [ $TOTAL_LAG -eq 0 ]; then
            if [ $no_of_zero_to_verify -eq 0 ]; then
                echo
                return 0
            fi
            no_of_zero_to_verify=$(($no_of_zero_to_verify - 1))
        fi
        sleep 1
    done
}

wait_for_zero_mirror_console_consumer_lags() {

    topic_id=$1

    # no of times to check for zero lagging
    no_of_zero_to_verify=3

    while [ 'x' == 'x' ]
    do
        TOTAL_LAG=0
        CONSUMER_LAGS=`$base_dir/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker \
                       --group $mirror_console_consumer_grp --zkconnect localhost:$zk_mirror_port \
                       --topic ${topic_prefix}_${topic_id} | grep "Consumer lag" | tr -d ' ' | cut -f2 -d '='`

        for lag in $CONSUMER_LAGS;
        do
            TOTAL_LAG=$(($TOTAL_LAG + $lag))
        done

        info "mirror console consumer TOTAL_LAG = $TOTAL_LAG"
        if [ $TOTAL_LAG -eq 0 ]; then
            if [ $no_of_zero_to_verify -eq 0 ]; then
                echo
                return 0
            fi
            no_of_zero_to_verify=$(($no_of_zero_to_verify - 1))
        fi
        sleep 1
    done
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

    rm -rf /tmp/zookeeper_source
    rm -rf /tmp/zookeeper_target

    rm -rf /tmp/kafka-source{1..4}-logs
    rm -rf /tmp/kafka-target{1..3}-logs

    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        rm -rf ${kafka_target_log_files[${i}]}
    done

    rm -f $base_dir/zookeeper_source.log
    rm -f $base_dir/zookeeper_target.log
    rm -f $base_dir/kafka_source{1..4}.log

    rm -f $producer_performance_log
    rm -f $producer_performance_crc_log
    rm -f $producer_performance_crc_sorted_log
    rm -f $producer_performance_crc_sorted_uniq_log

    rm -f $console_consumer_mirror_log
    rm -f $console_consumer_source_log
    rm -f $console_consumer_mirror_crc_log
    rm -f $console_consumer_source_crc_log

    rm -f $checksum_diff_log

    rm -f $console_consumer_mirror_crc_sorted_log
    rm -f $console_consumer_source_crc_sorted_log
    rm -f $console_consumer_mirror_crc_sorted_uniq_log
    rm -f $console_consumer_source_crc_sorted_uniq_log

    rm -f $consumer_rebalancing_log
}

start_zk() {
    info "starting zookeepers"

    $base_dir/../../bin/zookeeper-server-start.sh \
        $base_dir/config/zookeeper_source.properties \
        2>&1 > $base_dir/zookeeper_source.log &
    pid_zk_source=$!

    $base_dir/../../bin/zookeeper-server-start.sh \
        $base_dir/config/zookeeper_target.properties \
        2>&1 > $base_dir/zookeeper_target.log &
    pid_zk_target=$!
}

start_source_servers_cluster() {
    info "starting source cluster"

    for ((i=1; i<=$num_kafka_source_server; i++)) 
    do
        start_source_server $i
    done
}

start_source_server() {
    s_idx=$1

    $base_dir/bin/kafka-run-class.sh kafka.Kafka \
        ${kafka_source_prop_files[$s_idx]} \
        2>&1 >> ${kafka_source_log_files[$s_idx]} &    # append log msg after restarting
    kafka_source_pids[${s_idx}]=$!

    info "  -> kafka_source_pids[$s_idx]: ${kafka_source_pids[$s_idx]}"
}

start_target_servers_cluster() {
    info "starting mirror cluster"

    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        start_embedded_consumer_server $i
    done
}

start_embedded_consumer_server() {
    s_idx=$1

    $base_dir/bin/kafka-run-class.sh kafka.Kafka \
        ${kafka_target_prop_files[${s_idx}]} \
        $consumer_prop_file \
        ${mirror_producer_prop_files[${s_idx}]} \
        2>&1 >> ${kafka_target_log_files[${s_idx}]} &    # append log msg after restarting
    kafka_target_pids[$s_idx]=$!

    info "  -> kafka_target_pids[$s_idx]: ${kafka_target_pids[$s_idx]}"
}

start_console_consumer_for_source_producer() {

    topic_id=$1

    info "starting console consumers for source producer on topic id [$topic_id]"

    $base_dir/bin/kafka-run-class.sh kafka.consumer.ConsoleConsumer \
        --zookeeper localhost:$zk_source_port \
        --topic ${topic_prefix}_${topic_id} \
        --group $source_console_consumer_grp \
        --from-beginning --consumer-timeout-ms 5000 \
        --formatter "kafka.consumer.ConsoleConsumer\$ChecksumMessageFormatter" \
        --property topic=${topic_prefix}_${topic_id} \
        2>&1 >> ${console_consumer_source_log} 
}

start_console_consumer_for_mirror_producer() {

    topic_id=$1

    info "starting console consumers for mirroring producer on topic id [$topic_id]"

    $base_dir/bin/kafka-run-class.sh kafka.consumer.ConsoleConsumer \
        --zookeeper localhost:$zk_mirror_port \
        --topic ${topic_prefix}_${topic_id} \
        --group $mirror_console_consumer_grp \
        --from-beginning --consumer-timeout-ms 5000 \
        --formatter "kafka.consumer.ConsoleConsumer\$ChecksumMessageFormatter" \
        --property topic=${topic_prefix}_${topic_id} \
        2>&1 >> ${console_consumer_mirror_log} 
}

consume_source_producer_messages() {
    consumer_counter=1
    while [ $consumer_counter -le $max_topic_id ]
    do
        start_console_consumer_for_source_producer $consumer_counter
        consumer_counter=$(( $consumer_counter + 1 ))
    done
}

consume_mirror_producer_messages() {
    consumer_counter=1
    while [ $consumer_counter -le $max_topic_id ]
    do
        start_console_consumer_for_mirror_producer $consumer_counter
        consumer_counter=$(( $consumer_counter + 1 ))
    done
}

shutdown_producer() {
    info "shutting down producer"
    if [ "x${background_producer_pid_1}" != "x" ]; then
        # kill_child_processes 0 ${background_producer_pid_1};
        kill -TERM ${background_producer_pid_1} 2> /dev/null;
    fi

    if [ "x${background_producer_pid_2}" != "x" ]; then
        # kill_child_processes 0 ${background_producer_pid_2};
        kill -TERM ${background_producer_pid_2} 2> /dev/null;
    fi
}

shutdown_servers() {
    info "shutting down mirror console consumer"
    if [ "x${console_consumer_mirror_pid}" != "x" ]; then 
        kill_child_processes 0 ${console_consumer_mirror_pid};
    fi

    info "shutting down source console consumer"
    if [ "x${console_consumer_source_pid}" != "x" ]; then 
        kill_child_processes 0 ${console_consumer_source_pid};
    fi

    info "shutting down target servers"
    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        if [ "x${kafka_target_pids[$i]}" != "x" ]; then
            kill_child_processes 0 ${kafka_target_pids[$i]};
        fi
    done

    info "shutting down source servers"
    for ((i=1; i<=$num_kafka_source_server; i++))
    do
        if [ "x${kafka_source_pids[$i]}" != "x" ]; then
            kill_child_processes 0 ${kafka_source_pids[$i]};
        fi
    done

    info "shutting down zookeeper servers"
    if [ "x${pid_zk_target}" != "x" ]; then kill_child_processes 0 ${pid_zk_target}; fi
    if [ "x${pid_zk_source}" != "x" ]; then kill_child_processes 0 ${pid_zk_source}; fi
}

start_background_producer() {
    bkrinfo_str=$1
    start_topic_id=$2
    end_topic_id=$3

    batch_no=0
    topic_id=${start_topic_id}

    while [ 'x' == 'x' ]
    do
        sleeptime=

        get_random_range $sleep_min $sleep_max
        sleeptime=$?

        batch_no=$(($batch_no + 1))

        if [ $topic_id -gt $end_topic_id ]; then
            topic_id=${start_topic_id}
        fi

        $base_dir/bin/kafka-run-class.sh \
            kafka.perf.ProducerPerformance \
            --brokerinfo $bkrinfo_str \
            --topic ${topic_prefix}_${topic_id} \
            --messages $num_msg_per_batch \
            --message-size $message_size \
            --batch-size 50 \
            --vary-message-size \
            --threads 1 \
            --reporting-interval $num_msg_per_batch --async \
            2>&1 >> $base_dir/producer_performance.log    # appending all producers' msgs

        topic_id=$(( $topic_id + 1 ))

        sleep $sleeptime
    done
}

cmp_checksum() {

    cmp_result=0

    grep checksum $console_consumer_source_log | tr -d ' ' | cut -f2 -d ':' > $console_consumer_source_crc_log
    grep checksum $console_consumer_mirror_log | tr -d ' ' | cut -f2 -d ':' > $console_consumer_mirror_crc_log
    grep checksum $producer_performance_log | tr -d ' ' | cut -f4 -d ':' | cut -f1 -d '(' > $producer_performance_crc_log

    sort $console_consumer_mirror_crc_log > $console_consumer_mirror_crc_sorted_log
    sort $console_consumer_source_crc_log > $console_consumer_source_crc_sorted_log
    sort $producer_performance_crc_log > $producer_performance_crc_sorted_log

    sort -u $console_consumer_mirror_crc_log > $console_consumer_mirror_crc_sorted_uniq_log
    sort -u $console_consumer_source_crc_log > $console_consumer_source_crc_sorted_uniq_log
    sort -u $producer_performance_crc_log > $producer_performance_crc_sorted_uniq_log

    msg_count_from_source_consumer=`cat $console_consumer_source_crc_log | wc -l | tr -d ' '`
    uniq_msg_count_from_source_consumer=`cat $console_consumer_source_crc_sorted_uniq_log | wc -l | tr -d ' '`

    msg_count_from_mirror_consumer=`cat $console_consumer_mirror_crc_log | wc -l | tr -d ' '`
    uniq_msg_count_from_mirror_consumer=`cat $console_consumer_mirror_crc_sorted_uniq_log | wc -l | tr -d ' '`

    uniq_msg_count_from_producer=`cat $producer_performance_crc_sorted_uniq_log | wc -l | tr -d ' '`

    total_msg_published=`cat $producer_performance_crc_log | wc -l | tr -d ' '`

    duplicate_msg_in_producer=$(( $total_msg_published - $uniq_msg_count_from_producer ))

    crc_only_in_mirror_consumer=`comm -23 $console_consumer_mirror_crc_sorted_uniq_log $console_consumer_source_crc_sorted_uniq_log`
    crc_only_in_source_consumer=`comm -13 $console_consumer_mirror_crc_sorted_uniq_log $console_consumer_source_crc_sorted_uniq_log`
    crc_common_in_both_consumer=`comm -12 $console_consumer_mirror_crc_sorted_uniq_log $console_consumer_source_crc_sorted_uniq_log`

    crc_only_in_producer=`comm -23 $producer_performance_crc_sorted_uniq_log $console_consumer_source_crc_sorted_uniq_log`

    duplicate_mirror_crc=`comm -23 $console_consumer_mirror_crc_sorted_log $console_consumer_mirror_crc_sorted_uniq_log` 
    no_of_duplicate_msg=$(( $msg_count_from_mirror_consumer - $uniq_msg_count_from_mirror_consumer \
                          + $msg_count_from_source_consumer - $uniq_msg_count_from_source_consumer - \
                          2*$duplicate_msg_in_producer ))

    echo ""
    echo "========================================================"
    echo "no. of messages published            : $total_msg_published"
    echo "producer unique msg rec'd            : $uniq_msg_count_from_producer"
    echo "source consumer msg rec'd            : $msg_count_from_source_consumer"
    echo "source consumer unique msg rec'd     : $uniq_msg_count_from_source_consumer"
    echo "mirror consumer msg rec'd            : $msg_count_from_mirror_consumer"
    echo "mirror consumer unique msg rec'd     : $uniq_msg_count_from_mirror_consumer"
    echo "total source/mirror duplicate msg    : $no_of_duplicate_msg"
    echo "source/mirror uniq msg count diff    : $(($uniq_msg_count_from_source_consumer - \
                                                 $uniq_msg_count_from_mirror_consumer))"
    echo "========================================================"
    echo "(Please refer to $checksum_diff_log for more details)"
    echo ""

    echo "========================================================" >> $checksum_diff_log
    echo "crc only in producer"                                     >> $checksum_diff_log 
    echo "========================================================" >> $checksum_diff_log
    echo "${crc_only_in_producer}"                                  >> $checksum_diff_log 
    echo ""                                                         >> $checksum_diff_log
    echo "========================================================" >> $checksum_diff_log
    echo "crc only in source consumer"                              >> $checksum_diff_log 
    echo "========================================================" >> $checksum_diff_log
    echo "${crc_only_in_source_consumer}"                           >> $checksum_diff_log 
    echo ""                                                         >> $checksum_diff_log
    echo "========================================================" >> $checksum_diff_log
    echo "crc only in mirror consumer"                              >> $checksum_diff_log
    echo "========================================================" >> $checksum_diff_log
    echo "${crc_only_in_mirror_consumer}"                           >> $checksum_diff_log   
    echo ""                                                         >> $checksum_diff_log
    echo "========================================================" >> $checksum_diff_log
    echo "duplicate crc in mirror consumer"                         >> $checksum_diff_log
    echo "========================================================" >> $checksum_diff_log
    echo "${duplicate_mirror_crc}"                                  >> $checksum_diff_log

    topic_chksum_counter=1
    while [ $topic_chksum_counter -le $max_topic_id ]
    do
        # get producer topic counts
        this_chksum_count=`grep -c ${topic_prefix}_${topic_chksum_counter}\- $producer_performance_log`
        echo "PRODUCER topic ${topic_prefix}_${topic_chksum_counter} count: ${this_chksum_count}"

        topic_chksum_counter=$(($topic_chksum_counter + 1))
    done
    echo

    topic_chksum_counter=1
    while [ $topic_chksum_counter -le $max_topic_id ]
    do
        this_chksum_count=`grep -c ${topic_prefix}_${topic_chksum_counter}\- $console_consumer_source_log`
        echo "SOURCE consumer topic ${topic_prefix}_${topic_chksum_counter} count: ${this_chksum_count}"

        topic_chksum_counter=$(($topic_chksum_counter + 1))
    done
    echo

    topic_chksum_counter=1
    while [ $topic_chksum_counter -le $max_topic_id ]
    do
        this_chksum_count=`grep -c ${topic_prefix}_${topic_chksum_counter}\- $console_consumer_mirror_log`
        echo "MIRROR consumer topic ${topic_prefix}_${topic_chksum_counter} count: ${this_chksum_count}"

        topic_chksum_counter=$(($topic_chksum_counter + 1))
    done
    echo

    return $cmp_result
}

start_test() {

    start_zk
    sleep 2
    start_source_servers_cluster
    sleep 2
    start_target_servers_cluster
    sleep 2

    start_background_producer $producer_4_brokerinfo_str 1 $(( $unbalanced_start_id - 1 )) &
    background_producer_pid_1=$!

    info "=========================================="
    info "Started background producer pid [${background_producer_pid_1}]"
    info "=========================================="

    sleep 10
   
    start_background_producer $producer_3_brokerinfo_str $unbalanced_start_id $max_topic_id &
    background_producer_pid_2=$!

    info "=========================================="
    info "Started background producer pid [${background_producer_pid_2}]"
    info "=========================================="

    sleep 10

    verify_consumer_rebalancing

    info "abort_test: [${abort_test}]"
    if [ "${abort_test}_x" == "true_x" ]; then
        info "aborting test"
        iter=$((${num_iterations} + 1))
    fi
 
    while [ $num_iterations -ge $iter ]
    do
        echo
        info "=========================================="
        info "Iteration $iter of ${num_iterations}"
        info "=========================================="

        # terminate the broker if not the last iteration:
        if [[ $num_iterations -gt $iter && $no_bouncing -eq 0 ]]; then

            idx=

            if [ $(( $iter % 2 )) -eq 0 ]; then
                # even iterations -> bounce target kafka borker
                get_random_range 1 $num_kafka_target_server 
                idx=$?
                if [ "x${kafka_target_pids[$idx]}" != "x" ]; then
                    echo
                    info "#### Bouncing kafka TARGET broker ####"

                    info "terminating kafka target[$idx] with process id ${kafka_target_pids[$idx]}"
                    kill_child_processes 0 ${kafka_target_pids[$idx]}

                    info "sleeping for ${wait_time_after_killing_broker}s"
                    sleep $wait_time_after_killing_broker

                    info "starting kafka target server"
                    start_embedded_consumer_server $idx

                    info "sleeping for ${wait_time_after_restarting_broker}s"
                    sleep $wait_time_after_restarting_broker
                fi
            else
                # odd iterations -> bounce source kafka broker
                get_random_range 1 $num_kafka_source_server 
                idx=$?

                if [ "x${kafka_source_pids[$idx]}" != "x" ]; then
                    echo
                    info "#### Bouncing kafka SOURCE broker ####"

                    info "terminating kafka source[$idx] with process id ${kafka_source_pids[$idx]}"
                    kill_child_processes 0 ${kafka_source_pids[$idx]}

                    info "sleeping for ${wait_time_after_killing_broker}s"
                    sleep $wait_time_after_killing_broker

                    info "starting kafka source server"
                    start_source_server $idx

                    info "sleeping for ${wait_time_after_restarting_broker}s"
                    sleep $wait_time_after_restarting_broker
                fi
            fi

            verify_consumer_rebalancing

            info "abort_test: [${abort_test}]"
            if [ "${abort_test}_x" == "true_x" ]; then
                info "aborting test"
                iter=$((${num_iterations} + 1))
            fi

        else
            info "No bouncing performed"
        fi

        info "sleeping for 10 sec"
        sleep 10

        iter=$(($iter+1))
    done

    echo
    info "Tests completed. Waiting for consumers to catch up "
    
    shutdown_producer

    wait_for_zero_consumer_lags
}


# =====================
# main test begins here
# =====================

echo
info "============================================"
info "#### Starting Kafka Broker Failure Test ####"
info "============================================"
echo

initialize
cleanup
sleep 5

# Ctrl-c trap. Catches INT signal
trap "shutdown_producer; shutdown_servers; cmp_checksum; exit 0" INT

start_test

consume_source_producer_messages
consume_mirror_producer_messages

wait_for_zero_source_console_consumer_lags
wait_for_zero_mirror_console_consumer_lags

verify_consumer_rebalancing

shutdown_servers

cmp_checksum
result=$?

exit $result
