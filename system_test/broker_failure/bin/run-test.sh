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

# ===========
# run-test.sh
# ===========

# ====================================
# Do not change the followings
# (keep this section at the beginning
# of this script)
# ====================================
readonly system_test_root=$(dirname $0)/../..        # path of <kafka install>/system_test
readonly common_dir=${system_test_root}/common       # common util scripts for system_test
source   ${common_dir}/util.sh                       # include the util script

readonly base_dir=$(dirname $0)/..                   # the base dir of this test suite
readonly test_start_time="$(date +%s)"               # time starting this test
readonly bounce_source_id=1
readonly bounce_mir_mkr_id=2
readonly bounce_target_id=3
readonly log4j_prop_file=$base_dir/config/log4j.properties

iter=1                                               # init a counter to keep track of iterations
num_iterations=5                                     # total no. of iterations to run
svr_to_bounce=0                                      # servers to bounce: 1-source 2-mirror_maker 3-target
                                                     # 12 - source & mirror_maker
                                                     # 13 - source & target

# ====================================
# No need to change the following
# configurations in most cases
# ====================================
readonly zk_source_port=2181                         # source zk port
readonly zk_target_port=2182                         # target zk port
readonly test_topic=test01                           # topic used in this test
readonly consumer_grp=group1                         # consumer group
readonly source_console_consumer_grp=source
readonly target_console_consumer_grp=target
readonly message_size=100
readonly console_consumer_timeout_ms=15000
readonly num_kafka_source_server=4                   # requires same no. of property files such as:
                                                     # $base_dir/config/server_source{1..4}.properties
readonly num_kafka_target_server=3                   # requires same no. of property files such as:
                                                     # $base_dir/config/server_target{1..3}.properties
readonly num_kafka_mirror_maker=3                    # any values greater than 0
readonly wait_time_after_killing_broker=0            # wait after broker is stopped but before starting again
readonly wait_time_after_restarting_broker=10

# ====================================
# Change the followings as needed
# ====================================
num_msg_per_batch=500                                # no. of msg produced in each calling of ProducerPerformance
num_producer_threads=5                               # no. of producer threads to send msg
producer_sleep_min=5                                 # min & max sleep time (in sec) between each
producer_sleep_max=5                                 # batch of messages sent from producer

# ====================================
# zookeeper
# ====================================
pid_zk_source=
pid_zk_target=
zk_log4j_log=

# ====================================
# kafka source
# ====================================
kafka_source_pids=
kafka_source_prop_files=
kafka_source_log_files=
kafka_topic_creation_log_file=$base_dir/kafka_topic_creation.log
kafka_log4j_log=

# ====================================
# kafka target
# ====================================
kafka_target_pids=
kafka_target_prop_files=
kafka_target_log_files=

# ====================================
# mirror maker
# ====================================
kafka_mirror_maker_pids=
kafka_mirror_maker_log_files=
consumer_prop_file=$base_dir/config/whitelisttest.consumer.properties
mirror_producer_prop_files=

# ====================================
# console consumer source
# ====================================
console_consumer_source_pid=
console_consumer_source_log=$base_dir/console_consumer_source.log
console_consumer_source_mid_log=$base_dir/console_consumer_source_mid.log
console_consumer_source_mid_sorted_log=$base_dir/console_consumer_source_mid_sorted.log
console_consumer_source_mid_sorted_uniq_log=$base_dir/console_consumer_source_mid_sorted_uniq.log

# ====================================
# console consumer target
# ====================================
console_consumer_target_pid=
console_consumer_target_log=$base_dir/console_consumer_target.log
console_consumer_target_mid_log=$base_dir/console_consumer_target_mid.log
console_consumer_target_mid_sorted_log=$base_dir/console_consumer_target_mid_sorted.log
console_consumer_target_mid_sorted_uniq_log=$base_dir/console_consumer_target_mid_sorted_uniq.log

# ====================================
# producer
# ====================================
background_producer_pid=
producer_performance_log=$base_dir/producer_performance.log
producer_performance_mid_log=$base_dir/producer_performance_mid.log
producer_performance_mid_sorted_log=$base_dir/producer_performance_mid_sorted.log
producer_performance_mid_sorted_uniq_log=$base_dir/producer_performance_mid_sorted_uniq.log
tmp_file_to_stop_background_producer=/tmp/tmp_file_to_stop_background_producer

# ====================================
# test reports
# ====================================
checksum_diff_log=$base_dir/checksum_diff.log


# ====================================
# initialize prop and log files
# ====================================
initialize() {
    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        kafka_target_prop_files[${i}]=$base_dir/config/server_target${i}.properties
        kafka_target_log_files[${i}]=$base_dir/kafka_target${i}.log
        kafka_mirror_maker_log_files[${i}]=$base_dir/kafka_mirror_maker${i}.log
    done

    for ((i=1; i<=$num_kafka_source_server; i++))
    do
        kafka_source_prop_files[${i}]=$base_dir/config/server_source${i}.properties
        kafka_source_log_files[${i}]=$base_dir/kafka_source${i}.log
    done

    for ((i=1; i<=$num_kafka_mirror_maker; i++))
    do
        mirror_producer_prop_files[${i}]=$base_dir/config/mirror_producer${i}.properties
    done

    zk_log4j_log=`grep "log4j.appender.zookeeperAppender.File=" $log4j_prop_file | awk -F '=' '{print $2}'`
    kafka_log4j_log=`grep "log4j.appender.kafkaAppender.File=" $log4j_prop_file | awk -F '=' '{print $2}'`
}

# =========================================
# cleanup
# =========================================
cleanup() {
    info "cleaning up"

    rm -rf $tmp_file_to_stop_background_producer
    rm -rf $kafka_topic_creation_log_file

    rm -rf /tmp/zookeeper_source
    rm -rf /tmp/zookeeper_target

    rm -rf /tmp/kafka-source{1..4}-logs
    rm -rf /tmp/kafka-target{1..3}-logs

    rm -rf $zk_log4j_log
    rm -rf $kafka_log4j_log

    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        rm -rf ${kafka_target_log_files[${i}]}
        rm -rf ${kafka_mirror_maker_log_files[${i}]}
    done

    rm -f $base_dir/zookeeper_source.log
    rm -f $base_dir/zookeeper_target.log
    rm -f $base_dir/kafka_source{1..4}.log

    rm -f $producer_performance_log
    rm -f $producer_performance_mid_log
    rm -f $producer_performance_mid_sorted_log
    rm -f $producer_performance_mid_sorted_uniq_log

    rm -f $console_consumer_target_log
    rm -f $console_consumer_source_log
    rm -f $console_consumer_target_mid_log
    rm -f $console_consumer_source_mid_log

    rm -f $checksum_diff_log

    rm -f $console_consumer_target_mid_sorted_log
    rm -f $console_consumer_source_mid_sorted_log
    rm -f $console_consumer_target_mid_sorted_uniq_log
    rm -f $console_consumer_source_mid_sorted_uniq_log
}

# =========================================
# wait_for_zero_consumer_lags
# =========================================
wait_for_zero_consumer_lags() {

    this_group_name=$1
    this_zk_port=$2

    # no of times to check for zero lagging
    no_of_zero_to_verify=3

    while [ 'x' == 'x' ]
    do
        TOTAL_LAG=0
        CONSUMER_LAGS=`$base_dir/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker \
                       --group $target_console_consumer_grp \
                       --zkconnect localhost:$zk_target_port \
                       --topic $test_topic \
                       | grep "Consumer lag" | tr -d ' ' | cut -f2 -d '='`

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

# =========================================
# create_topic
# =========================================
create_topic() {
    this_topic_to_create=$1
    this_zk_conn_str=$2
    this_replica_factor=$3

    info "creating topic [$this_topic_to_create] on [$this_zk_conn_str]"
    $base_dir/../../bin/kafka-create-topic.sh \
        --topic $this_topic_to_create \
        --zookeeper $this_zk_conn_str \
        --replica $this_replica_factor \
        2> $kafka_topic_creation_log_file
}

# =========================================
# start_zk
# =========================================
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

# =========================================
# start_source_servers_cluster
# =========================================
start_source_servers_cluster() {
    info "starting source cluster"

    for ((i=1; i<=$num_kafka_source_server; i++))
    do
        start_source_server $i
    done
}

# =========================================
# start_source_server
# =========================================
start_source_server() {
    s_idx=$1

    $base_dir/bin/kafka-run-class.sh kafka.Kafka \
        ${kafka_source_prop_files[$s_idx]} \
        2>&1 >> ${kafka_source_log_files[$s_idx]} &
    kafka_source_pids[${s_idx}]=$!

    info "  -> kafka_source_pids[$s_idx]: ${kafka_source_pids[$s_idx]}"
}

# =========================================
# start_target_servers_cluster
# =========================================
start_target_servers_cluster() {
    info "starting mirror cluster"

    for ((i=1; i<=$num_kafka_target_server; i++))
    do
        start_target_server $i
    done
}

# =========================================
# start_target_server
# =========================================
start_target_server() {
    s_idx=$1

    $base_dir/bin/kafka-run-class.sh kafka.Kafka \
        ${kafka_target_prop_files[${s_idx}]} \
        2>&1 >> ${kafka_target_log_files[${s_idx}]} &
    kafka_target_pids[$s_idx]=$!

    info "  -> kafka_target_pids[$s_idx]: ${kafka_target_pids[$s_idx]}"
}

# =========================================
# start_target_mirror_maker
# =========================================
start_target_mirror_maker() {
    info "starting mirror maker"

    for ((i=1; i<=$num_kafka_mirror_maker; i++))
    do
        start_mirror_maker $i
    done
}

# =========================================
# start_mirror_maker
# =========================================
start_mirror_maker() {
    s_idx=$1

    $base_dir/bin/kafka-run-class.sh kafka.tools.MirrorMaker \
        --consumer.config $consumer_prop_file \
        --producer.config ${mirror_producer_prop_files[${s_idx}]} \
        --whitelist=\".*\" \
        2>&1 >> ${kafka_mirror_maker_log_files[$s_idx]} &
    kafka_mirror_maker_pids[${s_idx}]=$!

    info "  -> kafka_mirror_maker_pids[$s_idx]: ${kafka_mirror_maker_pids[$s_idx]}"
}

# =========================================
# start_console_consumer
# =========================================
start_console_consumer() {

    this_consumer_grp=$1
    this_consumer_zk_port=$2
    this_consumer_log=$3
    this_msg_formatter=$4

    info "starting console consumers for $this_consumer_grp"

    $base_dir/bin/kafka-run-class.sh kafka.tools.ConsoleConsumer \
        --zookeeper localhost:$this_consumer_zk_port \
        --topic $test_topic \
        --group $this_consumer_grp \
        --from-beginning \
        --consumer-timeout-ms $console_consumer_timeout_ms \
        --formatter "kafka.tools.ConsoleConsumer\$${this_msg_formatter}" \
        2>&1 > ${this_consumer_log} &
    console_consumer_pid=$!

    info "  -> console consumer pid: $console_consumer_pid"
}

# =========================================
# force_shutdown_background_producer
# - to be called when user press Ctrl-C
# =========================================
force_shutdown_background_producer() {
    info "force shutting down producer"
    `ps auxw | grep "run\-test\|ProducerPerformance" | grep -v grep | awk '{print $2}' | xargs kill -9`
}

# =========================================
# force_shutdown_consumer
# - to be called when user press Ctrl-C
# =========================================
force_shutdown_consumer() {
    info "force shutting down consumer"
    `ps auxw | grep ChecksumMessageFormatter | grep -v grep | awk '{print $2}' | xargs kill -9`
}

# =========================================
# shutdown_servers
# =========================================
shutdown_servers() {

    info "shutting down mirror makers"
    for ((i=1; i<=$num_kafka_mirror_maker; i++))
    do
        #info "stopping mm pid: ${kafka_mirror_maker_pids[$i]}"
        if [ "x${kafka_mirror_maker_pids[$i]}" != "x" ]; then
            kill_child_processes 0 ${kafka_mirror_maker_pids[$i]};
        fi
    done

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

# =========================================
# start_background_producer
# =========================================
start_background_producer() {

    topic=$1

    batch_no=0

    while [ ! -e $tmp_file_to_stop_background_producer ]
    do
        sleeptime=$(get_random_range $producer_sleep_min $producer_sleep_max)

        info "producing $num_msg_per_batch messages on topic '$topic'"
        $base_dir/bin/kafka-run-class.sh \
            kafka.tools.ProducerPerformance \
            --brokerinfo zk.connect=localhost:2181 \
            --topics $topic \
            --messages $num_msg_per_batch \
            --message-size $message_size \
            --threads $num_producer_threads \
            --initial-message-id $batch_no \
            2>&1 >> $base_dir/producer_performance.log    # appending all producers' msgs

        batch_no=$(($batch_no + $num_msg_per_batch))
        sleep $sleeptime
    done
}

# =========================================
# cmp_checksum
# =========================================
cmp_checksum() {

    cmp_result=0

    grep MessageID $console_consumer_source_log | sed s'/^.*MessageID://g' | awk -F ':' '{print $1}' > $console_consumer_source_mid_log
    grep MessageID $console_consumer_target_log | sed s'/^.*MessageID://g' | awk -F ':' '{print $1}' > $console_consumer_target_mid_log
    grep MessageID $producer_performance_log    | sed s'/^.*MessageID://g' | awk -F ':' '{print $1}' > $producer_performance_mid_log

    sort $console_consumer_target_mid_log > $console_consumer_target_mid_sorted_log
    sort $console_consumer_source_mid_log > $console_consumer_source_mid_sorted_log
    sort $producer_performance_mid_log > $producer_performance_mid_sorted_log

    sort -u $console_consumer_target_mid_log > $console_consumer_target_mid_sorted_uniq_log
    sort -u $console_consumer_source_mid_log > $console_consumer_source_mid_sorted_uniq_log
    sort -u $producer_performance_mid_log > $producer_performance_mid_sorted_uniq_log

    msg_count_from_source_consumer=`cat $console_consumer_source_mid_log | wc -l | tr -d ' '`
    uniq_msg_count_from_source_consumer=`cat $console_consumer_source_mid_sorted_uniq_log | wc -l | tr -d ' '`

    msg_count_from_mirror_consumer=`cat $console_consumer_target_mid_log | wc -l | tr -d ' '`
    uniq_msg_count_from_mirror_consumer=`cat $console_consumer_target_mid_sorted_uniq_log | wc -l | tr -d ' '`

    uniq_msg_count_from_producer=`cat $producer_performance_mid_sorted_uniq_log | wc -l | tr -d ' '`

    total_msg_published=`cat $producer_performance_mid_log | wc -l | tr -d ' '`

    duplicate_msg_in_producer=$(( $total_msg_published - $uniq_msg_count_from_producer ))

    crc_only_in_mirror_consumer=`comm -23 $console_consumer_target_mid_sorted_uniq_log $console_consumer_source_mid_sorted_uniq_log`
    crc_only_in_source_consumer=`comm -13 $console_consumer_target_mid_sorted_uniq_log $console_consumer_source_mid_sorted_uniq_log`
    crc_common_in_both_consumer=`comm -12 $console_consumer_target_mid_sorted_uniq_log $console_consumer_source_mid_sorted_uniq_log`

    crc_only_in_producer=`comm -23 $producer_performance_mid_sorted_uniq_log $console_consumer_source_mid_sorted_uniq_log`

    duplicate_mirror_mid=`comm -23 $console_consumer_target_mid_sorted_log $console_consumer_target_mid_sorted_uniq_log`
    no_of_duplicate_msg=$(( $msg_count_from_mirror_consumer - $uniq_msg_count_from_mirror_consumer \
                          + $msg_count_from_source_consumer - $uniq_msg_count_from_source_consumer - \
                          2*$duplicate_msg_in_producer ))

    source_mirror_uniq_msg_diff=$(($uniq_msg_count_from_source_consumer - $uniq_msg_count_from_mirror_consumer))

    echo ""
    echo "========================================================"
    echo "no. of messages published            : $total_msg_published"
    echo "producer unique msg rec'd            : $uniq_msg_count_from_producer"
    echo "source consumer msg rec'd            : $msg_count_from_source_consumer"
    echo "source consumer unique msg rec'd     : $uniq_msg_count_from_source_consumer"
    echo "mirror consumer msg rec'd            : $msg_count_from_mirror_consumer"
    echo "mirror consumer unique msg rec'd     : $uniq_msg_count_from_mirror_consumer"
    echo "total source/mirror duplicate msg    : $no_of_duplicate_msg"
    echo "source/mirror uniq msg count diff    : $source_mirror_uniq_msg_diff"
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
    echo "${duplicate_mirror_mid}"                                  >> $checksum_diff_log

    echo "================="
    if [[ $source_mirror_uniq_msg_diff -eq 0 && $uniq_msg_count_from_source_consumer -gt 0 ]]; then
        echo "## Test PASSED"
    else
        echo "## Test FAILED"
    fi
    echo "================="
    echo

    return $cmp_result
}

# =========================================
# start_test
# =========================================
start_test() {

    echo
    info "==========================================================="
    info "#### Starting Kafka Broker / Mirror Maker Failure Test ####"
    info "==========================================================="
    echo

    start_zk
    sleep 2

    start_source_servers_cluster
    sleep 2

    create_topic $test_topic localhost:$zk_source_port 1
    sleep 2

    start_target_servers_cluster
    sleep 2

    start_target_mirror_maker
    sleep 2

    start_background_producer $test_topic &
    background_producer_pid=$!

    info "Started background producer pid [${background_producer_pid}]"
    sleep 5

    # loop for no. of iterations specified in $num_iterations
    while [ $num_iterations -ge $iter ]
    do
        # if $svr_to_bounce is '0', it means no bouncing
        if [[ $num_iterations -ge $iter && $svr_to_bounce -gt 0 ]]; then
            idx=

            # check which type of broker bouncing is requested: source, mirror_maker or target

            # $svr_to_bounce contains $bounce_target_id - eg. '3', '123', ... etc
            svr_idx=`expr index $svr_to_bounce $bounce_target_id`
            if [[ $num_iterations -ge $iter && $svr_idx -gt 0 ]]; then
                echo
                info "=========================================="
                info "Iteration $iter of ${num_iterations}"
                info "=========================================="

                # bounce target kafka broker
                idx=$(get_random_range 1 $num_kafka_target_server)

                if [ "x${kafka_target_pids[$idx]}" != "x" ]; then
                    echo
                    info "#### Bouncing Kafka TARGET Broker ####"

                    info "terminating kafka target[$idx] with process id ${kafka_target_pids[$idx]}"
                    kill_child_processes 0 ${kafka_target_pids[$idx]}

                    info "sleeping for ${wait_time_after_killing_broker}s"
                    sleep $wait_time_after_killing_broker

                    info "starting kafka target server"
                    start_target_server $idx
                fi
                iter=$(($iter+1))
                info "sleeping for ${wait_time_after_restarting_broker}s"
                sleep $wait_time_after_restarting_broker
             fi

            # $svr_to_bounce contains $bounce_mir_mkr_id - eg. '2', '123', ... etc
            svr_idx=`expr index $svr_to_bounce $bounce_mir_mkr_id`
            if [[ $num_iterations -ge $iter && $svr_idx -gt 0 ]]; then
                echo
                info "=========================================="
                info "Iteration $iter of ${num_iterations}"
                info "=========================================="

                # bounce mirror maker
                idx=$(get_random_range 1 $num_kafka_mirror_maker)

                if [ "x${kafka_mirror_maker_pids[$idx]}" != "x" ]; then
                    echo
                    info "#### Bouncing Kafka Mirror Maker ####"

                    info "terminating kafka mirror maker [$idx] with process id ${kafka_mirror_maker_pids[$idx]}"
                    kill_child_processes 0 ${kafka_mirror_maker_pids[$idx]}

                    info "sleeping for ${wait_time_after_killing_broker}s"
                    sleep $wait_time_after_killing_broker

                    info "starting kafka mirror maker"
                    start_mirror_maker $idx
                fi
                iter=$(($iter+1))
                info "sleeping for ${wait_time_after_restarting_broker}s"
                sleep $wait_time_after_restarting_broker
             fi

            # $svr_to_bounce contains $bounce_source_id - eg. '1', '123', ... etc
            svr_idx=`expr index $svr_to_bounce $bounce_source_id`
            if [[ $num_iterations -ge $iter && $svr_idx -gt 0 ]]; then
                echo
                info "=========================================="
                info "Iteration $iter of ${num_iterations}"
                info "=========================================="

                # bounce source kafka broker
                idx=$(get_random_range 1 $num_kafka_source_server)

                if [ "x${kafka_source_pids[$idx]}" != "x" ]; then
                    echo
                    info "#### Bouncing Kafka SOURCE Broker ####"

                    info "terminating kafka source[$idx] with process id ${kafka_source_pids[$idx]}"
                    kill_child_processes 0 ${kafka_source_pids[$idx]}

                    info "sleeping for ${wait_time_after_killing_broker}s"
                    sleep $wait_time_after_killing_broker

                    info "starting kafka source server"
                    start_source_server $idx
                fi
                iter=$(($iter+1))
                info "sleeping for ${wait_time_after_restarting_broker}s"
                sleep $wait_time_after_restarting_broker
             fi
        else
            echo
            info "=========================================="
            info "Iteration $iter of ${num_iterations}"
            info "=========================================="

            info "No bouncing performed"
            iter=$(($iter+1))
            info "sleeping for ${wait_time_after_restarting_broker}s"
            sleep $wait_time_after_restarting_broker
        fi
    done

    # notify background producer to stop
    `touch $tmp_file_to_stop_background_producer`

    echo
    info "Tests completed. Waiting for consumers to catch up "

    # =======================================================
    # remove the following 'sleep 30' when KAFKA-313 is fixed
    # =======================================================
    info "sleeping 30 sec"
    sleep 30
}

# =========================================
# print_usage
# =========================================
print_usage() {
    echo
    echo "Error : invalid no. of arguments"
    echo "Usage : $0 -n <no. of iterations> -s <servers to bounce>"
    echo
    echo "  num of iterations - the number of iterations that the test runs"
    echo
    echo "  servers to bounce - the servers to be bounced in a round-robin fashion"
    echo "      Values of the servers:"
    echo "        0 - no bouncing"
    echo "        1 - source broker"
    echo "        2 - mirror maker"
    echo "        3 - target broker"
    echo "      Example:"
    echo "        * To bounce only mirror maker and target broker"
    echo "          in turns, enter the value 23"
    echo "        * To bounce only mirror maker, enter the value 2"
    echo "        * To run the test without bouncing, enter 0"
    echo
    echo "Usage Example : $0 -n 10 -s 12"
    echo "  (run 10 iterations and bounce source broker (1) + mirror maker (2) in turn)"
    echo
}


# =========================================
#
#         Main test begins here
#
# =========================================

# get command line arguments
while getopts "hb:i:n:s:x:" opt
do
    case $opt in
      b)
        num_msg_per_batch=$OPTARG
        ;;
      h)
        print_usage
        exit
        ;;
      i)
        producer_sleep_min=$OPTARG
        ;;
      n)
        num_iterations=$OPTARG
        ;;
      s)
        svr_to_bounce=$OPTARG
        ;;
      x)
        producer_sleep_max=$OPTARG
        ;;
      ?)
        print_usage
        exit
        ;;
    esac
done

# initialize and cleanup
initialize
cleanup
sleep 5

# Ctrl-c trap. Catches INT signal
trap "shutdown_servers; force_shutdown_consumer; force_shutdown_background_producer; cmp_checksum; exit 0" INT

# starting the test
start_test

# starting consumer to consume data in source
start_console_consumer $source_console_consumer_grp $zk_source_port $console_consumer_source_log DecodedMessageFormatter

# starting consumer to consume data in target
start_console_consumer $target_console_consumer_grp $zk_target_port $console_consumer_target_log DecodedMessageFormatter

# wait for zero source consumer lags
wait_for_zero_consumer_lags $source_console_consumer_grp $zk_source_port

# wait for zero target consumer lags
wait_for_zero_consumer_lags $target_console_consumer_grp $zk_target_port

# =======================================================
# remove the following 'sleep 30' when KAFKA-313 is fixed
# =======================================================
info "sleeping 30 sec"
sleep 30

shutdown_servers

cmp_checksum
result=$?

# ===============================================
# Report the time taken
# ===============================================
test_end_time="$(date +%s)"
total_test_time_sec=$(( $test_end_time - $test_start_time ))
total_test_time_min=$(( $total_test_time_sec / 60 ))
info "Total time taken: $total_test_time_min min for $num_iterations iterations"
echo

exit $result
