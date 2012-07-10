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

# ==================================================================
# run-test.sh
# 
# ==================================================================

# ====================================
# Do not change the followings
# (keep this section at the beginning
# of this script)
# ====================================
readonly system_test_root=$(dirname $0)/../..         # path of <kafka install>/system_test
readonly common_dir=${system_test_root}/common        # common util scripts for system_test
source   ${common_dir}/util.sh                        # include the util script

readonly base_dir=$(dirname $0)/..                    # root of this test suite
readonly base_dir_full_path=$(readlink -f $base_dir)  # full path of the root of this test suite
readonly config_dir=${base_dir}/config

readonly test_start_time="$(date +%s)"                # time starting the test

# ====================================
# Change the followings as needed
# ====================================
readonly num_kafka_server=3                           # same no. of property files such as server_{1..n}.properties
                                                      # will be automatically generated
readonly replica_factor=3                             # should be less than or equal to "num_kafka_server"
readonly my_brokerid_to_start=0                       # this should be '0' for now
readonly my_server_port_to_start=9091                 # if using this default, the ports to be used will be 9091, 9092, ...
readonly producer_msg_batch_size=200                  # batch no. of messsages by producer
readonly consumer_timeout_ms=10000                    # elapsed time for consumer to timeout and exit

# ====================================
# No need to change the following
# configurations in most cases
# ====================================
readonly test_topic=mytest
readonly max_wait_for_consumer_complete=30
readonly zk_prop_pathname=${config_dir}/zookeeper.properties
readonly zk_log4j_log_pathname=${base_dir}/zookeeper.log

readonly producer_prop_pathname=${config_dir}/producer.properties
readonly consumer_prop_pathname=${config_dir}/consumer.properties

readonly producer_perf_log_pathname=${base_dir}/producer_perf_output.log
readonly producer_perf_mid_log_pathname=${base_dir}/producer_perf_mid.log
readonly producer_perf_mid_sorted_log_pathname=${base_dir}/producer_perf_mid_sorted.log
readonly producer_perf_mid_sorted_uniq_log_pathname=${base_dir}/producer_perf_mid_sorted_uniq.log

readonly console_consumer_log_pathname=${base_dir}/console_consumer.log
readonly console_consumer_mid_log_pathname=${base_dir}/console_consumer_mid.log
readonly console_consumer_mid_sorted_log_pathname=${base_dir}/console_consumer_mid_sorted.log
readonly console_consumer_mid_sorted_uniq_log_pathname=${base_dir}/console_consumer_mid_sorted_uniq.log

readonly this_test_stderr_output_log_pathname=${base_dir}/this_test_stderr_output.log

# ====================================
# arrays for kafka brokers properties
# ====================================
kafka_data_log_dirs=
kafka_log4j_log_pathnames=
kafka_prop_pathnames=
kafka_brokerids=
kafka_sock_ports=
#kafka_first_data_file_sizes=
#kafka_first_data_file_checksums=

# ====================================
# Misc
# ====================================
zk_port=
zk_data_log_dir=
pid_zk=
kafka_pids=
test_failure_counter=0

initialize() {
    info "initializing ..."

    zk_port=`grep clientPort ${zk_prop_pathname} | awk -F '=' '{print $2}'`
    zk_data_log_dir=`grep dataDir ${zk_prop_pathname} | awk -F '=' '{print $2}'`

    for ((i=1; i<=$num_kafka_server; i++))
    do
        kafka_log4j_log_pathnames[${i}]=$base_dir/kafka_server_${i}.log
        kafka_prop_pathnames[${i}]=${config_dir}/server_${i}.properties

        kafka_data_log_dirs[${i}]=`grep ^log.dir ${kafka_prop_pathnames[${i}]} | awk -F '=' '{print $2}'`
        kafka_brokerids[${i}]=`grep ^brokerid= ${kafka_prop_pathnames[${i}]} | awk -F '=' '{print $2}'`
        kafka_sock_ports[${i}]=`grep ^port= ${kafka_prop_pathnames[${i}]} | awk -F '=' '{print $2}'`

        info "kafka $i data dir   : ${kafka_data_log_dirs[$i]}"
        info "kafka $i log4j log  : ${kafka_log4j_log_pathnames[$i]}"
        info "kafka $i prop file  : ${kafka_prop_pathnames[$i]}"
        info "kafka $i brokerid   : ${kafka_brokerids[$i]}"
        info "kafka $i socket     : ${kafka_sock_ports[$i]}"
    done

    info "zookeeper port     : $zk_port"
    info "zookeeper data dir : $zk_data_log_dir"
    echo
}

cleanup() {
    info "cleaning up kafka server log/data dir"
    for ((i=1; i<=$num_kafka_server; i++))
    do
        rm -rf ${kafka_data_log_dirs[$i]}
        rm -f ${kafka_log4j_log_pathnames[$i]}
    done

    rm -rf $zk_data_log_dir
    rm -f $zk_log4j_log_pathname
    rm -f $this_test_stderr_output_log_pathname

    rm -f $producer_perf_log_pathname
    rm -f $producer_perf_mid_log_pathname
    rm -f $producer_perf_mid_sorted_log_pathname
    rm -f $producer_perf_mid_sorted_uniq_log_pathname

    rm -f $console_consumer_log_pathname
    rm -f $console_consumer_mid_log_pathname
    rm -f $console_consumer_mid_sorted_log_pathname
    rm -f $console_consumer_mid_sorted_uniq_log_pathname
}

get_leader_brokerid() {
    log_line=`grep -i -h 'is leader' ${base_dir}/kafka_server_*.log | sort | tail -1`
    info "found the log line: $log_line"
    broker_id=`echo $log_line | sed s'/^.*INFO Broker //g' | awk -F ' ' '{print $1}'`

    return $broker_id
}

start_zk() {
    info "starting zookeeper"
    $base_dir/../../bin/zookeeper-server-start.sh $zk_prop_pathname \
        2>&1 > ${zk_log4j_log_pathname} &
    pid_zk=$!
}

stop_server() {
    s_idx=$1

    info "stopping server: $s_idx"

    if [ "x${kafka_pids[${s_idx}]}" != "x" ]; then
        kill_child_processes 0 ${kafka_pids[${s_idx}]};
    fi

    kafka_pids[${s_idx}]=
}

start_server() {
    s_idx=$1

    info "starting kafka server"
    $base_dir/bin/kafka-run-class.sh kafka.Kafka ${kafka_prop_pathnames[$s_idx]} \
        2>&1 >> ${kafka_log4j_log_pathnames[$s_idx]} &
    kafka_pids[${s_idx}]=$!
    info "  -> kafka_pids[$s_idx]: ${kafka_pids[$s_idx]}"
}

start_servers_cluster() {
    info "starting cluster"

    for ((i=1; i<=$num_kafka_server; i++)) 
    do
        start_server $i
    done
}

start_producer_perf() {
    this_topic=$1
    zk_conn_str=$2
    no_msg_to_produce=$3
    init_msg_id=$4

    info "starting producer performance"

    ${base_dir}/bin/kafka-run-class.sh kafka.perf.ProducerPerformance \
        --brokerinfo "zk.connect=${zk_conn_str}" \
        --topic ${this_topic} \
        --messages $no_msg_to_produce \
        --message-size 100 \
        --threads 5 \
        --initial-message-id $init_msg_id \
        2>&1 >> $producer_perf_log_pathname
}

start_console_consumer() {
    this_consumer_topic=$1
    this_zk_conn_str=$2

    info "starting console consumer"
    $base_dir/bin/kafka-run-class.sh kafka.consumer.ConsoleConsumer \
        --zookeeper $this_zk_conn_str \
        --topic $this_consumer_topic \
        --formatter 'kafka.consumer.ConsoleConsumer$DecodedMessageFormatter' \
        --consumer-timeout-ms $consumer_timeout_ms \
        2>&1 >> $console_consumer_log_pathname &
}

shutdown_servers() {

    info "shutting down servers"
    for ((i=1; i<=$num_kafka_server; i++))
    do
        if [ "x${kafka_pids[$i]}" != "x" ]; then
            kill_child_processes 0 ${kafka_pids[$i]};
        fi
    done

    info "shutting down zookeeper servers"
    if [ "x${pid_zk}" != "x" ]; then kill_child_processes 0 ${pid_zk}; fi
}

force_shutdown_producer() {
    info "force shutdown producer"
    `ps auxw | grep ProducerPerformance | awk '{print $2}' | xargs kill -9`
}

force_shutdown_consumer() {
    info "force shutdown consumer"
    `ps auxw | grep ConsoleConsumer | awk '{print $2}' | xargs kill -9`
}

create_topic() {
    this_topic_to_create=$1
    this_zk_conn_str=$2
    this_replica_factor=$3

    info "creating topic [$this_topic_to_create] on [$this_zk_conn_str]"
    $base_dir/../../bin/kafka-create-topic.sh --topic $this_topic_to_create \
        --zookeeper $this_zk_conn_str --replica $this_replica_factor
}

validate_results() {

    echo
    info "========================================================"
    info "VALIDATING TEST RESULTS"
    info "========================================================"

    # get the checksums and sizes of the replica data files
    for ((i=1; i<=$num_kafka_server; i++))
    do
        first_data_file_dir=${kafka_data_log_dirs[$i]}/${test_topic}-0
        first_data_file=`ls ${first_data_file_dir} | head -1`
        first_data_file_pathname=${first_data_file_dir}/$first_data_file
        kafka_first_data_file_sizes[$i]=`stat -c%s ${first_data_file_pathname}`
        kafka_first_data_file_checksums[$i]=`cksum ${first_data_file_pathname} | awk '{print $1}'`
        info "## broker[$i] data file: ${first_data_file_pathname} : [${kafka_first_data_file_sizes[$i]}]"
        info "##     ==> crc ${kafka_first_data_file_checksums[$i]}"
    done

    # get the MessageID from messages produced and consumed
    grep MessageID $console_consumer_log_pathname | sed s'/^.*MessageID://g' | awk -F ':' '{print $1}' > $console_consumer_mid_log_pathname
    grep MessageID $producer_perf_log_pathname    | sed s'/^.*MessageID://g' | awk -F ':' '{print $1}' > $producer_perf_mid_log_pathname

    sort $console_consumer_mid_log_pathname > $console_consumer_mid_sorted_log_pathname
    sort $producer_perf_mid_log_pathname    > $producer_perf_mid_sorted_log_pathname

    sort -u $console_consumer_mid_sorted_log_pathname > $console_consumer_mid_sorted_uniq_log_pathname
    sort -u $producer_perf_mid_sorted_log_pathname    > $producer_perf_mid_sorted_uniq_log_pathname

    msg_count_from_console_consumer=`cat $console_consumer_mid_log_pathname | wc -l | tr -d ' '`
    uniq_msg_count_from_console_consumer=`cat $console_consumer_mid_sorted_uniq_log_pathname | wc -l | tr -d ' '`

    msg_count_from_producer_perf=`cat $producer_perf_mid_log_pathname | wc -l | tr -d ' '`
    uniq_msg_count_from_producer_perf=`cat $producer_perf_mid_sorted_uniq_log_pathname | wc -l | tr -d ' '`

    # report the findings
    echo
    info "## no. of messages published            : $msg_count_from_producer_perf"
    info "## producer unique msg published        : $uniq_msg_count_from_producer_perf"
    info "## console consumer msg rec'd           : $msg_count_from_console_consumer"
    info "## console consumer unique msg rec'd    : $uniq_msg_count_from_console_consumer"
    echo

    validation_start_unix_ts=`date +%s`
    curr_unix_ts=`date +%s`
    size_unmatched_idx=1
    while [[ $(( $curr_unix_ts - $validation_start_unix_ts )) -le $max_wait_for_consumer_complete && $size_unmatched_idx -gt 0 ]]
    do
        info "wait 5s (up to ${max_wait_for_consumer_complete}s) and check replicas data sizes"
        sleep 5
        
        first_element_value=${kafka_first_data_file_sizes[1]}
        for ((i=2; i<=${#kafka_first_data_file_sizes[@]}; i++))
        do
            if [ $first_element_value -ne ${kafka_first_data_file_sizes[$i]} ]; then
                size_unmatched_idx=1
                break
            else
                size_unmatched_idx=0
            fi
        done

        curr_unix_ts=`date +%s`
    done

    # validate that sizes of all replicas should match
    first_element_value=${kafka_first_data_file_sizes[1]}
    for ((i=2; i<=${#kafka_first_data_file_sizes[@]}; i++))
    do
        if [ $first_element_value -ne ${kafka_first_data_file_sizes[$i]} ]; then
            info "## FAILURE: Unmatched size found"
            test_failure_counter=$(( $test_failure_counter + 1 ))
        fi
    done

    # validate that checksums of all replicas should match
    first_element_value=${kafka_first_data_file_checksums[1]}
    for ((i=2; i<=${#kafka_first_data_file_checksums[@]}; i++))
    do
        if [ $first_element_value -ne ${kafka_first_data_file_checksums[$i]} ]; then
            info "## FAILURE: Unmatched checksum found"
            test_failure_counter=$(( $test_failure_counter + 1 ))
        fi
    done

    # validate that there is no data loss
    if [ $uniq_msg_count_from_producer_perf -ne $uniq_msg_count_from_console_consumer ]; then
        test_failure_counter=$(( $test_failure_counter + 1 ))
    fi

    # report PASSED or FAILED
    info "========================================================"
    if [ $test_failure_counter -eq 0 ]; then
        info "## Test PASSED"
    else
        info "## Test FAILED"
    fi
    info "========================================================"
}


start_test() {
    echo
    info "======================================="
    info "####  Kafka Replicas System Test   ####"
    info "======================================="
    echo

    # Ctrl-c trap. Catches INT signal
    trap "force_shutdown_producer; force_shutdown_consumer; shutdown_servers; exit 0" INT

    generate_kafka_properties_files $base_dir_full_path $num_kafka_server $my_brokerid_to_start $my_server_port_to_start 

    initialize

    cleanup
    sleep 2

    start_zk
    sleep 2

    start_servers_cluster
    sleep 2

    create_topic $test_topic localhost:$zk_port $replica_factor 2> $this_test_stderr_output_log_pathname

    info "sleeping for 5s"
    sleep 5 
    echo

    for ((i=1; i<=$num_kafka_server; i++))
    do
        info "kafka server [$i] - reading leader"
        get_leader_brokerid
        ldr_bkr_id=$?
        info "leader broker id: $ldr_bkr_id"

        svr_idx=$(($ldr_bkr_id + 1))

        # ==========================================================
        # If KAFKA-350 is fixed, uncomment the following 3 lines to
        # STOP the server for failure test
        # ==========================================================
        #stop_server $svr_idx
        #info "sleeping for 10s"
        #sleep 10

        start_console_consumer $test_topic localhost:$zk_port
        info "sleeping for 5s"
        sleep 5

        init_id=$(( ($i - 1) * $producer_msg_batch_size ))
        start_producer_perf $test_topic localhost:$zk_port $producer_msg_batch_size $init_id
        info "sleeping for 15s"
        sleep 15
        echo

        # ==========================================================
        # If KAFKA-350 is fixed, uncomment the following 3 lines to
        # START the server for failure test
        # ==========================================================
        #start_server $svr_idx
        #info "sleeping for 30s"
        #sleep 30
    done

    validate_results
    echo

    shutdown_servers
    echo
}

# =================================================
# Main Test
# =================================================

start_test
