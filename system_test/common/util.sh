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

# =========================================
# info - print messages with timestamp
# =========================================
info() {
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") $*"
}

# =========================================
# info_no_newline - print messages with
# timestamp without newline
# =========================================
info_no_newline() {
    echo -e -n "$(date +"%Y-%m-%d %H:%M:%S") $*"
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

    echo $(($(($RANDOM % range)) + $lo))
}

# =========================================
# kill_child_processes - terminate a
# process and its child processes
# =========================================
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

# =========================================================================
# generate_kafka_properties_files -
# 1. it takes the following arguments and generate server_{1..n}.properties
#    for the total no. of kafka broker as specified in "num_server"; the
#    resulting properties files will be located at: 
#      <kafka home>/system_test/<test suite>/config
# 2. the default values in the generated properties files will be copied
#    from the settings in config/server.properties while the brokerid and
#    server port will be incremented accordingly
# 3. to generate properties files with non-default values such as 
#    "socket.send.buffer.bytes=2097152", simply add the property with new value
#    to the array variable kafka_properties_to_replace as shown below
# =========================================================================
generate_kafka_properties_files() {

    test_suite_full_path=$1      # eg. <kafka home>/system_test/single_host_multi_brokers
    num_server=$2                # total no. of brokers in the cluster
    brokerid_to_start=$3         # this should be '0' in most cases
    kafka_port_to_start=$4       # if 9091 is used, the rest would be 9092, 9093, ...

    this_config_dir=${test_suite_full_path}/config

    # info "test suite full path : $test_suite_full_path"
    # info "broker id to start   : $brokerid_to_start"
    # info "kafka port to start  : $kafka_port_to_start"
    # info "num of server        : $num_server"
    # info "config dir           : $this_config_dir"

    # =============================================
    # array to keep kafka properties statements
    # from the file 'server.properties' need
    # to be changed from their default values
    # =============================================
    # kafka_properties_to_replace     # DO NOT uncomment this line !!

    # =============================================
    # Uncomment the following kafka properties
    # array element as needed to change the default
    # values. Other kafka properties can be added
    # in a similar fashion.
    # =============================================
    # kafka_properties_to_replace[1]="socket.send.buffer.bytes=2097152"
    # kafka_properties_to_replace[2]="socket.receive.buffer.bytes=2097152"
    # kafka_properties_to_replace[3]="num.partitions=3"
    # kafka_properties_to_replace[4]="socket.request.max.bytes=10485760"

    server_properties=`cat ${this_config_dir}/server.properties`

    for ((i=1; i<=$num_server; i++))
    do
        # ======================
        # update misc properties
        # ======================
        for ((j=1; j<=${#kafka_properties_to_replace[@]}; j++))
        do
            keyword_to_replace=`echo ${kafka_properties_to_replace[${j}]} | awk -F '=' '{print $1}'`
            string_to_be_replaced=`echo "$server_properties" | grep $keyword_to_replace` 
            # info "string to be replaced : [$string_to_be_replaced]"
            # info "string to replace     : [${kafka_properties_to_replace[${j}]}]"

            echo "${server_properties}" | \
              sed -e "s/${string_to_be_replaced}/${kafka_properties_to_replace[${j}]}/g" \
              >${this_config_dir}/server_${i}.properties

            server_properties=`cat ${this_config_dir}/server_${i}.properties`
        done

        # ======================
        # update brokerid
        # ======================
        keyword_to_replace="brokerid="
        string_to_be_replaced=`echo "$server_properties" | grep $keyword_to_replace`
        brokerid_idx=$(( $brokerid_to_start + $i))
        string_to_replace="${keyword_to_replace}${brokerid_idx}"
        # info "string to be replaced : [${string_to_be_replaced}]"
        # info "string to replace     : [${string_to_replace}]"

        echo "${server_properties}" | \
          sed -e "s/${string_to_be_replaced}/${string_to_replace}/g" \
          >${this_config_dir}/server_${i}.properties

        server_properties=`cat ${this_config_dir}/server_${i}.properties`

        # ======================
        # update kafak_port
        # ======================
        keyword_to_replace="port="
        string_to_be_replaced=`echo "$server_properties" | grep $keyword_to_replace`
        port_idx=$(( $kafka_port_to_start + $i - 1 ))
        string_to_replace="${keyword_to_replace}${port_idx}"
        # info "string to be replaced : [${string_to_be_replaced}]"
        # info "string to replace     : [${string_to_replace}]"

        echo "${server_properties}" | \
          sed -e "s/${string_to_be_replaced}/${string_to_replace}/g" \
          >${this_config_dir}/server_${i}.properties

        server_properties=`cat ${this_config_dir}/server_${i}.properties`

        # ======================
        # update kafka_log dir
        # ======================
        keyword_to_replace="log.dir="
        string_to_be_replaced=`echo "$server_properties" | grep $keyword_to_replace`
        string_to_be_replaced=${string_to_be_replaced//\//\\\/}
        string_to_replace="${keyword_to_replace}\/tmp\/kafka_server_${i}_logs"
        # info "string to be replaced : [${string_to_be_replaced}]"
        # info "string to replace     : [${string_to_replace}]"

        echo "${server_properties}" | \
          sed -e "s/${string_to_be_replaced}/${string_to_replace}/g" \
          >${this_config_dir}/server_${i}.properties

        server_properties=`cat ${this_config_dir}/server_${i}.properties`

     done
}

