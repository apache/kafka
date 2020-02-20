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

from collections import namedtuple
from kafkatest.version import DEV_BRANCH, LATEST_0_8_2, LATEST_0_9, LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11, LATEST_1_0

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])

def fix_opts_for_new_jvm(node):
    # Startup scripts for early versions of Kafka contains options
    # that not supported on latest versions of JVM like -XX:+PrintGCDateStamps or -XX:UseParNewGC.
    # When system test run on JVM that doesn't support these options
    # we should setup environment variables with correct options.
    java_version = java_version(node)

    if int(java_version) <= 9:
        return ""

    cmd = ""
    if node.version == LATEST_0_8_2 or node.version == LATEST_0_9 or node.version == LATEST_0_10_0 or node.version == LATEST_0_10_1 or node.version == LATEST_0_10_2 or node.version == LATEST_0_11 or node.version == LATEST_1_0:
        cmd += "export KAFKA_GC_LOG_OPTS=\"-Xlog:gc*:file=/home/ducker/gc.log:time,tags:filecount=10,filesize=102400\"; "
    if node.version == LATEST_0_8_2:
        cmd += "export KAFKA_JVM_PERFORMANCE_OPTS=\"-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true\"; "

    return cmd

def java_version(node):
    # Determine java version on the node
    version = -1

    for line in node.account.ssh_capture("java -version"):
        print(line)
        if line.find("version") != -1
            print(line)

    return version


