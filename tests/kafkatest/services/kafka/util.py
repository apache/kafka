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

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])

def fix_opts_for_new_jvm(self, node):
    # Startup scripts for early versions of Kafka contains options
    # that not supported on latest versions of JVM like -XX:+PrintGCDateStamps or -XX:UseParNewGC.
    # When system test run on JVM that doesn't support these options
    # we should setup environment variables with correct options.
    java_version = node.account.ssh_capture("java -version 2>&1 | sed -E -n 's/.* version \"([0-9]*).*$/\1/p'")

    if java_version <= 9
        return ""

    cmd = ""
    if node.version == LATEST_0_9 or node.version == LATEST_0_8_2:
        cmd += "export KAFKA_GC_LOG_OPTS=\"-verbose:gc\"; "
    if node.version == LATEST_0_8_2:
        cmd += "export KAFKA_JVM_PERFORMANCE_OPTS=\"-server -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true\"; "
    return cmd
