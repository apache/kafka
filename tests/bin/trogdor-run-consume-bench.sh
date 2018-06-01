#!/usr/bin/env bash
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

COORDINATOR_ENDPOINT="localhost:8889"
TASK_ID="consume_bench_$RANDOM"
TASK_SPEC=$(
cat <<EOF
{
    "id": "$TASK_ID",
    "spec": {
        "class": "org.apache.kafka.trogdor.workload.ConsumeBenchSpec",
        "durationMs": 10000000,
        "consumerNode": "node0",
        "bootstrapServers": "localhost:9092",
        "maxMessages": 100,
        "activeTopics": {
            "foo[1-3]": {
                "numPartitions": 3,
                "replicationFactor": 1
            }
        }
    }
}
EOF
)

./bin/trogdor.sh client --create-task "${TASK_SPEC}" "${COORDINATOR_ENDPOINT}"
echo "\$TASK_ID = $TASK_ID"
