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

usage() {
    cat <<EOF
The Trogdor fault injector.

Usage:
  $0 [action] [options]

Actions:
  agent: Run the trogdor agent.
  coordinator: Run the trogdor coordinator.
  client: Run the client which communicates with the trogdor coordinator.
  agent-client: Run the client which communicates with the trogdor agent.
  help: This help message.
EOF
}

if [[ $# -lt 1 ]]; then
    usage
    exit 0
fi
action="${1}"
shift
CLASS=""
case ${action} in
    agent) CLASS="org.apache.kafka.trogdor.agent.Agent";;
    coordinator) CLASS="org.apache.kafka.trogdor.coordinator.Coordinator";;
    client) CLASS="org.apache.kafka.trogdor.coordinator.CoordinatorClient";;
    agent-client) CLASS="org.apache.kafka.trogdor.agent.AgentClient";;
    help) usage; exit 0;;
    *)  echo "Unknown action '${action}'.  Type '$0 help' for help."; exit 1;;
esac

export INCLUDE_TEST_JARS=1
exec $(dirname $0)/kafka-run-class.sh "${CLASS}" "$@"
