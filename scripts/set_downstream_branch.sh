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

# create branch map to downstream builds
# https://github.com/confluentinc/common-tools/blob/master/confluent/config/dev/versions.json
declare -A kafkaMuckrakeVersionMap

kafkaMuckrakeVersionMap["2.3"]="5.3.x"
kafkaMuckrakeVersionMap["2.4"]="5.4.x"
kafkaMuckrakeVersionMap["2.5"]="5.5.x"
kafkaMuckrakeVersionMap["2.6"]="5.6.x"
kafkaMuckrakeVersionMap["2.7"]="5.7.x"
kafkaMuckrakeVersionMap["2.8"]="5.8.x"
kafkaMuckrakeVersionMap["3.0"]="7.0.x"
kafkaMuckrakeVersionMap["3.1"]="7.1.x"
kafkaMuckrakeVersionMap["3.2"]="7.2.x"
kafkaMuckrakeVersionMap["3.3"]="7.3.x"
kafkaMuckrakeVersionMap["3.4"]="7.4.x"
kafkaMuckrakeVersionMap["3.5"]="7.5.x"
kafkaMuckrakeVersionMap["3.6"]="7.6.x"
kafkaMuckrakeVersionMap["3.7"]="7.7.x"
kafkaMuckrakeVersionMap["trunk"]="master"
kafkaMuckrakeVersionMap["master"]="master"

export DOWNSTREAM_BRANCH_NAME=${kafkaMuckrakeVersionMap["${SEMAPHORE_GIT_BRANCH}"]}
