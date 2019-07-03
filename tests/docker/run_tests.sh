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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KAFKA_NUM_CONTAINERS=${KAFKA_NUM_CONTAINERS:-14}
TC_PATHS=${TC_PATHS:-./kafkatest/}

die() {
    echo $@
    exit 1
}

if ${SCRIPT_DIR}/ducker-ak ssh | grep -q '(none)'; then
    ${SCRIPT_DIR}/ducker-ak up -n "${KAFKA_NUM_CONTAINERS}" || die "ducker-ak up failed"
fi
${SCRIPT_DIR}/ducker-ak test ${TC_PATHS} ${_DUCKTAPE_OPTIONS} || die "ducker-ak test failed"
