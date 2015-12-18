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

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M"
fi

base_dir=$(dirname $0)/..

if [ -z "$SCALA_VERSION" ]; then
        SCALA_VERSION=2.10.6
fi

if [ -z "$SCALA_BINARY_VERSION" ]; then
        SCALA_BINARY_VERSION=2.10
fi

JARPATH="$base_dir/core/build/dependant-libs-${SCALA_VERSION}/*.jar \
            $base_dir/core/build/libs/kafka_${SCALA_BINARY_VERSION}*.jar \
            $base_dir/clients/build/libs/kafka-clients*.jar" \
            exec $(dirname $0)/kafka-run-class.sh kafka.tools.ConsoleProducer $@
