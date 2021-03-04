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

base_dir=$(dirname $0)
jmh_project_name="jmh-benchmarks"

if [ ${base_dir} == "." ]; then
     gradlew_dir=".."
elif [ ${base_dir##./} == "${jmh_project_name}" ]; then
     gradlew_dir="."
else
    echo "JMH Benchmarks need to be run from the root of the kafka repository or the 'jmh-benchmarks' directory"
    exit
fi

gradleCmd="${gradlew_dir}/gradlew"
libDir="${base_dir}/build/libs"

echo "running gradlew :jmh-benchmarks:clean :jmh-benchmarks:shadowJar in quiet mode"

$gradleCmd  -q :jmh-benchmarks:clean :jmh-benchmarks:shadowJar

echo "gradle build done"

echo "running JMH with args: $@"

java -jar ${libDir}/kafka-jmh-benchmarks-*.jar "$@"

echo "JMH benchmarks done"
