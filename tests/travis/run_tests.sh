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

# To run tests use a command like:
#   TC_PATHS="tests/kafkatest/tests/streams tests/kafkatest/tests/tools" bash tests/travis/run_tests.sh
set -x

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TESTS_DIR=`dirname ${SCRIPT_DIR}`
KFK_SRC=`dirname ${TESTS_DIR}`


cd ${SCRIPT_DIR}
chmod 600 ssh/id_rsa

docker network rm knw
docker network create knw

docker kill $(docker ps -f=network=knw -q)
docker rm $(docker ps -a -f=network=knw -q)

for i in $(seq -w 1 12); do
  docker run -d -t --name knode${i} --network knw -v ${KFK_SRC}:/kfk_src raghavgautam/kfk-image
done

docker info
docker ps
docker network inspect knw

for i in $(seq -w 1 12); do
  echo knode${i}
  docker exec knode${i} bash -c "(tar xfz /kfk_src/core/build/distributions/kafka_*SNAPSHOT.tgz -C /opt || echo missing kafka tgz did you build kafka tarball) && mv /opt/kafka*SNAPSHOT /opt/kafka-trunk && ls -l /opt"
  docker exec knode01 bash -c "ssh knode$i hostname"
done

# hack to copy test dependencies
# this is required for running MiniKDC
(cd ${KFK_SRC} && ./gradlew copyDependantTestLibs)
for i in $(seq -w 1 12); do
  echo knode${i}
  docker exec knode${i} bash -c "cp /kfk_src/core/build/dependant-testlibs/* /opt/kafka-trunk/libs/"
  docker exec knode01 bash -c "ssh knode$i hostname"
done

docker exec knode01 bash -c "cd /kfk_src; ducktape ${_DUCKTAPE_OPTIONS} --cluster-file tests/cluster_file.json ${TC_PATHS:-tests/kafkatest/tests}"
