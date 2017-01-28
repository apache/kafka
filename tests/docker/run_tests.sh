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
#   TC_PATHS="tests/kafkatest/tests/streams tests/kafkatest/tests/tools" bash tests/docker/run_tests.sh
set -x

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TESTS_DIR=`dirname ${SCRIPT_DIR}`
KAFKA_SRC=`dirname ${TESTS_DIR}`
KAFKA_VERSION=$(grep "version=.*" ${KAFKA_SRC}/gradle.properties | cut -f 2 -d =)
JDK_INFO="openjdk8"
KAFKA_IMAGE=${KAFKA_IMAGE:-kafkadev/kafka-image:${KAFKA_VERSION}}_${JDK_INFO}
export KAFKA_NUM_CONTAINERS=12

chmod 600 ${SCRIPT_DIR}/ssh/id_rsa
cd ${KAFKA_SRC}
(( $(ls -1 core/build/distributions/kafka_*SNAPSHOT.tgz | wc -l) != 1 )) && echo 'Expecting a single file like core/build/distributions/kafka_*SNAPSHOT.tgz, found:' $(ls -1 core/build/distributions/kafka_*SNAPSHOT.tgz) && echo "Did you run ./gradlew clean releaseTarGz ?" && exit 1

docker network rm knw
docker network create knw

docker kill $(docker ps -f=network=knw -q)
docker rm $(docker ps -a -f=network=knw -q)

docker run --rm -it ${KAFKA_IMAGE} "true"
if [[ $? != 0 || ${KAFKA_IMAGE_REBUILD} != "" ]]; then
    echo "kafka image ${KAFKA_IMAGE} does not exist. Building it from scratch."
    COMMIT_INFO=$(git describe HEAD)
    docker build -t ${KAFKA_IMAGE} --label=commit_info=${COMMIT_INFO} ${SCRIPT_DIR}
fi

echo "Using kafka image: ${KAFKA_IMAGE}"
docker inspect ${KAFKA_IMAGE}
for i in $(seq -w 1 ${KAFKA_NUM_CONTAINERS}); do
  docker run -d -t --name knode${i} --network knw -v ${KAFKA_SRC}:/kafka_src ${KAFKA_IMAGE}
done

docker info
docker ps
docker network inspect knw

for i in $(seq -w 1 ${KAFKA_NUM_CONTAINERS}); do
  echo knode${i}
  docker exec knode${i} bash -c "(tar xfz /kafka_src/core/build/distributions/kafka_*SNAPSHOT.tgz -C /opt || echo missing kafka tgz did you build kafka tarball) && mv /opt/kafka*SNAPSHOT /opt/kafka-dev && ls -l /opt"
  docker exec knode01 bash -c "ssh knode$i hostname"
done

# hack to copy test dependencies
# this is required for running MiniKDC
(cd ${KAFKA_SRC} && ./gradlew copyDependantTestLibs)
for i in $(seq -w 1 ${KAFKA_NUM_CONTAINERS}); do
  echo knode${i}
  docker exec knode${i} bash -c "cp /kafka_src/core/build/dependant-testlibs/* /opt/kafka-dev/libs/"
  docker exec knode01 bash -c "ssh knode$i hostname"
done

bash tests/cluster_file_generator.sh > tests/cluster_file.json
docker exec knode01 bash -c "cd /kafka_src; ducktape ${_DUCKTAPE_OPTIONS} --cluster-file tests/cluster_file.json ${TC_PATHS:-tests/kafkatest/tests}"
