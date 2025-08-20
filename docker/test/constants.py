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

KAFKA_TOPICS="fixtures/kafka/bin/kafka-topics.sh"
KAFKA_CONSOLE_PRODUCER="fixtures/kafka/bin/kafka-console-producer.sh"
KAFKA_CONSOLE_CONSUMER="fixtures/kafka/bin/kafka-console-consumer.sh"
KAFKA_RUN_CLASS="fixtures/kafka/bin/kafka-run-class.sh"

COMBINED_MODE_COMPOSE="fixtures/mode/combined/docker-compose.yml"
ISOLATED_MODE_COMPOSE="fixtures/mode/isolated/docker-compose.yml"

CLIENT_TIMEOUT=40000

SSL_FLOW_TESTS="SSL Flow Tests"
SSL_CLIENT_CONFIG="fixtures/secrets/client-ssl.properties"
SSL_TOPIC="test-topic-ssl"

FILE_INPUT_FLOW_TESTS="File Input Flow Tests"
FILE_INPUT_TOPIC="test-topic-file-input"

BROKER_RESTART_TESTS="Broker Restart Tests"
BROKER_CONTAINER="broker1"
BROKER_RESTART_TEST_TOPIC="test-topic-broker-restart"

BROKER_METRICS_TESTS="Broker Metrics Tests"
BROKER_METRICS_TEST_TOPIC="test-topic-broker-metrics"
JMX_TOOL="org.apache.kafka.tools.JmxTool"
BROKER_METRICS_HEADING='"time","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:Count","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:EventType","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:FifteenMinuteRate","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:FiveMinuteRate","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:MeanRate","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:OneMinuteRate","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:RateUnit"'

SSL_ERROR_PREFIX="SSL_ERR"
BROKER_RESTART_ERROR_PREFIX="BROKER_RESTART_ERR"
FILE_INPUT_ERROR_PREFIX="FILE_INPUT_ERR"
BROKER_METRICS_ERROR_PREFIX="BROKER_METRICS_ERR"
