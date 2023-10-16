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

SCHEMA_REGISTRY_URL="http://localhost:8081"
CONNECT_URL="http://localhost:8083/connectors"
CLIENT_TIMEOUT=40
SCHEMA_REGISTRY_TEST_TOPIC="test_topic_schema"
CONNECT_TEST_TOPIC="test_topic_connect"
CONNECT_SOURCE_CONNECTOR_CONFIG="@fixtures/source_connector.json"

SSL_TOPIC="test_topic_ssl"
SSL_CA_LOCATION="./fixtures/secrets/ca-cert"
SSL_CERTIFICATE_LOCATION="./fixtures/secrets/client_python_client.pem"
SSL_KEY_LOCATION="./fixtures/secrets/client_python_client.key"
SSL_KEY_PASSWORD="abcdefgh"

BROKER_RESTART_TEST_TOPIC="test_topic_broker_restart"

SCHEMA_REGISTRY_ERROR_PREFIX="SCHEMA_REGISTRY_ERR"
CONNECT_ERROR_PREFIX="CONNECT_ERR"
SSL_ERROR_PREFIX="SSL_ERR"
BROKER_RESTART_ERROR_PREFIX="BROKER_RESTART_ERR"