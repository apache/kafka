/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Filter;
import org.apache.kafka.connect.transforms.predicates.RecordIsTombstone;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.PREDICATES_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;

/**
 * Integration test for preflight connector config validation
 */
@Category(IntegrationTest.class)
public class ConnectorValidationIntegrationTest {

    private static final String WORKER_GROUP_ID = "connect-worker-group-id";

    // Use a single embedded cluster for all test cases in order to cut down on runtime
    private static EmbeddedConnectCluster connect;

    @BeforeClass
    public static void setup() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(GROUP_ID_CONFIG, WORKER_GROUP_ID);

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connector-validation-connect-cluster")
                .workerProps(workerProps)
                .numBrokers(1)
                .numWorkers(1)
                .build();
        connect.start();
    }

    @AfterClass
    public static void close() {
        if (connect != null) {
            // stop all Connect, Kafka and Zk threads.
            Utils.closeQuietly(connect::stop, "Embedded Connect cluster");
        }
    }

    @Test
    public void testSinkConnectorHasNeitherTopicsListNorTopicsRegex() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.remove(TOPICS_CONFIG);
        config.remove(TOPICS_REGEX_CONFIG);
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                2, // One error each for topics list and topics regex
                "Sink connector config should fail preflight validation when neither topics list nor topics regex are provided",
                0
        );
    }

    @Test
    public void testSinkConnectorHasBothTopicsListAndTopicsRegex() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.put(TOPICS_CONFIG, "t1");
        config.put(TOPICS_REGEX_CONFIG, "r.*");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                2, // One error each for topics list and topics regex
                "Sink connector config should fail preflight validation when both topics list and topics regex are provided",
                0
        );
    }

    @Test
    public void testSinkConnectorDeadLetterQueueTopicInTopicsList() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.put(TOPICS_CONFIG, "t1");
        config.put(DLQ_TOPIC_NAME_CONFIG, "t1");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Sink connector config should fail preflight validation when DLQ topic is included in topics list",
                0
        );
    }

    @Test
    public void testSinkConnectorDeadLetterQueueTopicMatchesTopicsRegex() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.put(TOPICS_REGEX_CONFIG, "r.*");
        config.put(DLQ_TOPIC_NAME_CONFIG, "ruh.roh");
        config.remove(TOPICS_CONFIG);
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Sink connector config should fail preflight validation when DLQ topic matches topics regex",
                0
        );
    }

    @Test
    public void testSinkConnectorDefaultGroupIdConflictsWithWorkerGroupId() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        // Combined with the logic in SinkUtils::consumerGroupId, this should conflict with the worker group ID
        config.put(NAME_CONFIG, "worker-group-id");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Sink connector config should fail preflight validation when default consumer group ID conflicts with Connect worker group ID",
                0
        );
    }

    @Test
    public void testSinkConnectorOverriddenGroupIdConflictsWithWorkerGroupId() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + GROUP_ID_CONFIG, WORKER_GROUP_ID);
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Sink connector config should fail preflight validation when overridden consumer group ID conflicts with Connect worker group ID",
                0
        );
    }

    @Test
    public void testSourceConnectorHasDuplicateTopicCreationGroups() throws InterruptedException {
        Map<String, String> config = defaultSourceConnectorProps();
        config.put(TOPIC_CREATION_GROUPS_CONFIG, "g1, g2, g1");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Source connector config should fail preflight validation when the same topic creation group is specified multiple times",
                0
        );
    }

    @Test
    public void testConnectorHasDuplicateTransformations() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String transformName = "t";
        config.put(TRANSFORMS_CONFIG, transformName + ", " + transformName);
        config.put(TRANSFORMS_CONFIG + "." + transformName + ".type", Filter.class.getName());
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when the same transformation is specified multiple times",
                0
        );
    }

    @Test
    public void testConnectorHasMissingTransformClass() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String transformName = "t";
        config.put(TRANSFORMS_CONFIG, transformName);
        config.put(TRANSFORMS_CONFIG + "." + transformName + ".type", "WheresTheFruit");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when a transformation with a class not found on the worker is specified",
                0
        );
    }

    @Test
    public void testConnectorHasInvalidTransformClass() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String transformName = "t";
        config.put(TRANSFORMS_CONFIG, transformName);
        config.put(TRANSFORMS_CONFIG + "." + transformName + ".type", MonitorableSinkConnector.class.getName());
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when a transformation with a class of the wrong type is specified",
                0
        );
    }

    @Test
    public void testConnectorHasNegationForUndefinedPredicate() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String transformName = "t";
        config.put(TRANSFORMS_CONFIG, transformName);
        config.put(TRANSFORMS_CONFIG + "." + transformName + ".type", Filter.class.getName());
        config.put(TRANSFORMS_CONFIG + "." + transformName + ".negate", "true");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when an undefined predicate is negated",
                0
        );
    }

    @Test
    public void testConnectorHasDuplicatePredicates() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String predicateName = "p";
        config.put(PREDICATES_CONFIG, predicateName + ", " + predicateName);
        config.put(PREDICATES_CONFIG + "." + predicateName + ".type", RecordIsTombstone.class.getName());
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when the same predicate is specified multiple times",
                0
        );
    }

    @Test
    public void testConnectorHasMissingPredicateClass() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String predicateName = "p";
        config.put(PREDICATES_CONFIG, predicateName);
        config.put(PREDICATES_CONFIG + "." + predicateName + ".type", "WheresTheFruit");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when a predicate with a class not found on the worker is specified",
                0
        );
    }

    @Test
    public void testConnectorHasInvalidPredicateClass() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        String predicateName = "p";
        config.put(PREDICATES_CONFIG, predicateName);
        config.put(PREDICATES_CONFIG + "." + predicateName + ".type", MonitorableSinkConnector.class.getName());
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when a predicate with a class of the wrong type is specified",
                0
        );
    }

    @Test
    public void testConnectorHasMissingConverterClass() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.put(KEY_CONVERTER_CLASS_CONFIG, "WheresTheFruit");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when a converter with a class not found on the worker is specified",
                0
        );
    }

    @Test
    public void testConnectorHasMissingHeaderConverterClass() throws InterruptedException {
        Map<String, String> config = defaultSinkConnectorProps();
        config.put(HEADER_CONVERTER_CLASS_CONFIG, "WheresTheFruit");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(
                config.get(CONNECTOR_CLASS_CONFIG),
                config,
                1,
                "Connector config should fail preflight validation when a header converter with a class not found on the worker is specified",
                0
        );
    }

    private Map<String, String> defaultSourceConnectorProps() {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(NAME_CONFIG, "source-connector");
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, "t1");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

    private Map<String, String> defaultSinkConnectorProps() {
        // setup up props for the sink connector
        Map<String, String> props = new HashMap<>();
        props.put(NAME_CONFIG, "sink-connector");
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPICS_CONFIG, "t1");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

}
