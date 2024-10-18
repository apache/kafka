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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.ConnectAssertions;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.NoRetryException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.CUSTOM_TRANSACTION_BOUNDARIES_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.MAX_MESSAGES_PER_SECOND_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.MESSAGES_PER_POLL_CONFIG;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.OFFSETS_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TRANSACTION_BOUNDARY_INTERVAL_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.CONNECTOR;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.INTERVAL;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.POLL;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class ExactlyOnceSourceIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceSourceIntegrationTest.class);
    private static final String CLUSTER_GROUP_ID = "exactly-once-source-integration-test";
    private static final String CONNECTOR_NAME = "exactlyOnceQuestionMark";

    private static final int CONSUME_RECORDS_TIMEOUT_MS = 60_000;
    private static final int SOURCE_TASK_PRODUCE_TIMEOUT_MS = 30_000;
    private static final int ACL_PROPAGATION_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_NUM_WORKERS = 3;

    // Tests require that a minimum but not unreasonably large number of records are sourced.
    // Throttle the poll such that a reasonable amount of records are produced while the test runs.
    private static final int MINIMUM_MESSAGES = 100;
    private static final String MESSAGES_PER_POLL = Integer.toString(MINIMUM_MESSAGES);
    private static final String MESSAGES_PER_SECOND = Long.toString(MINIMUM_MESSAGES / 2);

    private Properties brokerProps;
    private Map<String, String> workerProps;
    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @BeforeEach
    public void setup() {
        workerProps = new HashMap<>();
        workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, CLUSTER_GROUP_ID);

        brokerProps = new Properties();
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");

        // build a Connect cluster backed by Kafka
        connectBuilder = new EmbeddedConnectCluster.Builder()
                .numWorkers(DEFAULT_NUM_WORKERS)
                .numBrokers(1)
                .workerProps(workerProps)
                .brokerProps(brokerProps);

        // get a handle to the connector
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    private void startConnect() {
        connect = connectBuilder.build();
        connect.start();
    }

    @AfterEach
    public void close() {
        try {
            // stop the Connect cluster and its backing Kafka cluster.
            connect.stop();
        } finally {
            // Clear the handle for the connector. Fun fact: if you don't do this, your tests become quite flaky.
            RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);
        }
    }

    /**
     * A simple test for the pre-flight validation API for connectors to provide their own guarantees for exactly-once semantics.
     */
    @Test
    public void testPreflightValidation() {
        connectBuilder.numWorkers(1);
        startConnect();

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, "topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);

        // Test out the "exactly.once.support" property
        props.put(EXACTLY_ONCE_SUPPORT_CONFIG, "required");

        // Connector will return null from SourceConnector::exactlyOnceSupport
        props.put(CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG, MonitorableSourceConnector.EXACTLY_ONCE_NULL);
        ConfigInfos validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(1, validation.errorCount(),
                "Preflight validation should have exactly one error");
        ConfigInfo propertyValidation = findConfigInfo(EXACTLY_ONCE_SUPPORT_CONFIG, validation);
        assertFalse(propertyValidation.configValue().errors().isEmpty(),
                "Preflight validation for exactly-once support property should have at least one error message");

        // Connector will return UNSUPPORTED from SourceConnector::exactlyOnceSupport
        props.put(CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG, MonitorableSourceConnector.EXACTLY_ONCE_UNSUPPORTED);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(1, validation.errorCount(), "Preflight validation should have exactly one error");
        propertyValidation = findConfigInfo(EXACTLY_ONCE_SUPPORT_CONFIG, validation);
        assertFalse(propertyValidation.configValue().errors().isEmpty(), 
                "Preflight validation for exactly-once support property should have at least one error message");

        // Connector will throw an exception from SourceConnector::exactlyOnceSupport
        props.put(CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG, MonitorableSourceConnector.EXACTLY_ONCE_FAIL);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(1, validation.errorCount(), "Preflight validation should have exactly one error");
        propertyValidation = findConfigInfo(EXACTLY_ONCE_SUPPORT_CONFIG, validation);
        assertFalse(propertyValidation.configValue().errors().isEmpty(),
                "Preflight validation for exactly-once support property should have at least one error message");

        // Connector will return SUPPORTED from SourceConnector::exactlyOnceSupport
        props.put(CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG, MonitorableSourceConnector.EXACTLY_ONCE_SUPPORTED);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(0, validation.errorCount(), "Preflight validation should have zero errors");

        // Test out the transaction boundary definition property
        props.put(TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        // Connector will return null from SourceConnector::canDefineTransactionBoundaries
        props.put(CUSTOM_TRANSACTION_BOUNDARIES_CONFIG, MonitorableSourceConnector.TRANSACTION_BOUNDARIES_NULL);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(1, validation.errorCount(), "Preflight validation should have exactly one error");
        propertyValidation = findConfigInfo(TRANSACTION_BOUNDARY_CONFIG, validation);
        assertFalse(propertyValidation.configValue().errors().isEmpty(),
                "Preflight validation for transaction boundary property should have at least one error message");

        // Connector will return UNSUPPORTED from SourceConnector::canDefineTransactionBoundaries
        props.put(CUSTOM_TRANSACTION_BOUNDARIES_CONFIG, MonitorableSourceConnector.TRANSACTION_BOUNDARIES_UNSUPPORTED);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(1, validation.errorCount(), "Preflight validation should have exactly one error");
        propertyValidation = findConfigInfo(TRANSACTION_BOUNDARY_CONFIG, validation);
        assertFalse(propertyValidation.configValue().errors().isEmpty(),
                "Preflight validation for transaction boundary property should have at least one error message");

        // Connector will throw an exception from SourceConnector::canDefineTransactionBoundaries
        props.put(CUSTOM_TRANSACTION_BOUNDARIES_CONFIG, MonitorableSourceConnector.TRANSACTION_BOUNDARIES_FAIL);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(1, validation.errorCount(), "Preflight validation should have exactly one error");
        propertyValidation = findConfigInfo(TRANSACTION_BOUNDARY_CONFIG, validation);
        assertFalse(propertyValidation.configValue().errors().isEmpty(), 
                "Preflight validation for transaction boundary property should have at least one error message");

        // Connector will return SUPPORTED from SourceConnector::canDefineTransactionBoundaries
        props.put(CUSTOM_TRANSACTION_BOUNDARIES_CONFIG, MonitorableSourceConnector.TRANSACTION_BOUNDARIES_SUPPORTED);
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals(0, validation.errorCount(), "Preflight validation should have zero errors");
    }

    /**
     * A simple green-path test that ensures the worker can start up a source task with exactly-once support enabled
     * and write some records to Kafka that will be visible to a downstream consumer using the "READ_COMMITTED"
     * isolation level. The "poll" transaction boundary is used.
     */
    @Test
    public void testPollBoundary() throws Exception {
        // Much slower offset commit interval; should never be triggered during this test
        workerProps.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "600000");
        connectBuilder.numWorkers(1);
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        int numTasks = 1;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, Integer.toString(numTasks));
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, POLL.toString());
        props.put(MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL);
        props.put(MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(MINIMUM_MESSAGES);
        connectorHandle.expectedCommits(MINIMUM_MESSAGES);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        StartAndStopLatch connectorStop = connectorHandle.expectedStops(1, true);
        connect.deleteConnector(CONNECTOR_NAME);
        assertConnectorStopped(connectorStop);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        ConsumerRecords<byte[], byte[]> records = connect.kafka().consumeAll(
                CONSUME_RECORDS_TIMEOUT_MS,
                Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                null,
                topic
        );
        assertTrue(records.count() >= MINIMUM_MESSAGES,
                "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + records.count());
        assertExactlyOnceSeqnos(records, numTasks);
    }

    /**
     * A simple green-path test that ensures the worker can start up a source task with exactly-once support enabled
     * and write some records to Kafka that will be visible to a downstream consumer using the "READ_COMMITTED"
     * isolation level. The "interval" transaction boundary is used with a connector-specific override.
     */
    @Test
    public void testIntervalBoundary() throws Exception {
        // Much slower offset commit interval; should never be triggered during this test
        workerProps.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "600000");
        connectBuilder.numWorkers(1);
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        int numTasks = 1;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, Integer.toString(numTasks));
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, INTERVAL.toString());
        props.put(TRANSACTION_BOUNDARY_INTERVAL_CONFIG, "10000");
        props.put(MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL);
        props.put(MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(MINIMUM_MESSAGES);
        connectorHandle.expectedCommits(MINIMUM_MESSAGES);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        StartAndStopLatch connectorStop = connectorHandle.expectedStops(1, true);
        connect.deleteConnector(CONNECTOR_NAME);
        assertConnectorStopped(connectorStop);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        ConsumerRecords<byte[], byte[]> records = connect.kafka().consumeAll(
                CONSUME_RECORDS_TIMEOUT_MS,
                Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                null,
                topic
        );
        assertTrue(records.count() >= MINIMUM_MESSAGES,
                "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + records.count());
        assertExactlyOnceSeqnos(records, numTasks);
    }

    /**
     * A simple green-path test that ensures the worker can start up a source task with exactly-once support enabled
     * and write some records to Kafka that will be visible to a downstream consumer using the "READ_COMMITTED"
     * isolation level. The "connector" transaction boundary is used with a connector that defines transactions whose
     * size correspond to successive elements of the Fibonacci sequence, where transactions with an even number of
     * records are aborted, and those with an odd number of records are committed.
     */
    @Test
    public void testConnectorBoundary() throws Exception {
        String offsetsTopic = "exactly-once-source-cluster-offsets";
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, offsetsTopic);
        connectBuilder.numWorkers(1);
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());
        props.put(CUSTOM_TRANSACTION_BOUNDARIES_CONFIG, MonitorableSourceConnector.TRANSACTION_BOUNDARIES_SUPPORTED);
        props.put(MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL);
        props.put(MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);

        // the connector aborts some transactions, which causes records that it has emitted (and for which
        // SourceTask::commitRecord has been invoked) to be invisible to consumers; we expect the task to
        // emit at most 233 records in total before 100 records have been emitted as part of one or more
        // committed transactions
        connectorHandle.expectedRecords(233);
        connectorHandle.expectedCommits(233);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // consume all records from the source topic or fail, to ensure that they were correctly produced
        ConsumerRecords<byte[], byte[]> sourceRecords = connect.kafka().consumeAll(
            CONSUME_RECORDS_TIMEOUT_MS,
            Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
            null,
            topic
        );
        assertTrue(sourceRecords.count() >= MINIMUM_MESSAGES,
                "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + sourceRecords.count());

        // also consume from the cluster's offsets topic to verify that the expected offsets (which should correspond to the connector's
        // custom transaction boundaries) were committed
        List<Long> expectedOffsetSeqnos = new ArrayList<>();
        long lastExpectedOffsetSeqno = 1;
        long nextExpectedOffsetSeqno = 1;
        while (nextExpectedOffsetSeqno <= MINIMUM_MESSAGES) {
            expectedOffsetSeqnos.add(nextExpectedOffsetSeqno);
            nextExpectedOffsetSeqno += lastExpectedOffsetSeqno;
            lastExpectedOffsetSeqno = nextExpectedOffsetSeqno - lastExpectedOffsetSeqno;
        }
        ConsumerRecords<byte[], byte[]> offsetRecords = connect.kafka()
                .consume(
                        expectedOffsetSeqnos.size(),
                        TimeUnit.MINUTES.toMillis(1),
                        consumerProps,
                        offsetsTopic
                );

        List<Long> actualOffsetSeqnos = parseAndAssertOffsetsForSingleTask(offsetRecords);

        assertEquals(expectedOffsetSeqnos, actualOffsetSeqnos.subList(0, expectedOffsetSeqnos.size()),
                "Committed offsets should match connector-defined transaction boundaries");

        List<Long> expectedRecordSeqnos = LongStream.range(1, MINIMUM_MESSAGES + 1).boxed().collect(Collectors.toList());
        long priorBoundary = 1;
        long nextBoundary = 2;
        while (priorBoundary < expectedRecordSeqnos.get(expectedRecordSeqnos.size() - 1)) {
            if (nextBoundary % 2 == 0) {
                for (long i = priorBoundary + 1; i < nextBoundary + 1; i++) {
                    expectedRecordSeqnos.remove(i);
                }
            }
            nextBoundary += priorBoundary;
            priorBoundary = nextBoundary - priorBoundary;
        }
        List<Long> actualRecordSeqnos = parseAndAssertValuesForSingleTask(sourceRecords);
        // Have to sort the records by seqno since we produce to multiple partitions and in-order consumption isn't guaranteed
        Collections.sort(actualRecordSeqnos);
        assertEquals(expectedRecordSeqnos, actualRecordSeqnos.subList(0, expectedRecordSeqnos.size()),
                "Committed records should exclude connector-aborted transactions");
    }

    /**
     * Brings up a one-node cluster, then intentionally fences out the transactional producer used by the leader
     * for writes to the config topic to simulate a zombie leader being active in the cluster. The leader should
     * automatically recover, verify that it is still the leader, and then succeed to create a connector when the
     * user resends the request.
     */
    @Test
    public void testFencedLeaderRecovery() throws Exception {
        connectBuilder.numWorkers(1);
        // Much slower offset commit interval; should never be triggered during this test
        workerProps.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "600000");
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        int numTasks = 1;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, Integer.toString(numTasks));
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, POLL.toString());
        props.put(MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL);
        props.put(MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(MINIMUM_MESSAGES);
        connectorHandle.expectedCommits(MINIMUM_MESSAGES);

        // fence out the leader of the cluster
        Producer<?, ?> zombieLeader = transactionalProducer(
                "simulated-zombie-leader",
                DistributedConfig.transactionalProducerId(CLUSTER_GROUP_ID)
        );
        zombieLeader.initTransactions();
        zombieLeader.close();

        // start a source connector--should fail the first time
        assertThrows(ConnectRestException.class, () -> connect.configureConnector(CONNECTOR_NAME, props));

        // the second request should succeed because the leader has reclaimed write privileges for the config topic
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        StartAndStopLatch connectorStop = connectorHandle.expectedStops(1, true);
        connect.deleteConnector(CONNECTOR_NAME);
        assertConnectorStopped(connectorStop);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        ConsumerRecords<byte[], byte[]> records = connect.kafka().consumeAll(
                CONSUME_RECORDS_TIMEOUT_MS,
                Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                null,
                topic
        );
        assertTrue(records.count() >= MINIMUM_MESSAGES,
                "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + records.count());
        assertExactlyOnceSeqnos(records, numTasks);
    }

    /**
     * A moderately-complex green-path test that ensures the worker can start up and run tasks for a source
     * connector that gets reconfigured, and will fence out potential zombie tasks for older generations before
     * bringing up new task instances.
     */
    @Test
    public void testConnectorReconfiguration() throws Exception {
        // Much slower offset commit interval; should never be triggered during this test
        workerProps.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "600000");
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL);
        props.put(MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(MINIMUM_MESSAGES);
        connectorHandle.expectedCommits(MINIMUM_MESSAGES);

        StartAndStopLatch connectorStart = connectorAndTaskStart(3);
        props.put(TASKS_MAX_CONFIG, "3");
        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorStarted(connectorStart);

        assertProducersAreFencedOnReconfiguration(3, 5, topic, props);
        assertProducersAreFencedOnReconfiguration(5, 1, topic, props);
        assertProducersAreFencedOnReconfiguration(1, 5, topic, props);
        assertProducersAreFencedOnReconfiguration(5, 3, topic, props);

        // Do a final sanity check to make sure that the last generation of tasks is able to run
        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        StartAndStopLatch connectorStop = connectorHandle.expectedStops(1, true);
        connect.deleteConnector(CONNECTOR_NAME);
        assertConnectorStopped(connectorStop);

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        ConsumerRecords<byte[], byte[]> records = connect.kafka().consumeAll(
                CONSUME_RECORDS_TIMEOUT_MS,
                Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                null,
                topic
        );
        assertTrue(records.count() >= MINIMUM_MESSAGES,
                "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + records.count());
        // We used at most five tasks during the tests; each of them should have been able to produce records
        assertExactlyOnceSeqnos(records, 5);
    }

    /**
     * This test ensures that tasks are marked failed in the status API when the round of
     * zombie fencing that takes place before they are brought up fails. In addition, once
     * the issue with the connector config that made fencing impossible is rectified, tasks
     * can be successfully restarted.
     * <p>
     * Fencing failures are induced by bringing up an ACL-secured Kafka cluster and creating
     * a connector whose principal is not authorized to access the transactional IDs that Connect
     * uses for its tasks.
     * <p>
     * When the connector is initially brought up, no fencing is necessary. However, once it is
     * reconfigured and generates new task configs, a round of zombie fencing is triggered,
     * and all of its tasks fail when that round of zombie fencing fails.
     * <p>
     * After, the connector's principal is granted access to the necessary transactional IDs,
     * all of its tasks are restarted, and we verify that they are able to come up successfully
     * this time.
     */
    @Test
    public void testTasksFailOnInabilityToFence() throws Exception {
        brokerProps.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");
        brokerProps.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
        brokerProps.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, "PLAIN");
        brokerProps.put(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, "PLAIN");
        brokerProps.put(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG, "PLAIN");
        String listenerSaslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"super\" "
                + "password=\"super_pwd\" "
                + "user_connector=\"connector_pwd\" "
                + "user_super=\"super_pwd\";";
        brokerProps.put("listener.name.external.plain.sasl.jaas.config", listenerSaslJaasConfig);
        brokerProps.put("listener.name.controller.plain.sasl.jaas.config", listenerSaslJaasConfig);
        brokerProps.put("super.users", "User:super");

        Map<String, String> superUserClientConfig = new HashMap<>();
        superUserClientConfig.put("sasl.mechanism", "PLAIN");
        superUserClientConfig.put("security.protocol", "SASL_PLAINTEXT");
        superUserClientConfig.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"super\" "
                        + "password=\"super_pwd\";");
        // Give the worker super-user privileges
        workerProps.putAll(superUserClientConfig);

        final String globalOffsetsTopic = "connect-worker-offsets-topic";
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, globalOffsetsTopic);

        connectBuilder.clientProps(superUserClientConfig);

        startConnect();

        String topic = "test-topic";
        try (Admin admin = connect.kafka().createAdminClient()) {
            admin.createTopics(Collections.singleton(new NewTopic(topic, 3, (short) 1))).all().get();
        }

        Map<String, String> props = new HashMap<>();
        int tasksMax = 2; // Use two tasks since single-task connectors don't require zombie fencing
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));
        // Give the connectors' consumer and producer super-user privileges
        superUserClientConfig.forEach((property, value) -> {
            props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + property, value);
            props.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + property, value);
        });
        // But limit its admin client's privileges
        props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + "sasl.mechanism", "PLAIN");
        props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + "security.protocol", "SASL_PLAINTEXT");
        props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"connector\" "
                        + "password=\"connector_pwd\";");
        // Grant the connector's admin permissions to access the topics for its records and offsets
        // Intentionally leave out permissions required for fencing
        try (Admin admin = connect.kafka().createAdminClient()) {
            admin.createAcls(Arrays.asList(
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                            new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                    ),
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, globalOffsetsTopic, PatternType.LITERAL),
                            new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                    )
            )).all().get();
        }

        log.info("Bringing up connector with fresh slate; fencing should not be necessary");
        connect.configureConnector(CONNECTOR_NAME, props);

        // Hack: There is a small chance that our recent ACL updates for the connector have
        // not yet been propagated across the entire Kafka cluster, and that our connector
        // will fail on startup when it tries to list the end offsets of the worker's offsets topic
        // So, we implement some retry logic here to add a layer of resiliency in that case
        waitForCondition(
                () -> {
                    ConnectorStateInfo status = connect.connectorStatus(CONNECTOR_NAME);
                    if ("RUNNING".equals(status.connector().state())) {
                        return true;
                    } else if ("FAILED".equals(status.connector().state())) {
                        log.debug("Restarting failed connector {}", CONNECTOR_NAME);
                        connect.restartConnector(CONNECTOR_NAME);
                    }
                    return false;
                },
                ACL_PROPAGATION_TIMEOUT_MS,
                "Connector was not able to start in time, "
                        + "or ACL updates were not propagated across the Kafka cluster soon enough"
        );

        // Also verify that the connector's tasks have been able to start successfully
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, tasksMax, "Connector and task should have started successfully");

        log.info("Reconfiguring connector; fencing should be necessary, and tasks should fail to start");
        props.put("message.in.a.bottle", "19e184427ac45bd34c8588a4e771aa1a");
        connect.configureConnector(CONNECTOR_NAME, props);

        // Verify that the task has failed, and that the failure is visible to users via the REST API
        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(CONNECTOR_NAME, tasksMax, "Task should have failed on startup");

        // Now grant the necessary permissions for fencing to the connector's admin
        try (Admin admin = connect.kafka().createAdminClient()) {
            admin.createAcls(Arrays.asList(
                    new AclBinding(
                            new ResourcePattern(ResourceType.TRANSACTIONAL_ID, Worker.taskTransactionalId(CLUSTER_GROUP_ID, CONNECTOR_NAME, 0), PatternType.LITERAL),
                            new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                    ),
                    new AclBinding(
                            new ResourcePattern(ResourceType.TRANSACTIONAL_ID, Worker.taskTransactionalId(CLUSTER_GROUP_ID, CONNECTOR_NAME, 1), PatternType.LITERAL),
                            new AccessControlEntry("User:connector", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                    )
            ));
        }

        log.info("Restarting connector after tweaking its ACLs; fencing should succeed this time");
        connect.restartConnectorAndTasks(CONNECTOR_NAME, false, true, false);

        // Verify that the connector and its tasks have been able to restart successfully
        // Use the same retry logic as above, in case there is a delay in the propagation of our ACL updates
        waitForCondition(
                () -> {
                    ConnectorStateInfo status = connect.connectorStatus(CONNECTOR_NAME);
                    boolean connectorRunning = "RUNNING".equals(status.connector().state());
                    boolean allTasksRunning = status.tasks().stream()
                            .allMatch(t -> "RUNNING".equals(t.state()));
                    boolean expectedNumTasks = status.tasks().size() == tasksMax;
                    if (connectorRunning && allTasksRunning && expectedNumTasks) {
                        return true;
                    } else {
                        if (!connectorRunning) {
                            if ("FAILED".equals(status.connector().state())) {
                                // Only task failures are expected ;if the connector has failed, something
                                // else is wrong and we should fail the test immediately
                                throw new NoRetryException(
                                        new AssertionError("Connector " + CONNECTOR_NAME + " has failed unexpectedly")
                                );
                            }
                        }
                        // Restart all failed tasks
                        status.tasks().stream()
                                .filter(t -> "FAILED".equals(t.state()))
                                .map(ConnectorStateInfo.TaskState::id)
                                .forEach(t -> connect.restartTask(CONNECTOR_NAME, t));
                        return false;
                    }
                },
                ConnectAssertions.CONNECTOR_SETUP_DURATION_MS,
                "Connector and task should have restarted successfully"
        );
    }

    /**
     * This test focuses extensively on the per-connector offsets feature.
     * <p>
     * First, a connector is brought up whose producer is configured to write to a different Kafka cluster
     * than the one the Connect cluster users for its internal topics, then the contents of the connector's
     * dedicated offsets topic and the worker's internal offsets topic are inspected to ensure that offsets
     * have been backed up from the dedicated topic to the global topic.
     * <p>
     * Then, a "soft downgrade" is simulated: the Connect cluster is shut down and reconfigured to disable
     * exactly-once support. The cluster is brought up again, the connector is allowed to produce some data,
     * the connector is shut down, and this time, the records the connector has produced are inspected for
     * accuracy. Because of the downgrade, exactly-once semantics are lost, but we check to make sure that
     * the task has maintained exactly-once semantics <i>up to the last-committed record</i>.
     */
    @Test
    public void testSeparateOffsetsTopic() throws Exception {
        final String globalOffsetsTopic = "connect-worker-offsets-topic";
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, globalOffsetsTopic);

        startConnect();

        int numConnectorTargetedBrokers = 1;
        EmbeddedKafkaCluster connectorTargetedCluster = new EmbeddedKafkaCluster(numConnectorTargetedBrokers, brokerProps);
        try (Closeable clusterShutdown = connectorTargetedCluster::stop) {
            connectorTargetedCluster.start();
            // Wait for the connector-targeted Kafka cluster to get on its feet
            waitForCondition(
                    () -> connectorTargetedCluster.runningBrokers().size() == numConnectorTargetedBrokers,
                    ConnectAssertions.WORKER_SETUP_DURATION_MS,
                    "Separate Kafka cluster did not start in time"
            );

            String topic = "test-topic";
            connectorTargetedCluster.createTopic(topic, 3);

            int numTasks = 1;

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
            props.put(TASKS_MAX_CONFIG, Integer.toString(numTasks));
            props.put(TOPIC_CONFIG, topic);
            props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
            props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
            props.put(NAME_CONFIG, CONNECTOR_NAME);
            props.put(TRANSACTION_BOUNDARY_CONFIG, POLL.toString());
            props.put(MESSAGES_PER_POLL_CONFIG, MESSAGES_PER_POLL);
            props.put(MAX_MESSAGES_PER_SECOND_CONFIG, MESSAGES_PER_SECOND);
            props.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, connectorTargetedCluster.bootstrapServers());
            props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, connectorTargetedCluster.bootstrapServers());
            props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, connectorTargetedCluster.bootstrapServers());
            String offsetsTopic = CONNECTOR_NAME + "-offsets";
            props.put(OFFSETS_TOPIC_CONFIG, offsetsTopic);

            // expect all records to be consumed and committed by the connector
            connectorHandle.expectedRecords(MINIMUM_MESSAGES);
            connectorHandle.expectedCommits(MINIMUM_MESSAGES);

            // start a source connector
            connect.configureConnector(CONNECTOR_NAME, props);
            connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                    CONNECTOR_NAME,
                    numTasks,
                    "connector and tasks did not start in time"
            );

            log.info("Waiting for records to be provided to worker by task");
            // wait for the connector tasks to produce enough records
            connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

            log.info("Waiting for records to be committed to Kafka by worker");
            // wait for the connector tasks to commit enough records
            connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

            // consume at least the expected number of records from the source topic or fail, to ensure that they were correctly produced
            int recordNum = connectorTargetedCluster
                    .consume(
                        MINIMUM_MESSAGES,
                            TimeUnit.MINUTES.toMillis(1),
                            Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                            "test-topic")
                    .count();
            assertTrue(recordNum >= MINIMUM_MESSAGES,
                    "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + recordNum);

            // also consume from the connector's dedicated offsets topic
            ConsumerRecords<byte[], byte[]> offsetRecords = connectorTargetedCluster
                    .consumeAll(
                            TimeUnit.MINUTES.toMillis(1),
                            Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                            null,
                            offsetsTopic
                    );
            List<Long> seqnos = parseAndAssertOffsetsForSingleTask(offsetRecords);
            seqnos.forEach(seqno ->
                assertEquals(0, seqno % MINIMUM_MESSAGES,
                        "Offset commits should occur on connector-defined poll boundaries, which happen every " + MINIMUM_MESSAGES + " records")
            );

            // also consume from the cluster's global offsets topic
            offsetRecords = connect.kafka().consumeAll(TimeUnit.MINUTES.toMillis(1), globalOffsetsTopic);
            seqnos = parseAndAssertOffsetsForSingleTask(offsetRecords);
            seqnos.forEach(seqno ->
                assertEquals(0, seqno % MINIMUM_MESSAGES,
                        "Offset commits should occur on connector-defined poll boundaries, which happen every " + MINIMUM_MESSAGES + " records")
            );

            // Shut down the whole cluster
            connect.workers().forEach(connect::removeWorker);
            // Reconfigure the cluster with exactly-once support disabled
            workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "disabled");

            // Establish new expectations for records+offsets
            connectorHandle.expectedRecords(MINIMUM_MESSAGES);
            connectorHandle.expectedCommits(MINIMUM_MESSAGES);

            // Restart the whole cluster
            for (int i = 0; i < DEFAULT_NUM_WORKERS; i++) {
                connect.addWorker();
            }

            // And perform a basic sanity check that the cluster is able to come back up, our connector and its task are able to resume running,
            // and the task is still able to produce source records and commit offsets
            connect.assertions().assertAtLeastNumWorkersAreUp(DEFAULT_NUM_WORKERS, "cluster did not restart in time");
            connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                    CONNECTOR_NAME,
                    1,
                    "connector and tasks did not resume running after cluster restart in time"
            );

            log.info("Waiting for records to be provided to worker by task");
            // wait for the connector tasks to produce enough records
            connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

            log.info("Waiting for records to be committed to Kafka by worker");
            // wait for the connector tasks to commit enough records
            connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

            StartAndStopLatch connectorStop = connectorHandle.expectedStops(1, true);
            connect.deleteConnector(CONNECTOR_NAME);
            assertConnectorStopped(connectorStop);

            // consume all records from the source topic or fail, to ensure that they were correctly produced
            ConsumerRecords<byte[], byte[]> sourceRecords = connectorTargetedCluster.consumeAll(
                    CONSUME_RECORDS_TIMEOUT_MS,
                    Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                    null,
                    topic
            );
            assertTrue(sourceRecords.count() >= MINIMUM_MESSAGES,
                    "Not enough records produced by source connector. Expected at least: " + MINIMUM_MESSAGES + " + but got " + sourceRecords.count());
            // also have to check which offsets have actually been committed, since we no longer have exactly-once semantics
            offsetRecords = connectorTargetedCluster.consumeAll(
                    CONSUME_RECORDS_TIMEOUT_MS,
                    Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                    null,
                    offsetsTopic
            );
            assertAtLeastOnceSeqnos(sourceRecords, offsetRecords, numTasks);
        }
    }

    /**
     * A simple test to ensure that source tasks fail when trying to produce to their own offsets topic.
     * <p>
     * We fail the tasks in order to prevent deadlock that occurs when:
     * <ol>
     *     <li>
     *         A task provides a record whose topic is the task's offsets topic
     *     </li>
     *     <li>
     *         That record is dispatched to the task's producer in a transaction that remains open
     *         at least until the worker polls the task again
     *     </li>
     *     <li>
     *         In the subsequent call to SourceTask::poll, the task requests offsets from the worker
     *         (which requires a read to the end of the offsets topic, and will block until any open
     *         transactions on the topic are either committed or aborted)
     *     </li>
     * </ol>
     */
    @Test
    public void testPotentialDeadlockWhenProducingToOffsetsTopic() throws Exception {
        connectBuilder.numWorkers(1);
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        Map<String, String> props = new HashMap<>();
        // See below; this connector does nothing except request offsets from the worker in SourceTask::poll
        // and then return a single record targeted at its offsets topic
        props.put(CONNECTOR_CLASS_CONFIG, NaughtyConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, INTERVAL.toString());
        props.put(OFFSETS_TOPIC_CONFIG, "whoops");

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
            CONNECTOR_NAME, 1, "Task should have failed after trying to produce to its own offsets topic");
    }

    private ConfigInfo findConfigInfo(String property, ConfigInfos validationResult) {
        return validationResult.values().stream()
                .filter(info -> property.equals(info.configKey().name()))
                .findAny()
                .orElseThrow(() -> new AssertionError("Failed to find configuration validation result for property '" + property + "'"));
    }

    private List<Long> parseAndAssertOffsetsForSingleTask(ConsumerRecords<byte[], byte[]> offsetRecords) {
        Map<Integer, List<Long>> parsedOffsets = parseOffsetForTasks(offsetRecords);
        assertEquals(Collections.singleton(0), parsedOffsets.keySet(), "Expected records to only be produced from a single task");
        return parsedOffsets.get(0);
    }

    private List<Long> parseAndAssertValuesForSingleTask(ConsumerRecords<byte[], byte[]> sourceRecords) {
        Map<Integer, List<Long>> parsedValues = parseValuesForTasks(sourceRecords);
        assertEquals(Collections.singleton(0), parsedValues.keySet(), "Expected records to only be produced from a single task");
        return parsedValues.get(0);
    }

    private void assertExactlyOnceSeqnos(ConsumerRecords<byte[], byte[]> sourceRecords, int numTasks) {
        Map<Integer, List<Long>> parsedValues = parseValuesForTasks(sourceRecords);
        assertSeqnos(parsedValues, numTasks);
    }

    private void assertAtLeastOnceSeqnos(ConsumerRecords<byte[], byte[]> sourceRecords, ConsumerRecords<byte[], byte[]> offsetRecords, int numTasks) {
        Map<Integer, List<Long>> parsedValues = parseValuesForTasks(sourceRecords);
        Map<Integer, Long> lastCommittedValues = parseOffsetForTasks(offsetRecords)
                .entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Collections.max(e.getValue())
                ));
        parsedValues.replaceAll((task, values) -> {
            Long committedValue = lastCommittedValues.get(task);
            assertNotNull(committedValue, "No committed offset found for task " + task);
            return values.stream().filter(v -> v <= committedValue).collect(Collectors.toList());
        });
        assertSeqnos(parsedValues, numTasks);
    }

    private void assertSeqnos(Map<Integer, List<Long>> parsedValues, int numTasks) {
        Set<Integer> expectedKeys = IntStream.range(0, numTasks).boxed().collect(Collectors.toSet());
        assertEquals(expectedKeys, parsedValues.keySet(), "Expected records to be produced by each task");

        parsedValues.forEach((taskId, seqnos) -> {
            // We don't check for order here because the records may have been produced to multiple topic partitions,
            // which makes in-order consumption impossible
            Set<Long> expectedSeqnos = LongStream.range(1, seqnos.size() + 1).boxed().collect(Collectors.toSet());
            Set<Long> actualSeqnos = new HashSet<>(seqnos);

            Set<Long> missingSeqnos = new HashSet<>(expectedSeqnos);
            missingSeqnos.removeAll(actualSeqnos);
            Set<Long> extraSeqnos = new HashSet<>(actualSeqnos);
            extraSeqnos.removeAll(expectedSeqnos);

            // Try to provide the most friendly error message possible if this test fails
            assertTrue(
                    missingSeqnos.isEmpty() && extraSeqnos.isEmpty(),
                    "Seqnos for task " + taskId + " should start at 1 and increase strictly by 1 with each record, " +
                            "but the actual seqnos did not.\n" +
                            "Seqnos that should have been emitted but were not: " + missingSeqnos + "\n" +
                            "seqnos that should not have been emitted but were: " + extraSeqnos
            );
        });
    }

    private Map<Integer, List<Long>> parseValuesForTasks(ConsumerRecords<byte[], byte[]> sourceRecords) {
        Map<Integer, List<Long>> result = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> sourceRecord : sourceRecords) {
            assertNotNull(sourceRecord.key(), "Record key should not be null");
            assertNotNull(sourceRecord.value(), "Record value should not be null");

            String key = new String(sourceRecord.key());
            String value = new String(sourceRecord.value());

            String keyPrefix = "key-";
            String valuePrefix = "value-";

            assertTrue(key.startsWith(keyPrefix), "Key should start with \"" + keyPrefix + "\"");
            assertTrue(value.startsWith(valuePrefix), "Value should start with \"" + valuePrefix + "\"");
            assertEquals(
                    key.substring(keyPrefix.length()),
                    value.substring(valuePrefix.length()),
                    "key and value should be identical after prefix"
            );

            String[] split = key.substring(keyPrefix.length()).split("-");
            assertEquals(3, split.length, "Key should match pattern 'key-<connectorName>-<taskId>-<seqno>");
            assertEquals(CONNECTOR_NAME, split[0], "Key should match pattern 'key-<connectorName>-<taskId>-<seqno>");

            int taskId;
            try {
                taskId = Integer.parseInt(split[1], 10);
            } catch (NumberFormatException e) {
                throw new AssertionError("Task ID in key should be an integer, was '" + split[1] + "'", e);
            }

            long seqno;
            try {
                seqno = Long.parseLong(split[2], 10);
            } catch (NumberFormatException e) {
                throw new AssertionError("Seqno in key should be a long, was '" + split[2] + "'", e);
            }

            result.computeIfAbsent(taskId, t -> new ArrayList<>()).add(seqno);
        }
        return result;
    }

    private Map<Integer, List<Long>> parseOffsetForTasks(ConsumerRecords<byte[], byte[]> offsetRecords) {
        JsonConverter offsetsConverter = new JsonConverter();
        // The JSON converter behaves identically for keys and values. If that ever changes, we may need to update this test to use
        // separate converter instances.
        offsetsConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);

        Map<Integer, List<Long>> result = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> offsetRecord : offsetRecords) {
            Object keyObject = offsetsConverter.toConnectData("topic name is not used by converter", offsetRecord.key()).value();
            Object valueObject = offsetsConverter.toConnectData("topic name is not used by converter", offsetRecord.value()).value();

            assertNotNull(keyObject, "Offset key should not be null");
            assertNotNull(valueObject, "Offset value should not be null");

            @SuppressWarnings("unchecked")
            List<Object> key = assertAndCast(keyObject, List.class, "Key");
            assertEquals(
                    2,
                    key.size(),
                    "Offset topic key should be a list containing two elements: the name of the connector, and the connector-provided source partition"
            );
            assertEquals(CONNECTOR_NAME, key.get(0));
            @SuppressWarnings("unchecked")
            Map<String, Object> partition = assertAndCast(key.get(1), Map.class, "Key[1]");
            Object taskIdObject = partition.get("task.id");
            assertNotNull(taskIdObject, "Serialized source partition should contain 'task.id' field from MonitorableSourceConnector");
            String taskId = assertAndCast(taskIdObject, String.class, "task ID");
            assertTrue(taskId.startsWith(CONNECTOR_NAME + "-"), "task ID should match pattern '<connectorName>-<taskId>");
            String taskIdRemainder = taskId.substring(CONNECTOR_NAME.length() + 1);
            int taskNum;
            try {
                taskNum = Integer.parseInt(taskIdRemainder);
            } catch (NumberFormatException e) {
                throw new AssertionError("task ID should match pattern '<connectorName>-<taskId>', where <taskId> is an integer", e);
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> value = assertAndCast(valueObject, Map.class, "Value");

            Object seqnoObject = value.get("saved");
            assertNotNull(seqnoObject, "Serialized source offset should contain 'seqno' field from MonitorableSourceConnector");
            long seqno = assertAndCast(seqnoObject, Long.class, "Seqno offset field");

            result.computeIfAbsent(taskNum, t -> new ArrayList<>()).add(seqno);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    static <T> T assertAndCast(Object o, Class<T> klass, String objectDescription) {
        String className = o == null ? "null" : o.getClass().getName();
        assertInstanceOf(klass, o, objectDescription + " should be " + klass.getName() + "; was " + className + " instead");
        return (T) o;
    }

    /**
     * Clear all existing task handles for the connector, then preemptively create {@code numTasks} many task handles for it,
     * and return a {@link StartAndStopLatch} that can be used to {@link StartAndStopLatch#await(long, TimeUnit) await}
     * the startup of that connector and the expected number of tasks.
     * @param numTasks the number of tasks that should be started
     * @return a {@link StartAndStopLatch} that will block until the connector and the expected number of tasks have started
     */
    private StartAndStopLatch connectorAndTaskStart(int numTasks) {
        connectorHandle.clearTasks();
        IntStream.range(0, numTasks)
                .mapToObj(i -> MonitorableSourceConnector.taskId(CONNECTOR_NAME, i))
                .forEach(connectorHandle::taskHandle);
        return connectorHandle.expectedStarts(1, true);
    }

    private void assertConnectorStarted(StartAndStopLatch connectorStart) throws InterruptedException {
        assertTrue(
                connectorStart.await(
                        ConnectAssertions.CONNECTOR_SETUP_DURATION_MS,
                        TimeUnit.MILLISECONDS
                ),
                "Connector and tasks did not finish startup in time"
        );
    }

    private void assertConnectorStopped(StartAndStopLatch connectorStop) throws InterruptedException {
        assertTrue(
                connectorStop.await(
                        ConnectAssertions.CONNECTOR_SHUTDOWN_DURATION_MS,
                        TimeUnit.MILLISECONDS
                ),
                "Connector and tasks did not finish shutdown in time"
        );
    }

    private void assertProducersAreFencedOnReconfiguration(
            int currentNumTasks,
            int newNumTasks,
            String topic,
            Map<String, String> baseConnectorProps) throws InterruptedException {

        // create a collection of producers that simulate the producers used for the existing tasks
        List<KafkaProducer<byte[], byte[]>> producers = IntStream.range(0, currentNumTasks)
                .mapToObj(i -> transactionalProducer(
                        "simulated-task-producer-" + CONNECTOR_NAME + "-" + i,
                        Worker.taskTransactionalId(CLUSTER_GROUP_ID, CONNECTOR_NAME, i)
                )).collect(Collectors.toList());

        producers.forEach(KafkaProducer::initTransactions);

        // reconfigure the connector with a new number of tasks
        StartAndStopLatch connectorStart = connectorAndTaskStart(newNumTasks);
        baseConnectorProps.put(TASKS_MAX_CONFIG, Integer.toString(newNumTasks));
        log.info("Reconfiguring connector from {} tasks to {}", currentNumTasks, newNumTasks);
        connect.configureConnector(CONNECTOR_NAME, baseConnectorProps);
        assertConnectorStarted(connectorStart);

        // validate that the old producers were fenced out
        producers.forEach(producer -> assertTransactionalProducerIsFenced(producer, topic));
    }

    private KafkaProducer<byte[], byte[]> transactionalProducer(String clientId, String transactionalId) {
        Map<String, Object> transactionalProducerProps = new HashMap<>();
        transactionalProducerProps.put(CLIENT_ID_CONFIG, clientId);
        transactionalProducerProps.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        transactionalProducerProps.put(TRANSACTIONAL_ID_CONFIG, transactionalId);
        return connect.kafka().createProducer(transactionalProducerProps);
    }

    private void assertTransactionalProducerIsFenced(KafkaProducer<byte[], byte[]> producer, String topic) {
        producer.beginTransaction();
        assertThrows(
                ProducerFencedException.class,
                () -> {
                    producer.send(new ProducerRecord<>(topic, new byte[] {69}, new byte[] {96}));
                    producer.commitTransaction();
                },
                "Producer should be fenced out"
        );
        producer.close(Duration.ZERO);
    }

    public static class NaughtyConnector extends SourceConnector {
        private Map<String, String> props;

        @Override
        public void start(Map<String, String> props) {
            this.props = props;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return NaughtyTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return IntStream.range(0, maxTasks).mapToObj(i -> props).collect(Collectors.toList());
        }

        @Override
        public void stop() {
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public String version() {
            return "none";
        }
    }

    public static class NaughtyTask extends SourceTask {
        private String topic;

        @Override
        public void start(Map<String, String> props) {
            if (!props.containsKey(OFFSETS_TOPIC_CONFIG)) {
                throw new ConnectException("No offsets topic");
            }
            this.topic = props.get(OFFSETS_TOPIC_CONFIG);
        }

        @Override
        public List<SourceRecord> poll() {
            // Request a read to the end of the offsets topic
            context.offsetStorageReader().offset(Collections.singletonMap("", null));
            // Produce a record to the offsets topic
            return Collections.singletonList(new SourceRecord(null, null, topic, null, "", null, null));
        }

        @Override
        public void stop() {
        }

        @Override
        public String version() {
            return "none";
        }
    }
}
