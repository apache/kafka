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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectClusterAssertions;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
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
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.CONNECTOR;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.INTERVAL;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.POLL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class ExactlyOnceSourceIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceSourceIntegrationTest.class);
    private static final String CLUSTER_GROUP_ID = "exactly-once-source-integration-test";
    private static final String CONNECTOR_NAME = "exactlyOnceQuestionMark";

    private static final int SOURCE_TASK_PRODUCE_TIMEOUT_MS = 30_000;

    private Properties brokerProps;
    private Map<String, String> workerProps;
    private EmbeddedConnectCluster.Builder connectBuilder;
    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @Before
    public void setup() {
        workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, CLUSTER_GROUP_ID);

        brokerProps = new Properties();
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");

        // build a Connect cluster backed by Kafka and Zk
        connectBuilder = new EmbeddedConnectCluster.Builder()
                .numWorkers(3)
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

    @After
    public void close() {
        try {
            // stop all Connect, Kafka and Zk threads.
            connect.stop();
        } finally {
            // Clear the handle for the connector. Fun fact: if you don't do this, your tests become quite flaky.
            RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);
        }
    }

    /**
     * A simple test for the pre-flight validation API for connectors to provide their own delivery guarantees.
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
        props.put("exactly.once.support.level", "null");
        ConfigInfos validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have exactly one error", 1, validation.errorCount());
        ConfigInfo propertyValidation = findConfigInfo(EXACTLY_ONCE_SUPPORT_CONFIG, validation);
        assertFalse("Preflight validation for exactly-once support property should have at least one error message",
                propertyValidation.configValue().errors().isEmpty());

        // Connector will return UNSUPPORTED from SourceConnector::exactlyOnceSupport
        props.put("exactly.once.support.level", "unsupported");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have exactly one error", 1, validation.errorCount());
        propertyValidation = findConfigInfo(EXACTLY_ONCE_SUPPORT_CONFIG, validation);
        assertFalse("Preflight validation for exactly-once support property should have at least one error message",
                propertyValidation.configValue().errors().isEmpty());

        // Connector will throw an exception from SourceConnector::exactlyOnceSupport
        props.put("exactly.once.support.level", "fail");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have exactly one error", 1, validation.errorCount());
        propertyValidation = findConfigInfo(EXACTLY_ONCE_SUPPORT_CONFIG, validation);
        assertFalse("Preflight validation for exactly-once support property should have at least one error message",
                propertyValidation.configValue().errors().isEmpty());

        // Connector will return SUPPORTED from SourceConnector::exactlyOnceSupport
        props.put("exactly.once.support.level", "supported");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have zero errors", 0, validation.errorCount());

        // Test out the transaction boundary definition property
        props.put(TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());

        // Connector will return null from SourceConnector::canDefineTransactionBoundaries
        props.put("custom.transaction.boundary.support", "null");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have exactly one error", 1, validation.errorCount());
        propertyValidation = findConfigInfo(TRANSACTION_BOUNDARY_CONFIG, validation);
        assertFalse("Preflight validation for transaction boundary property should have at least one error message",
                propertyValidation.configValue().errors().isEmpty());

        // Connector will return UNSUPPORTED from SourceConnector::canDefineTransactionBoundaries
        props.put("custom.transaction.boundary.support", "unsupported");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have exactly one error", 1, validation.errorCount());
        propertyValidation = findConfigInfo(TRANSACTION_BOUNDARY_CONFIG, validation);
        assertFalse("Preflight validation for transaction boundary property should have at least one error message",
                propertyValidation.configValue().errors().isEmpty());

        // Connector will throw an exception from SourceConnector::canDefineTransactionBoundaries
        props.put("custom.transaction.boundary.support", "null");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have exactly one error", 1, validation.errorCount());
        propertyValidation = findConfigInfo(TRANSACTION_BOUNDARY_CONFIG, validation);
        assertFalse("Preflight validation for transaction boundary property should have at least one error message",
                propertyValidation.configValue().errors().isEmpty());

        // Connector will return SUPPORTED from SourceConnector::canDefineTransactionBoundaries
        props.put("custom.transaction.boundary.support", "supported");
        validation = connect.validateConnectorConfig(MonitorableSourceConnector.class.getSimpleName(), props);
        assertEquals("Preflight validation should have zero errors", 0, validation.errorCount());
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

        int recordsProduced = 100;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, POLL.toString());
        props.put("messages.per.poll", Integer.toString(recordsProduced));

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(recordsProduced);
        connectorHandle.expectedCommits(recordsProduced);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka()
            .consume(
                recordsProduced,
                TimeUnit.MINUTES.toMillis(1),
                Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                "test-topic")
            .count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + recordsProduced + " + but got " + recordNum,
            recordNum >= recordsProduced);
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

        int recordsProduced = 100;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, INTERVAL.toString());
        props.put(TRANSACTION_BOUNDARY_INTERVAL_CONFIG, "10000");
        props.put("messages.per.poll", Integer.toString(recordsProduced));

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(recordsProduced);
        connectorHandle.expectedCommits(recordsProduced);

        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka()
                .consume(
                        recordsProduced,
                        TimeUnit.MINUTES.toMillis(1),
                        Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                        "test-topic")
                .count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + recordsProduced + " + but got " + recordNum,
                recordNum >= recordsProduced);
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

        int recordsProduced = 100;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, CONNECTOR.toString());
        props.put("custom.transaction.boundary.support", "supported");
        props.put("messages.per.poll", Integer.toString(recordsProduced));

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(recordsProduced);
        connectorHandle.expectedCommits(recordsProduced);

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
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // consume all records from the source topic or fail, to ensure that they were correctly produced
        ConsumerRecords<byte[], byte[]> sourceRecords = connect.kafka()
                .consume(
                        recordsProduced,
                        TimeUnit.MINUTES.toMillis(1),
                        consumerProps,
                        "test-topic");
        assertTrue("Not enough records produced by source connector. Expected at least: " + recordsProduced + " + but got " + sourceRecords.count(),
                sourceRecords.count() >= recordsProduced);

        // also consume from the cluster's  offsets topic to verify that the expected offsets (which should correspond to the connector's
        // custom transaction boundaries) were committed
        List<Long> expectedOffsetSeqnos = new ArrayList<>();
        long lastExpectedOffsetSeqno = 1;
        long nextExpectedOffsetSeqno = 1;
        while (nextExpectedOffsetSeqno <= recordsProduced) {
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

        List<Long> actualOffsetSeqnos = new ArrayList<>();
        offsetRecords.forEach(record -> actualOffsetSeqnos.add(parseAndAssertOffsetForSingleTask(record)));

        assertEquals("Committed offsets should match connector-defined transaction boundaries",
                expectedOffsetSeqnos, actualOffsetSeqnos.subList(0, expectedOffsetSeqnos.size()));

        List<Long> expectedRecordSeqnos = LongStream.range(1, recordsProduced + 1).boxed().collect(Collectors.toList());
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
        List<Long> actualRecordSeqnos = new ArrayList<>();
        sourceRecords.forEach(record -> actualRecordSeqnos.add(parseAndAssertValueForSingleTask(record)));
        Collections.sort(actualRecordSeqnos);
        assertEquals("Committed records should exclude connector-aborted transactions",
                expectedRecordSeqnos, actualRecordSeqnos.subList(0, expectedRecordSeqnos.size()));
    }

    /**
     * Brings up a one-node cluster, then intentionally fences out the transactional producer used by the leader
     * for writes to the config topic to simulate a zombie leader being active in the cluster. The leader should
     * automatically recover, verify that it is still the leader, and then succeed to create a connector when the
     * user resends  the request.
     */
    @Test
    public void testFencedLeaderRecovery() throws Exception {
        // Much slower offset commit interval; should never be triggered during this test
        workerProps.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "600000");
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        int recordsProduced = 100;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, POLL.toString());
        props.put("messages.per.poll", Integer.toString(recordsProduced));

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(recordsProduced);
        connectorHandle.expectedCommits(recordsProduced);

        // make sure the worker is actually up (otherwise, it may fence out our simulated zombie leader, instead of the other way around)
        assertEquals(404, connect.requestGet(connect.endpointForResource("connectors/nonexistent")).getStatus());

        // fence out the leader of the cluster
        transactionalProducer(DistributedConfig.transactionalProducerId(CLUSTER_GROUP_ID)).initTransactions();

        // start a source connector--should fail the first time
        assertThrows(ConnectRestException.class, () -> connect.configureConnector(CONNECTOR_NAME, props));

        // if at first you don't succeed, then spam the worker with rest requests until it gives in to your demands
        connect.configureConnector(CONNECTOR_NAME, props);

        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka()
                .consume(
                        recordsProduced,
                        TimeUnit.MINUTES.toMillis(1),
                        Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                        "test-topic")
                .count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + recordsProduced + " + but got " + recordNum,
                recordNum >= recordsProduced);
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

        int recordsProduced = 100;

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put("messages.per.poll", Integer.toString(recordsProduced));

        // expect all records to be consumed and committed by the connector
        connectorHandle.expectedRecords(recordsProduced);
        connectorHandle.expectedCommits(recordsProduced);

        StartAndStopLatch connectorStart = connectorAndTaskStart(3);
        props.put(TASKS_MAX_CONFIG, "3");
        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorStarted(connectorStart);

        assertProducersAreFencedOnReconfiguration(3, 5, topic, props);
        assertProducersAreFencedOnReconfiguration(5, 1, topic, props);
        assertProducersAreFencedOnReconfiguration(1, 5, topic, props);
        assertProducersAreFencedOnReconfiguration(5, 3, topic, props);

        // Do a final sanity check to make sure that the final generation of tasks is able to run
        log.info("Waiting for records to be provided to worker by task");
        // wait for the connector tasks to produce enough records
        connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

        log.info("Waiting for records to be committed to Kafka by worker");
        // wait for the connector tasks to commit enough records
        connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

        // consume all records from the source topic or fail, to ensure that they were correctly produced
        int recordNum = connect.kafka()
                .consume(
                        recordsProduced,
                        TimeUnit.MINUTES.toMillis(1),
                        Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                        "test-topic")
                .count();
        assertTrue("Not enough records produced by source connector. Expected at least: " + recordsProduced + " + but got " + recordNum,
                recordNum >= recordsProduced);
    }

    @Test
    public void testConnectorFailsOnInabilityToFence() throws Exception {
        brokerProps.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
        brokerProps.put("sasl.enabled.mechanisms", "PLAIN");
        brokerProps.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
        brokerProps.put("security.inter.broker.protocol", "SASL_PLAINTEXT");
        brokerProps.put("listeners", "PLAINTEXT://localhost:0,SASL_PLAINTEXT://localhost:0");
        brokerProps.put("listener.name.sasl_plaintext.plain.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"super\" "
                        + "password=\"super_pwd\" "
                        + "user_super=\"super_pwd\";");
        brokerProps.put("super.users", "User:super");

        Map<String, String> superUserClientConfig = new HashMap<>();
        superUserClientConfig.put("sasl.mechanism", "PLAIN");
        superUserClientConfig.put("security.protocol", "SASL_PLAINTEXT");
        superUserClientConfig.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"super\" "
                        + "password=\"super_pwd\";");
        workerProps.putAll(superUserClientConfig);

        connectBuilder.brokerListener(ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT));
        startConnect();

        String topic = "test-topic";
        Properties adminClientProperties = new Properties();
        superUserClientConfig.forEach(adminClientProperties::put);
        adminClientProperties.put(BOOTSTRAP_SERVERS_CONFIG,
                connect.kafka().bootstrapServers(ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)));
        connect.kafka().createTopic(topic, 3, 1, Collections.emptyMap(), adminClientProperties);

        Map<String, String> props = new HashMap<>();
        int tasksMax = 2; // Use two tasks since single-task connectors don't require zombie fencing
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));
        superUserClientConfig.forEach((property, value) -> {
            props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + property, value);
            props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + property, value);
            props.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + property, value);
        });

        StartAndStopLatch connectorStart = connectorAndTaskStart(tasksMax);

        log.info("Bringing up connector with valid admin client credentials");
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorStarted(connectorStart);
        // Verify that the task has been able to start successfully
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, tasksMax, "Connector and task should have started successfully");

        // Reconfigure the connector to point towards a broker port that won't be able to provide a principal for it,
        // which should lead to an authorization exception since the User:ANONYMOUS principal can't do anything
        superUserClientConfig.keySet().forEach(property ->
            props.remove(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + property)
        );
        props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + "security.protocol", "PLAINTEXT");
        props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG,
                connect.kafka().bootstrapServers(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)));
        log.info("Bringing up connector with invalid admin client credentials");
        connect.configureConnector(CONNECTOR_NAME, props);

        // Verify that the task has failed, and that the failure is visible to users via the REST API
        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(CONNECTOR_NAME, tasksMax, "Task should have failed on startup");
    }

    @Test
    public void testSeparateOffsetsTopic() throws Exception {
        String globalOffsetsTopic = "cluster-global-offsets-topic";
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, globalOffsetsTopic);
        startConnect();
        EmbeddedKafkaCluster connectorTargetedCluster = new EmbeddedKafkaCluster(1, brokerProps);
        try (Closeable clusterShutdown = connectorTargetedCluster::stop) {
            connectorTargetedCluster.start();
            String topic = "test-topic";
            connectorTargetedCluster.createTopic(topic, 3);

            int recordsProduced = 100;

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
            props.put(TASKS_MAX_CONFIG, "1");
            props.put(TOPIC_CONFIG, topic);
            props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
            props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
            props.put(NAME_CONFIG, CONNECTOR_NAME);
            props.put(TRANSACTION_BOUNDARY_CONFIG, POLL.toString());
            props.put("messages.per.poll", Integer.toString(recordsProduced));
            props.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, connectorTargetedCluster.bootstrapServers());
            props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, connectorTargetedCluster.bootstrapServers());
            props.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, connectorTargetedCluster.bootstrapServers());
            String offsetsTopic = CONNECTOR_NAME + "-offsets";
            props.put(OFFSETS_TOPIC_CONFIG, offsetsTopic);

            // expect all records to be consumed and committed by the connector
            connectorHandle.expectedRecords(recordsProduced);
            connectorHandle.expectedCommits(recordsProduced);

            // start a source connector
            connect.configureConnector(CONNECTOR_NAME, props);

            log.info("Waiting for records to be provided to worker by task");
            // wait for the connector tasks to produce enough records
            connectorHandle.awaitRecords(SOURCE_TASK_PRODUCE_TIMEOUT_MS);

            log.info("Waiting for records to be committed to Kafka by worker");
            // wait for the connector tasks to commit enough records
            connectorHandle.awaitCommits(TimeUnit.MINUTES.toMillis(1));

            // consume all records from the source topic or fail, to ensure that they were correctly produced
            int recordNum = connectorTargetedCluster
                    .consume(
                            recordsProduced,
                            TimeUnit.MINUTES.toMillis(1),
                            Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                            "test-topic")
                    .count();
            assertTrue("Not enough records produced by source connector. Expected at least: " + recordsProduced + " + but got " + recordNum,
                    recordNum >= recordsProduced);

            // also consume from the connector's dedicated offsets topic; just need to read one offset record
            ConsumerRecord<byte[], byte[]> offsetRecord = connectorTargetedCluster
                    .consume(
                            1,
                            TimeUnit.MINUTES.toMillis(1),
                            Collections.singletonMap(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                            offsetsTopic
                    ).iterator().next();
            long seqno = parseAndAssertOffsetForSingleTask(offsetRecord);
            assertEquals("Offset commits should occur on connector-defined poll boundaries, which happen every " + recordsProduced + " records",
                    0, seqno % recordsProduced);

            // also consume from the cluster's global offsets topic; again, just need to read one offset record
            offsetRecord = connect.kafka()
                    .consume(
                            1,
                            TimeUnit.MINUTES.toMillis(1),
                            globalOffsetsTopic
                    ).iterator().next();
            seqno = parseAndAssertOffsetForSingleTask(offsetRecord);
            assertEquals("Offset commits should occur on connector-defined poll boundaries, which happen every " + recordsProduced + " records",
                    0, seqno % recordsProduced);

            // Delete the connector before shutting down the Kafka cluster it's targeting
            connect.deleteConnector(CONNECTOR_NAME);
        }
    }

    @Test
    public void testPotentialDeadlockWhenProducingToOffsetsTopic() throws Exception {
        connectBuilder.numWorkers(1);
        startConnect();

        String topic = "test-topic";
        connect.kafka().createTopic(topic, 3);

        int recordsProduced = 100;

        Map<String, String> props = new HashMap<>();
        // See below; this connector does nothing except request offsets from the worker in SourceTask::poll
        // and then return a single record targeted at its offsets topic
        props.put(CONNECTOR_CLASS_CONFIG, NaughtyConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(TRANSACTION_BOUNDARY_CONFIG, INTERVAL.toString());
        props.put("messages.per.poll", Integer.toString(recordsProduced));
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

    @SuppressWarnings("unchecked")
    private long parseAndAssertOffsetForSingleTask(ConsumerRecord<byte[], byte[]> offsetRecord) {
        JsonConverter offsetsConverter = new JsonConverter();
        // The JSON converter behaves identically for keys and values. If that ever changes, we may need to update this test to use
        // separate converter instances.

        offsetsConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);
        Object keyObject = offsetsConverter.toConnectData("topic name is not used by converter", offsetRecord.key()).value();
        Object valueObject = offsetsConverter.toConnectData("topic name is not used by converter", offsetRecord.value()).value();

        assertNotNull("Offset value should not be null", valueObject);

        assertEquals("Serialized source partition should match expected format",
                Arrays.asList(CONNECTOR_NAME, MonitorableSourceConnector.sourcePartition(MonitorableSourceConnector.taskId(CONNECTOR_NAME, 0))),
                keyObject);

        Map<String, Object> value = assertAndCast(valueObject, Map.class, "Value");

        Object seqnoObject = value.get("saved");
        assertNotNull("Serialized source offset should contain 'seqno' field from MonitorableSourceConnector", seqnoObject);
        return assertAndCast(seqnoObject, Long.class, "Seqno offset field");
    }

    private long parseAndAssertValueForSingleTask(ConsumerRecord<byte[], byte[]> sourceRecord) {
        assertNotNull("Record key should not be null", sourceRecord.key());
        assertNotNull("Record value should not be null", sourceRecord.value());

        String key = new String(sourceRecord.key());
        String value = new String(sourceRecord.value());

        String taskId = MonitorableSourceConnector.taskId(CONNECTOR_NAME, 0);
        String keyPrefix = "key-" + taskId + "-";
        String valuePrefix = "value-" + taskId + "-";

        assertTrue("Key should start with \"" + keyPrefix + "\"", key.startsWith(keyPrefix));
        assertTrue("Value should start with \"" + valuePrefix + "\"", value.startsWith(valuePrefix));

        String keySeqno = key.substring(keyPrefix.length());
        String valueSeqno = value.substring(valuePrefix.length());

        assertEquals("Seqnos for key and value should match", keySeqno, valueSeqno);

        return Long.parseLong(keySeqno);
    }

    @SuppressWarnings("unchecked")
    private static <T> T assertAndCast(Object o, Class<T> klass, String objectDescription) {
        String className = o == null ? "null" : o.getClass().getName();
        assertTrue(objectDescription + " should be " + klass.getName() + "; was " + className + " instead", klass.isInstance(o));
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
        assertTrue("Connector and tasks did not finish startup in time",
                connectorStart.await(
                        EmbeddedConnectClusterAssertions.CONNECTOR_SETUP_DURATION_MS,
                        TimeUnit.MILLISECONDS
                )
        );
    }

    private void assertProducersAreFencedOnReconfiguration(
            int currentNumTasks,
            int newNumTasks,
            String topic,
            Map<String, String> baseConnectorProps) throws InterruptedException {

        // create a collection of producers that simulate the producers used for the existing tasks
        List<KafkaProducer<byte[], byte[]>> producers = IntStream.range(0, currentNumTasks)
                .mapToObj(i -> Worker.transactionalId(CLUSTER_GROUP_ID, CONNECTOR_NAME, i))
                .map(this::transactionalProducer)
                .collect(Collectors.toList());

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

    private KafkaProducer<byte[], byte[]> transactionalProducer(String transactionalId) {
        Map<String, Object> transactionalProducerProps = new HashMap<>();
        transactionalProducerProps.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        transactionalProducerProps.put(TRANSACTIONAL_ID_CONFIG, transactionalId);
        return connect.kafka().createProducer(transactionalProducerProps);
    }

    private void assertTransactionalProducerIsFenced(KafkaProducer<byte[], byte[]> producer, String topic) {
        producer.beginTransaction();
        assertThrows("Producer should be fenced out",
                ProducerFencedException.class,
                () -> {
                    producer.send(new ProducerRecord<>(topic, new byte[] {69}, new byte[] {96}));
                    producer.commitTransaction();
                }
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
