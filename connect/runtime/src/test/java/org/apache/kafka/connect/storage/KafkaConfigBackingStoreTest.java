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
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TestFuture;
import org.apache.kafka.connect.util.TopicAdmin;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.INCLUDE_TASKS_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.ONLY_FAILED_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.READ_WRITE_TOTAL_TIMEOUT_MS;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.RESTART_KEY;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaConfigBackingStore.class, WorkerConfig.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*"})
public class KafkaConfigBackingStoreTest {
    private static final String CLIENT_ID_BASE = "test-client-id-";
    private static final String TOPIC = "connect-configs";
    private static final short TOPIC_REPLICATION_FACTOR = 5;
    private static final Map<String, String> DEFAULT_CONFIG_STORAGE_PROPS = new HashMap<>();

    static {
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.CONFIG_TOPIC_CONFIG, TOPIC);
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, Short.toString(TOPIC_REPLICATION_FACTOR));
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.GROUP_ID_CONFIG, "connect");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        DEFAULT_CONFIG_STORAGE_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    }

    private static final List<String> CONNECTOR_IDS = Arrays.asList("connector1", "connector2");
    private static final List<String> CONNECTOR_CONFIG_KEYS = Arrays.asList("connector-connector1", "connector-connector2");
    private static final List<String> COMMIT_TASKS_CONFIG_KEYS = Arrays.asList("commit-connector1", "commit-connector2");
    private static final List<String> TARGET_STATE_KEYS =  Arrays.asList("target-state-connector1", "target-state-connector2");
    private static final List<String> CONNECTOR_TASK_COUNT_RECORD_KEYS = Arrays.asList("tasks-fencing-connector1", "tasks-fencing-connector2");

    private static final String CONNECTOR_1_NAME = "connector1";
    private static final String CONNECTOR_2_NAME = "connector2";
    private static final List<String> RESTART_CONNECTOR_KEYS = Arrays.asList(RESTART_KEY(CONNECTOR_1_NAME), RESTART_KEY(CONNECTOR_2_NAME));

    // Need a) connector with multiple tasks and b) multiple connectors
    private static final List<ConnectorTaskId> TASK_IDS = Arrays.asList(
            new ConnectorTaskId("connector1", 0),
            new ConnectorTaskId("connector1", 1),
            new ConnectorTaskId("connector2", 0)
    );
    private static final List<String> TASK_CONFIG_KEYS = Arrays.asList("task-connector1-0", "task-connector1-1", "task-connector2-0");

    // Need some placeholders -- the contents don't matter here, just that they are restored properly
    private static final List<Map<String, String>> SAMPLE_CONFIGS = Arrays.asList(
            Collections.singletonMap("config-key-one", "config-value-one"),
            Collections.singletonMap("config-key-two", "config-value-two"),
            Collections.singletonMap("config-key-three", "config-value-three")
    );
    private static final List<Struct> CONNECTOR_CONFIG_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(0)),
            new Struct(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(1)),
            new Struct(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(2))
    );
    private static final List<Struct> TASK_CONFIG_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.TASK_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(0)),
            new Struct(KafkaConfigBackingStore.TASK_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(1))
    );
    private static final List<Struct> CONNECTOR_TASK_COUNT_RECORD_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.TASK_COUNT_RECORD_V0).put("task-count", 6),
            new Struct(KafkaConfigBackingStore.TASK_COUNT_RECORD_V0).put("task-count", 9)
    );
    private static final Struct TARGET_STATE_STARTED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V0).put("state", "STARTED");
    private static final Struct TARGET_STATE_PAUSED_LEGACY = new Struct(KafkaConfigBackingStore.TARGET_STATE_V0)
            .put("state", "PAUSED");
    private static final Struct TARGET_STATE_PAUSED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V1)
            .put("state", "PAUSED")
            .put("state.v2", "PAUSED");
    private static final Struct TARGET_STATE_STOPPED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V1)
            .put("state", "PAUSED")
            .put("state.v2", "STOPPED");

    private static final Struct TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 2);

    private static final Struct TASKS_COMMIT_STRUCT_ZERO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 0);

    private static final Struct ONLY_FAILED_MISSING_STRUCT = new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(INCLUDE_TASKS_FIELD_NAME, false);
    private static final Struct INCLUDE_TASKS_MISSING_STRUCT = new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(ONLY_FAILED_FIELD_NAME, true);
    private static final List<Struct> RESTART_REQUEST_STRUCTS = Arrays.asList(
                new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(ONLY_FAILED_FIELD_NAME, true).put(INCLUDE_TASKS_FIELD_NAME, false),
                ONLY_FAILED_MISSING_STRUCT,
                INCLUDE_TASKS_MISSING_STRUCT);

    // The exact format doesn't matter here since both conversions are mocked
    private static final List<byte[]> CONFIGS_SERIALIZED = Arrays.asList(
            "config-bytes-1".getBytes(), "config-bytes-2".getBytes(), "config-bytes-3".getBytes(),
            "config-bytes-4".getBytes(), "config-bytes-5".getBytes(), "config-bytes-6".getBytes(),
            "config-bytes-7".getBytes(), "config-bytes-8".getBytes(), "config-bytes-9".getBytes()
    );

    @Mock
    private Converter converter;
    @Mock
    private ConfigBackingStore.UpdateListener configUpdateListener;
    private Map<String, String> props = new HashMap<>(DEFAULT_CONFIG_STORAGE_PROPS);
    private DistributedConfig config;
    @Mock
    KafkaBasedLog<String, byte[]> storeLog;
    @Mock
    Producer<String, byte[]> fencableProducer;
    @Mock
    Future<RecordMetadata> producerFuture;
    private KafkaConfigBackingStore configStorage;

    private final Capture<String> capturedTopic = EasyMock.newCapture();
    private final Capture<Map<String, Object>> capturedProducerProps = EasyMock.newCapture();
    private final Capture<Map<String, Object>> capturedConsumerProps = EasyMock.newCapture();
    private final Capture<Supplier<TopicAdmin>> capturedAdminSupplier = EasyMock.newCapture();
    private final Capture<NewTopic> capturedNewTopic = EasyMock.newCapture();
    private final Capture<Callback<ConsumerRecord<String, byte[]>>> capturedConsumedCallback = EasyMock.newCapture();
    private final MockTime time = new MockTime();

    private long logOffset = 0;

    private void createStore() {
        config = PowerMock.createPartialMock(
                DistributedConfig.class,
                new String[]{"kafkaClusterId"},
                props);
        EasyMock.expect(config.kafkaClusterId()).andReturn("test-cluster").anyTimes();
        // The kafkaClusterId is used in the constructor for KafkaConfigBackingStore
        // So temporarily enter replay mode in order to mock that call
        EasyMock.replay(config);
        configStorage = PowerMock.createPartialMock(
                KafkaConfigBackingStore.class,
                new String[]{"createKafkaBasedLog", "createFencableProducer"},
                converter, config, null, null, CLIENT_ID_BASE, time);
        Whitebox.setInternalState(configStorage, "configLog", storeLog);
        configStorage.setUpdateListener(configUpdateListener);
        // The mock must be reset and re-mocked for the remainder of the test.
        // TODO: Once this migrates to Mockito, just use a spy()
        EasyMock.reset(config);
        EasyMock.expect(config.kafkaClusterId()).andReturn("test-cluster").anyTimes();
    }

    @Before
    public void setUp() {
        createStore();
    }

    @Test
    public void testStartStop() throws Exception {
        props.put("config.storage.min.insync.replicas", "3");
        props.put("config.storage.max.message.bytes", "1001");
        createStore();
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);
        expectStop();
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        assertEquals(TOPIC, capturedTopic.getValue());
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", capturedProducerProps.getValue().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", capturedProducerProps.getValue().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        assertEquals(TOPIC, capturedNewTopic.getValue().name());
        assertEquals(1, capturedNewTopic.getValue().numPartitions());
        assertEquals(TOPIC_REPLICATION_FACTOR, capturedNewTopic.getValue().replicationFactor());
        assertEquals("3", capturedNewTopic.getValue().configs().get("min.insync.replicas"));
        assertEquals("1001", capturedNewTopic.getValue().configs().get("max.message.bytes"));
        configStorage.start();
        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testSnapshotCannotMutateInternalState() throws Exception {
        props.put("config.storage.min.insync.replicas", "3");
        props.put("config.storage.max.message.bytes", "1001");
        createStore();
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        configStorage.start();
        ClusterConfigState snapshot = configStorage.snapshot();
        assertNotSame(snapshot.connectorTaskCounts, configStorage.connectorTaskCounts);
        assertNotSame(snapshot.connectorConfigs, configStorage.connectorConfigs);
        assertNotSame(snapshot.connectorTargetStates, configStorage.connectorTargetStates);
        assertNotSame(snapshot.taskConfigs, configStorage.taskConfigs);
        assertNotSame(snapshot.connectorTaskCountRecords, configStorage.connectorTaskCountRecords);
        assertNotSame(snapshot.connectorTaskConfigGenerations, configStorage.connectorTaskConfigGenerations);
        assertNotSame(snapshot.connectorsPendingFencing, configStorage.connectorsPendingFencing);
        assertNotSame(snapshot.inconsistentConnectors, configStorage.inconsistent);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        expectConvertWriteAndRead(
                CONNECTOR_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        configUpdateListener.onConnectorConfigUpdate(CONNECTOR_IDS.get(0));
        EasyMock.expectLastCall();

        expectConvertWriteAndRead(
                CONNECTOR_CONFIG_KEYS.get(1), KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(1),
                "properties", SAMPLE_CONFIGS.get(1));
        configUpdateListener.onConnectorConfigUpdate(CONNECTOR_IDS.get(1));
        EasyMock.expectLastCall();

        // Config deletion
        expectConnectorRemoval(CONNECTOR_CONFIG_KEYS.get(1), TARGET_STATE_KEYS.get(1));
        configUpdateListener.onConnectorConfigRemove(CONNECTOR_IDS.get(1));
        EasyMock.expectLastCall();

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));

        // Writing should block until it is written and read back from Kafka
        configStorage.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0));
        configState = configStorage.snapshot();
        assertEquals(1, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));

        // Second should also block and all configs should still be available
        configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(1));
        configState = configStorage.snapshot();
        assertEquals(2, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.connectorConfig(CONNECTOR_IDS.get(1)));

        // Deletion should remove the second one we added
        configStorage.removeConnectorConfig(CONNECTOR_IDS.get(1));
        configState = configStorage.snapshot();
        assertEquals(4, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));
        assertNull(configState.targetState(CONNECTOR_IDS.get(1)));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfigProducerError() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);

        expectConvert(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONNECTOR_CONFIG_STRUCTS.get(0), CONFIGS_SERIALIZED.get(0));

        storeLog.sendWithReceipt(EasyMock.anyObject(), EasyMock.anyObject());
        EasyMock.expectLastCall().andReturn(producerFuture);

        producerFuture.get(EasyMock.anyLong(), EasyMock.anyObject());
        EasyMock.expectLastCall().andThrow(new ExecutionException(new TopicAuthorizationException(Collections.singleton("test"))));

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Verify initial state
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertEquals(0, configState.connectors().size());

        // verify that the producer exception from KafkaBasedLog::send is propagated
        ConnectException e = assertThrows(ConnectException.class, () -> configStorage.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0)));
        assertTrue(e.getMessage().contains("Error writing connector configuration to Kafka"));
        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRemoveConnectorConfigSlowProducer() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> connectorConfigProducerFuture = PowerMock.createMock(Future.class);
        // tombstone for the connector config
        storeLog.sendWithReceipt(EasyMock.anyObject(), EasyMock.isNull());
        EasyMock.expectLastCall().andReturn(connectorConfigProducerFuture);

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> targetStateProducerFuture = PowerMock.createMock(Future.class);
        // tombstone for the connector target state
        storeLog.sendWithReceipt(EasyMock.anyObject(), EasyMock.isNull());
        EasyMock.expectLastCall().andReturn(targetStateProducerFuture);

        connectorConfigProducerFuture.get(EasyMock.eq(READ_WRITE_TOTAL_TIMEOUT_MS), EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> {
            time.sleep(READ_WRITE_TOTAL_TIMEOUT_MS - 1000);
            return null;
        });

        // the future get timeout is expected to be reduced according to how long the previous Future::get took
        targetStateProducerFuture.get(EasyMock.eq(1000L), EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> {
            time.sleep(1000);
            return null;
        });

        @SuppressWarnings("unchecked")
        Future<Void> future = PowerMock.createMock(Future.class);
        EasyMock.expect(storeLog.readToEnd()).andAnswer(() -> future);

        // the Future::get calls on the previous two producer futures exhausted the overall timeout; so expect the
        // timeout on the log read future to be 0
        EasyMock.expect(future.get(EasyMock.eq(0L), EasyMock.anyObject())).andReturn(null);

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        configStorage.removeConnectorConfig("test-connector");
        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testWritePrivileges() throws Exception {
        // With exactly.once.source.support = preparing (or also, "enabled"), we need to use a transactional producer
        // to write some types of messages to the config topic
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        createStore();

        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        // Try and fail to write a task count record to the config topic without write privileges
        expectConvert(KafkaConfigBackingStore.TASK_COUNT_RECORD_V0, CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(0), CONFIGS_SERIALIZED.get(0));
        // Claim write privileges
        expectFencableProducer();
        // And write the task count record successfully
        expectConvert(KafkaConfigBackingStore.TASK_COUNT_RECORD_V0, CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(0), CONFIGS_SERIALIZED.get(0));
        fencableProducer.beginTransaction();
        EasyMock.expectLastCall();
        EasyMock.expect(fencableProducer.send(EasyMock.anyObject())).andReturn(null);
        fencableProducer.commitTransaction();
        EasyMock.expectLastCall();
        expectRead(CONNECTOR_TASK_COUNT_RECORD_KEYS.get(0), CONFIGS_SERIALIZED.get(0), CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(0));

        // Try to write a connector config
        expectConvert(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONNECTOR_CONFIG_STRUCTS.get(0), CONFIGS_SERIALIZED.get(1));
        fencableProducer.beginTransaction();
        EasyMock.expectLastCall();
        EasyMock.expect(fencableProducer.send(EasyMock.anyObject())).andReturn(null);
        // Get fenced out
        fencableProducer.commitTransaction();
        EasyMock.expectLastCall().andThrow(new ProducerFencedException("Better luck next time"));
        fencableProducer.close(Duration.ZERO);
        EasyMock.expectLastCall();
        // And fail when trying to write again without reclaiming write privileges
        expectConvert(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONNECTOR_CONFIG_STRUCTS.get(0), CONFIGS_SERIALIZED.get(1));

        // In the meantime, write a target state (which doesn't require write privileges)
        expectConvert(KafkaConfigBackingStore.TARGET_STATE_V1, TARGET_STATE_PAUSED, CONFIGS_SERIALIZED.get(1));
        storeLog.sendWithReceipt("target-state-" + CONNECTOR_IDS.get(1), CONFIGS_SERIALIZED.get(1));
        EasyMock.expectLastCall().andReturn(producerFuture);
        producerFuture.get(EasyMock.anyLong(), EasyMock.anyObject());
        EasyMock.expectLastCall().andReturn(null);

        // Reclaim write privileges
        expectFencableProducer();
        // And successfully write the config
        expectConvert(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONNECTOR_CONFIG_STRUCTS.get(0), CONFIGS_SERIALIZED.get(1));
        fencableProducer.beginTransaction();
        EasyMock.expectLastCall();
        EasyMock.expect(fencableProducer.send(EasyMock.anyObject())).andReturn(null);
        fencableProducer.commitTransaction();
        EasyMock.expectLastCall();
        expectConvertRead(CONNECTOR_CONFIG_KEYS.get(1), CONNECTOR_CONFIG_STRUCTS.get(0), CONFIGS_SERIALIZED.get(2));
        configUpdateListener.onConnectorConfigUpdate(CONNECTOR_IDS.get(1));
        EasyMock.expectLastCall();

        expectPartitionCount(1);
        expectStop();
        fencableProducer.close(Duration.ZERO);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should fail the first time since we haven't claimed write privileges
        assertThrows(IllegalStateException.class, () -> configStorage.putTaskCountRecord(CONNECTOR_IDS.get(0), 6));
        // Should succeed now
        configStorage.claimWritePrivileges();
        configStorage.putTaskCountRecord(CONNECTOR_IDS.get(0), 6);

        // Should fail again when we get fenced out
        assertThrows(PrivilegedWriteException.class, () -> configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(0)));
        // Should fail if we retry without reclaiming write privileges
        assertThrows(IllegalStateException.class, () -> configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(0)));

        // Should succeed even without write privileges (target states can be written by anyone)
        configStorage.putTargetState(CONNECTOR_IDS.get(1), TargetState.PAUSED);

        // Should succeed if we re-claim write privileges
        configStorage.claimWritePrivileges();
        configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(0));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskCountRecordsAndGenerations() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        // Task configs should read to end, write to the log, read to end, write root, then read to end again
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(1), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(1),
                "properties", SAMPLE_CONFIGS.get(1));
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(2),
                "tasks", 2); // Starts with 0 tasks, after update has 2
        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)));
        EasyMock.expectLastCall();

        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedConfigs.put(TASK_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(1));
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(2));
        expectReadToEnd(serializedConfigs);

        // Task count records are read back after writing as well
        expectConvertWriteRead(
                CONNECTOR_TASK_COUNT_RECORD_KEYS.get(0), KafkaConfigBackingStore.TASK_COUNT_RECORD_V0, CONFIGS_SERIALIZED.get(3),
                "task-count", 4);
        serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(CONNECTOR_TASK_COUNT_RECORD_KEYS.get(0), CONFIGS_SERIALIZED.get(3));
        expectReadToEnd(serializedConfigs);

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Bootstrap as if we had already added the connector, but no tasks had been added yet
        whiteboxAddConnector(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), Collections.emptyList());

        // Before anything is written
        String connectorName = CONNECTOR_IDS.get(0);
        ClusterConfigState configState = configStorage.snapshot();
        assertFalse(configState.pendingFencing(connectorName));
        assertNull(configState.taskCountRecord(connectorName));
        assertNull(configState.taskConfigGeneration(connectorName));

        // Writing task configs should block until all the writes have been performed and the root record update
        // has completed
        List<Map<String, String>> taskConfigs = Arrays.asList(SAMPLE_CONFIGS.get(0), SAMPLE_CONFIGS.get(1));
        configStorage.putTaskConfigs("connector1", taskConfigs);

        configState = configStorage.snapshot();
        assertEquals(3, configState.offset());
        assertTrue(configState.pendingFencing(connectorName));
        assertNull(configState.taskCountRecord(connectorName));
        assertEquals(0, (long) configState.taskConfigGeneration(connectorName));

        configStorage.putTaskCountRecord(connectorName, 4);

        configState = configStorage.snapshot();
        assertEquals(4, configState.offset());
        assertFalse(configState.pendingFencing(connectorName));
        assertEquals(4, (long) configState.taskCountRecord(connectorName));
        assertEquals(0, (long) configState.taskConfigGeneration(connectorName));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigs() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        // Task configs should read to end, write to the log, read to end, write root, then read to end again
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(1), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(1),
                "properties", SAMPLE_CONFIGS.get(1));
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(2),
                "tasks", 2); // Starts with 0 tasks, after update has 2
        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)));
        EasyMock.expectLastCall();

        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedConfigs.put(TASK_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(1));
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(2));
        expectReadToEnd(serializedConfigs);

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Bootstrap as if we had already added the connector, but no tasks had been added yet
        whiteboxAddConnector(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), Collections.emptyList());

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertNull(configState.taskConfig(TASK_IDS.get(0)));
        assertNull(configState.taskConfig(TASK_IDS.get(1)));

        // Writing task configs should block until all the writes have been performed and the root record update
        // has completed
        List<Map<String, String>> taskConfigs = Arrays.asList(SAMPLE_CONFIGS.get(0), SAMPLE_CONFIGS.get(1));
        configStorage.putTaskConfigs("connector1", taskConfigs);

        // Validate root config by listing all connectors and tasks
        configState = configStorage.snapshot();
        assertEquals(3, configState.offset());
        String connectorName = CONNECTOR_IDS.get(0);
        assertEquals(Arrays.asList(connectorName), new ArrayList<>(configState.connectors()));
        assertEquals(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)), configState.tasks(connectorName));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.taskConfig(TASK_IDS.get(1)));
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigsStartsOnlyReconfiguredTasks() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        // Task configs should read to end, write to the log, read to end, write root, then read to end again
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(1), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(1),
                "properties", SAMPLE_CONFIGS.get(1));
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(2),
                "tasks", 2); // Starts with 0 tasks, after update has 2
        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)));
        EasyMock.expectLastCall();

        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedConfigs.put(TASK_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(1));
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(2));
        expectReadToEnd(serializedConfigs);

        // Task configs should read to end, write to the log, read to end, write root, then read to end again
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(2), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(3),
                "properties", SAMPLE_CONFIGS.get(2));
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                COMMIT_TASKS_CONFIG_KEYS.get(1), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(4),
                "tasks", 1); // Starts with 2 tasks, after update has 3

        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK_IDS.get(2)));
        EasyMock.expectLastCall();

        // Records to be read by consumer as it reads to the end of the log
        serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(TASK_CONFIG_KEYS.get(2), CONFIGS_SERIALIZED.get(3));
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(4));
        expectReadToEnd(serializedConfigs);

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Bootstrap as if we had already added the connector, but no tasks had been added yet
        whiteboxAddConnector(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), Collections.emptyList());
        whiteboxAddConnector(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(1), Collections.emptyList());

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertNull(configState.taskConfig(TASK_IDS.get(0)));
        assertNull(configState.taskConfig(TASK_IDS.get(1)));

        // Writing task configs should block until all the writes have been performed and the root record update
        // has completed
        List<Map<String, String>> taskConfigs = Arrays.asList(SAMPLE_CONFIGS.get(0), SAMPLE_CONFIGS.get(1));
        configStorage.putTaskConfigs("connector1", taskConfigs);
        taskConfigs = Collections.singletonList(SAMPLE_CONFIGS.get(2));
        configStorage.putTaskConfigs("connector2", taskConfigs);

        // Validate root config by listing all connectors and tasks
        configState = configStorage.snapshot();
        assertEquals(5, configState.offset());
        String connectorName1 = CONNECTOR_IDS.get(0);
        String connectorName2 = CONNECTOR_IDS.get(1);
        assertEquals(Arrays.asList(connectorName1, connectorName2), new ArrayList<>(configState.connectors()));
        assertEquals(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)), configState.tasks(connectorName1));
        assertEquals(Collections.singletonList(TASK_IDS.get(2)), configState.tasks(connectorName2));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.taskConfig(TASK_IDS.get(1)));
        assertEquals(SAMPLE_CONFIGS.get(2), configState.taskConfig(TASK_IDS.get(2)));
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigsZeroTasks() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        // Task configs should read to end, write to the log, read to end, write root.
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
            COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(0),
            "tasks", 0); // We have 0 tasks
        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Collections.emptyList());
        EasyMock.expectLastCall();

        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        expectReadToEnd(serializedConfigs);

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Bootstrap as if we had already added the connector, but no tasks had been added yet
        whiteboxAddConnector(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), Collections.emptyList());

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());

        // Writing task configs should block until all the writes have been performed and the root record update
        // has completed
        List<Map<String, String>> taskConfigs = Collections.emptyList();
        configStorage.putTaskConfigs("connector1", taskConfigs);

        // Validate root config by listing all connectors and tasks
        configState = configStorage.snapshot();
        assertEquals(1, configState.offset());
        String connectorName = CONNECTOR_IDS.get(0);
        assertEquals(Arrays.asList(connectorName), new ArrayList<>(configState.connectors()));
        assertEquals(Collections.emptyList(), configState.tasks(connectorName));
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestoreTargetState() throws Exception {
        expectConfigure();
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, TARGET_STATE_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(4), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0, 0, TARGET_STATE_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(5), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        // A worker running an older version wrote this target state; make sure we can handle it correctly
        deserialized.put(CONFIGS_SERIALIZED.get(3), TARGET_STATE_PAUSED_LEGACY);
        deserialized.put(CONFIGS_SERIALIZED.get(4), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        deserialized.put(CONFIGS_SERIALIZED.get(5), TARGET_STATE_STOPPED);
        logOffset = 6;

        expectStart(existingRecords, deserialized);

        // Shouldn't see any callbacks since this is during startup

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector with initial state paused
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(6, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        assertEquals(TargetState.PAUSED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(TargetState.STOPPED, configState.targetState(CONNECTOR_IDS.get(1)));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testBackgroundUpdateTargetState() throws Exception {
        // verify that we handle target state changes correctly when they come up through the log

        expectConfigure();
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserializedOnStartup = new LinkedHashMap<>();
        deserializedOnStartup.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserializedOnStartup.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserializedOnStartup.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserializedOnStartup.put(CONFIGS_SERIALIZED.get(3), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        logOffset = 5;

        expectStart(existingRecords, deserializedOnStartup);

        LinkedHashMap<String, byte[]> serializedAfterStartup = new LinkedHashMap<>();
        serializedAfterStartup.put(TARGET_STATE_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedAfterStartup.put(TARGET_STATE_KEYS.get(1), CONFIGS_SERIALIZED.get(1));

        Map<String, Struct> deserializedAfterStartup = new HashMap<>();
        deserializedAfterStartup.put(TARGET_STATE_KEYS.get(0), TARGET_STATE_PAUSED);
        deserializedAfterStartup.put(TARGET_STATE_KEYS.get(1), TARGET_STATE_STOPPED);

        expectRead(serializedAfterStartup, deserializedAfterStartup);

        configUpdateListener.onConnectorTargetStateChange(CONNECTOR_IDS.get(0));
        configUpdateListener.onConnectorTargetStateChange(CONNECTOR_IDS.get(1));
        EasyMock.expectLastCall();

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector with initial state started
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(Collections.singleton(CONNECTOR_IDS.get(0)), configStorage.connectorTargetStates.keySet());
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));

        // Should see two connectors now, one paused and one stopped
        configStorage.refresh(0, TimeUnit.SECONDS);
        configState = configStorage.snapshot();
        assertEquals(new HashSet<>(CONNECTOR_IDS), configStorage.connectorTargetStates.keySet());
        assertEquals(TargetState.PAUSED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(TargetState.STOPPED, configState.targetState(CONNECTOR_IDS.get(1)));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testSameTargetState() throws Exception {
        // verify that we handle target state changes correctly when they come up through the log

        expectConfigure();
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
            new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(3), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        logOffset = 5;

        expectStart(existingRecords, deserialized);

        // on resume update listener shouldn't be called
        configUpdateListener.onConnectorTargetStateChange(EasyMock.anyString());
        EasyMock.expectLastCall().andStubThrow(new AssertionError("unexpected call to onConnectorTargetStateChange"));

        expectRead(TARGET_STATE_KEYS.get(0), CONFIGS_SERIALIZED.get(0), TARGET_STATE_STARTED);

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector with initial state paused
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));

        configStorage.refresh(0, TimeUnit.SECONDS);

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testBackgroundConnectorDeletion() throws Exception {
        // verify that we handle connector deletions correctly when they come up through the log

        expectConfigure();
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(3), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        logOffset = 5;

        expectStart(existingRecords, deserialized);

        LinkedHashMap<String, byte[]> serializedData = new LinkedHashMap<>();
        serializedData.put(CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedData.put(TARGET_STATE_KEYS.get(0), CONFIGS_SERIALIZED.get(1));

        Map<String, Struct> deserializedData = new HashMap<>();
        deserializedData.put(CONNECTOR_CONFIG_KEYS.get(0), null);
        deserializedData.put(TARGET_STATE_KEYS.get(0), null);

        expectRead(serializedData, deserializedData);

        configUpdateListener.onConnectorConfigRemove(CONNECTOR_IDS.get(0));
        EasyMock.expectLastCall();

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector with initial state paused
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(new ConnectorTaskId(CONNECTOR_IDS.get(0), 0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.taskConfig(new ConnectorTaskId(CONNECTOR_IDS.get(0), 1)));
        assertEquals(2, configState.taskCount(CONNECTOR_IDS.get(0)));

        configStorage.refresh(0, TimeUnit.SECONDS);
        configState = configStorage.snapshot();
        // Connector should now be removed from the snapshot
        assertFalse(configState.contains(CONNECTOR_IDS.get(0)));
        assertEquals(0, configState.taskCount(CONNECTOR_IDS.get(0)));
        // Ensure that the deleted connector's deferred task updates have been cleaned up
        // in order to prevent unbounded growth of the map
        assertEquals(Collections.emptyMap(), configStorage.deferredTaskUpdates);

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestoreTargetStateUnexpectedDeletion() throws Exception {
        expectConfigure();
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, TARGET_STATE_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(4), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(3), null);
        deserialized.put(CONFIGS_SERIALIZED.get(4), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        logOffset = 5;

        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // The target state deletion should reset the state to STARTED
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(5, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestore() throws Exception {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.

        expectConfigure();
        // Overwrite each type at least once to ensure we see the latest data after loading
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_TASK_COUNT_RECORD_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(4), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(5), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 6, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_TASK_COUNT_RECORD_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(6), new RecordHeaders(), Optional.empty()),
                // Connector after root update should make it through, task update shouldn't
                new ConsumerRecord<>(TOPIC, 0, 7, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(7), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 8, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(8), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(3), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(4), CONNECTOR_CONFIG_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(5), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        deserialized.put(CONFIGS_SERIALIZED.get(6), CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(7), CONNECTOR_CONFIG_STRUCTS.get(2));
        deserialized.put(CONFIGS_SERIALIZED.get(8), TASK_CONFIG_STRUCTS.get(1));
        logOffset = 9;
        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector and its config should be the last one seen anywhere in the log
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(logOffset, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));
        // CONNECTOR_CONFIG_STRUCTS[2] -> SAMPLE_CONFIGS[2]
        assertEquals(SAMPLE_CONFIGS.get(2), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        // Should see 2 tasks for that connector. Only config updates before the root key update should be reflected
        assertEquals(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)), configState.tasks(CONNECTOR_IDS.get(0)));
        // Both TASK_CONFIG_STRUCTS[0] -> SAMPLE_CONFIGS[0]
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(1)));
        assertEquals(9, (int) configState.taskCountRecord(CONNECTOR_IDS.get(1)));
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());
        assertEquals(Collections.singleton("connector1"), configState.connectorsPendingFencing);

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestoreConnectorDeletion() throws Exception {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.

        expectConfigure();
        // Overwrite each type at least once to ensure we see the latest data after loading
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, TARGET_STATE_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(4), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(5), new RecordHeaders(), Optional.empty()));

        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(3), null);
        deserialized.put(CONFIGS_SERIALIZED.get(4), null);
        deserialized.put(CONFIGS_SERIALIZED.get(5), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);

        logOffset = 6;
        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector and its config should be the last one seen anywhere in the log
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(6, configState.offset()); // Should always be next to be read, even if uncommitted
        assertTrue(configState.connectors().isEmpty());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRestoreZeroTasks() throws Exception {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.
        expectConfigure();
        // Overwrite each type at least once to ensure we see the latest data after loading
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
            new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                    CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(4), new RecordHeaders(), Optional.empty()),
            // Connector after root update should make it through, task update shouldn't
            new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(5), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 6, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(6), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(TOPIC, 0, 7, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                    CONFIGS_SERIALIZED.get(7), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(3), CONNECTOR_CONFIG_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(4), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        deserialized.put(CONFIGS_SERIALIZED.get(5), CONNECTOR_CONFIG_STRUCTS.get(2));
        deserialized.put(CONFIGS_SERIALIZED.get(6), TASK_CONFIG_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(7), TASKS_COMMIT_STRUCT_ZERO_TASK_CONNECTOR);
        logOffset = 8;
        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Should see a single connector and its config should be the last one seen anywhere in the log
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(8, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        // CONNECTOR_CONFIG_STRUCTS[2] -> SAMPLE_CONFIGS[2]
        assertEquals(SAMPLE_CONFIGS.get(2), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        // Should see 0 tasks for that connector.
        assertEquals(Collections.emptyList(), configState.tasks(CONNECTOR_IDS.get(0)));
        // Both TASK_CONFIG_STRUCTS[0] -> SAMPLE_CONFIGS[0]
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigsDoesNotResolveAllInconsistencies() throws Exception {
        // Test a case where a failure and compaction has left us in an inconsistent state when reading the log.
        // We start out by loading an initial configuration where we started to write a task update, and then
        // compaction cleaned up the earlier record.

        expectConfigure();
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, CONNECTOR_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                // This is the record that has been compacted:
                //new ConsumerRecord<>(TOPIC, 0, 1, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(1)),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(4), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0, 0, TASK_CONFIG_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(5), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(4), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        deserialized.put(CONFIGS_SERIALIZED.get(5), TASK_CONFIG_STRUCTS.get(1));
        logOffset = 6;
        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);

        // Successful attempt to write new task config
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectReadToEnd(new LinkedHashMap<>());
        expectConvertWriteRead(
                COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(2),
                "tasks", 1); // Updated to just 1 task
        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Arrays.asList(TASK_IDS.get(0)));
        EasyMock.expectLastCall();
        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(2));
        expectReadToEnd(serializedConfigs);

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();
        // After reading the log, it should have been in an inconsistent state
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(6, configState.offset()); // Should always be next to be read, not last committed
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        // Inconsistent data should leave us with no tasks listed for the connector and an entry in the inconsistent list
        assertEquals(Collections.emptyList(), configState.tasks(CONNECTOR_IDS.get(0)));
        // Both TASK_CONFIG_STRUCTS[0] -> SAMPLE_CONFIGS[0]
        assertNull(configState.taskConfig(TASK_IDS.get(0)));
        assertNull(configState.taskConfig(TASK_IDS.get(1)));
        assertEquals(Collections.singleton(CONNECTOR_IDS.get(0)), configState.inconsistentConnectors());

        // Next, issue a write that has everything that is needed and it should be accepted. Note that in this case
        // we are going to shrink the number of tasks to 1
        configStorage.putTaskConfigs("connector1", Collections.singletonList(SAMPLE_CONFIGS.get(0)));
        // Validate updated config
        configState = configStorage.snapshot();
        // This is only two more ahead of the last one because multiple calls fail, and so their configs are not written
        // to the topic. Only the last call with 1 task config + 1 commit actually gets written.
        assertEquals(8, configState.offset());
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        assertEquals(Arrays.asList(TASK_IDS.get(0)), configState.tasks(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(0)));
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutRestartRequestOnlyFailed() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_IDS.get(0), true, false);
        testPutRestartRequest(restartRequest);
    }

    @Test
    public void testPutRestartRequestOnlyFailedIncludingTasks() throws Exception {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_IDS.get(0), true, true);
        testPutRestartRequest(restartRequest);
    }

    private void testPutRestartRequest(RestartRequest restartRequest) throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        expectConvertWriteAndRead(
                RESTART_CONNECTOR_KEYS.get(0), KafkaConfigBackingStore.RESTART_REQUEST_V0, CONFIGS_SERIALIZED.get(0),
                ONLY_FAILED_FIELD_NAME, restartRequest.onlyFailed());
        final Capture<RestartRequest> capturedRestartRequest = EasyMock.newCapture();
        configUpdateListener.onRestartRequest(EasyMock.capture(capturedRestartRequest));
        EasyMock.expectLastCall();

        expectPartitionCount(1);
        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        // Writing should block until it is written and read back from Kafka
        configStorage.putRestartRequest(restartRequest);

        assertEquals(restartRequest.connectorName(), capturedRestartRequest.getValue().connectorName());
        assertEquals(restartRequest.onlyFailed(), capturedRestartRequest.getValue().onlyFailed());
        assertEquals(restartRequest.includeTasks(), capturedRestartRequest.getValue().includeTasks());

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testRecordToRestartRequest() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(0),
                CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty());
        Struct struct = RESTART_REQUEST_STRUCTS.get(0);
        SchemaAndValue schemaAndValue = new SchemaAndValue(struct.schema(), structToMap(struct));
        RestartRequest restartRequest = configStorage.recordToRestartRequest(record, schemaAndValue);
        assertEquals(CONNECTOR_1_NAME, restartRequest.connectorName());
        assertEquals(struct.getBoolean(INCLUDE_TASKS_FIELD_NAME), restartRequest.includeTasks());
        assertEquals(struct.getBoolean(ONLY_FAILED_FIELD_NAME), restartRequest.onlyFailed());
    }

    @Test
    public void testRecordToRestartRequestOnlyFailedInconsistent() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(0),
                CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty());
        Struct struct = ONLY_FAILED_MISSING_STRUCT;
        SchemaAndValue schemaAndValue = new SchemaAndValue(struct.schema(), structToMap(struct));
        RestartRequest restartRequest = configStorage.recordToRestartRequest(record, schemaAndValue);
        assertEquals(CONNECTOR_1_NAME, restartRequest.connectorName());
        assertEquals(struct.getBoolean(INCLUDE_TASKS_FIELD_NAME), restartRequest.includeTasks());
        assertFalse(restartRequest.onlyFailed());
    }

    @Test
    public void testRecordToRestartRequestIncludeTasksInconsistent() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(0),
                CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty());
        Struct struct = INCLUDE_TASKS_MISSING_STRUCT;
        SchemaAndValue schemaAndValue = new SchemaAndValue(struct.schema(), structToMap(struct));
        RestartRequest restartRequest = configStorage.recordToRestartRequest(record, schemaAndValue);
        assertEquals(CONNECTOR_1_NAME, restartRequest.connectorName());
        assertFalse(restartRequest.includeTasks());
        assertEquals(struct.getBoolean(ONLY_FAILED_FIELD_NAME), restartRequest.onlyFailed());
    }

    @Test
    public void testRestoreRestartRequestInconsistentState() throws Exception {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state doesnt prevent startup.

        expectConfigure();
        // Overwrite each type at least once to ensure we see the latest data after loading
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(0),
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(2), new RecordHeaders(), Optional.empty()),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0, RESTART_CONNECTOR_KEYS.get(1),
                        CONFIGS_SERIALIZED.get(3), new RecordHeaders(), Optional.empty()));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), RESTART_REQUEST_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), RESTART_REQUEST_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(2), RESTART_REQUEST_STRUCTS.get(2));
        deserialized.put(CONFIGS_SERIALIZED.get(3), null);
        logOffset = 4;
        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutLogLevel() throws Exception {
        final String logger1 = "org.apache.zookeeper";
        final String logger2 = "org.apache.cassandra";
        final String logger3 = "org.apache.kafka.clients";
        final String logger4 = "org.apache.kafka.connect";
        final String level1 = "ERROR";
        final String level3 = "WARN";
        final String level4 = "DEBUG";

        final Struct existingLogLevel = new Struct(KafkaConfigBackingStore.LOGGER_LEVEL_V0)
                .put("level", level1);

        // Pre-populate the config topic with a couple of logger level records; these should be ignored (i.e.,
        // not reported to the update listener)
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, "logger-cluster-" + logger1,
                        CONFIGS_SERIALIZED.get(0), new RecordHeaders(), Optional.empty()
                ),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, "logger-cluster-" + logger2,
                        CONFIGS_SERIALIZED.get(1), new RecordHeaders(), Optional.empty()
                )
        );
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap<>();
        deserialized.put(CONFIGS_SERIALIZED.get(0), existingLogLevel);
        // Make sure we gracefully handle tombstones
        deserialized.put(CONFIGS_SERIALIZED.get(1), null);
        logOffset = 2;

        expectConfigure();
        expectStart(existingRecords, deserialized);
        expectPartitionCount(1);
        expectStop();

        expectConvertWriteRead(
                "logger-cluster-" + logger3, KafkaConfigBackingStore.LOGGER_LEVEL_V0, CONFIGS_SERIALIZED.get(2),
                "level", level3);
        expectConvertWriteRead(
                "logger-cluster-" + logger4, KafkaConfigBackingStore.LOGGER_LEVEL_V0, CONFIGS_SERIALIZED.get(3),
                "level", level4);

        LinkedHashMap<String, byte[]> newRecords = new LinkedHashMap<>();
        newRecords.put("logger-cluster-" + logger3, CONFIGS_SERIALIZED.get(2));
        newRecords.put("logger-cluster-" + logger4, CONFIGS_SERIALIZED.get(3));
        expectReadToEnd(newRecords);

        configUpdateListener.onLoggingLevelUpdate(logger3, level3);
        EasyMock.expectLastCall();
        configUpdateListener.onLoggingLevelUpdate(logger4, level4);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        configStorage.start();

        configStorage.putLoggerLevel(logger3, level3);
        configStorage.putLoggerLevel(logger4, level4);
        configStorage.refresh(0, TimeUnit.SECONDS);

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testExceptionOnStartWhenConfigTopicHasMultiplePartitions() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList(), Collections.emptyMap());

        expectPartitionCount(2);

        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        ConfigException e = assertThrows(ConfigException.class, () -> configStorage.start());
        assertTrue(e.getMessage().contains("required to have a single partition"));

        PowerMock.verifyAll();
    }

    @Test
    public void testFencableProducerPropertiesInsertedByDefault() throws Exception {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        String groupId = "my-connect-cluster";
        props.put(GROUP_ID_CONFIG, groupId);
        props.remove(TRANSACTIONAL_ID_CONFIG);
        props.remove(ENABLE_IDEMPOTENCE_CONFIG);
        createStore();

        PowerMock.replayAll();

        Map<String, Object> fencableProducerProperties = configStorage.fencableProducerProps(config);
        assertEquals("connect-cluster-" + groupId, fencableProducerProperties.get(TRANSACTIONAL_ID_CONFIG));
        assertEquals("true", fencableProducerProperties.get(ENABLE_IDEMPOTENCE_CONFIG));

        PowerMock.verifyAll();
    }

    @Test
    public void testFencableProducerPropertiesOverrideUserSuppliedValues() throws Exception {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        String groupId = "my-other-connect-cluster";
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(TRANSACTIONAL_ID_CONFIG, "my-custom-transactional-id");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "false");
        createStore();

        PowerMock.replayAll();

        Map<String, Object> fencableProducerProperties = configStorage.fencableProducerProps(config);
        assertEquals("connect-cluster-" + groupId, fencableProducerProperties.get(TRANSACTIONAL_ID_CONFIG));
        assertEquals("true", fencableProducerProperties.get(ENABLE_IDEMPOTENCE_CONFIG));

        PowerMock.verifyAll();
    }

    @Test
    public void testConsumerPropertiesInsertedByDefaultWithExactlyOnceSourceEnabled() throws Exception {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        props.remove(ISOLATION_LEVEL_CONFIG);
        createStore();

        expectConfigure();
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        assertEquals(
                IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );

        PowerMock.verifyAll();
    }

    @Test
    public void testConsumerPropertiesOverrideUserSuppliedValuesWithExactlyOnceSourceEnabled() throws Exception {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        props.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT));
        createStore();

        expectConfigure();
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        assertEquals(
                IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );

        PowerMock.verifyAll();
    }

    @Test
    public void testConsumerPropertiesNotInsertedByDefaultWithoutExactlyOnceSourceEnabled() throws Exception {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        props.remove(ISOLATION_LEVEL_CONFIG);
        createStore();

        expectConfigure();
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        assertNull(capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG));

        PowerMock.verifyAll();
    }

    @Test
    public void testConsumerPropertiesDoNotOverrideUserSuppliedValuesWithoutExactlyOnceSourceEnabled() throws Exception {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        props.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT));
        createStore();

        expectConfigure();
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        assertEquals(
                IsolationLevel.READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );

        PowerMock.verifyAll();
    }

    @Test
    public void testClientIds() throws Exception {
        props = new HashMap<>(DEFAULT_CONFIG_STORAGE_PROPS);
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        createStore();

        expectConfigure();
        PowerMock.replayAll();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        Map<String, Object> fencableProducerProps = configStorage.fencableProducerProps(config);

        final String expectedClientId = CLIENT_ID_BASE + "configs";
        assertEquals(expectedClientId, capturedProducerProps.getValue().get(CLIENT_ID_CONFIG));
        assertEquals(expectedClientId, capturedConsumerProps.getValue().get(CLIENT_ID_CONFIG));
        assertEquals(expectedClientId + "-leader", fencableProducerProps.get(CLIENT_ID_CONFIG));

        PowerMock.verifyAll();
    }

    private void expectConfigure() throws Exception {
        PowerMock.expectPrivate(configStorage, "createKafkaBasedLog",
                EasyMock.capture(capturedTopic), EasyMock.capture(capturedProducerProps),
                EasyMock.capture(capturedConsumerProps), EasyMock.capture(capturedConsumedCallback),
                EasyMock.capture(capturedNewTopic), EasyMock.capture(capturedAdminSupplier),
                EasyMock.anyObject(WorkerConfig.class), EasyMock.anyObject(Time.class))
                .andReturn(storeLog);
    }

    private void expectFencableProducer() throws Exception {
        fencableProducer.initTransactions();
        EasyMock.expectLastCall();
        PowerMock.expectPrivate(configStorage, "createFencableProducer")
                .andReturn(fencableProducer);
    }

    private void expectPartitionCount(int partitionCount) {
        EasyMock.expect(storeLog.partitionCount())
                .andReturn(partitionCount);
    }

    // If non-empty, deserializations should be a LinkedHashMap
    private void expectStart(final List<ConsumerRecord<String, byte[]>> preexistingRecords,
                             final Map<byte[], Struct> deserializations) {
        storeLog.start();
        PowerMock.expectLastCall().andAnswer(() -> {
            for (ConsumerRecord<String, byte[]> rec : preexistingRecords)
                capturedConsumedCallback.getValue().onCompletion(null, rec);
            return null;
        });
        for (Map.Entry<byte[], Struct> deserializationEntry : deserializations.entrySet()) {
            // Note null schema because default settings for internal serialization are schema-less
            EasyMock.expect(converter.toConnectData(EasyMock.eq(TOPIC), EasyMock.aryEq(deserializationEntry.getKey())))
                    .andReturn(new SchemaAndValue(null, structToMap(deserializationEntry.getValue())));
        }
    }

    private void expectStop() {
        storeLog.stop();
        PowerMock.expectLastCall();
    }

    private void expectRead(LinkedHashMap<String, byte[]> serializedValues,
                            Map<String, Struct> deserializedValues) {
        expectReadToEnd(serializedValues);
        for (Map.Entry<String, Struct> deserializedValueEntry : deserializedValues.entrySet()) {
            byte[] serializedValue = serializedValues.get(deserializedValueEntry.getKey());
            EasyMock.expect(converter.toConnectData(EasyMock.eq(TOPIC), EasyMock.aryEq(serializedValue)))
                    .andReturn(new SchemaAndValue(null, structToMap(deserializedValueEntry.getValue())));
        }
    }

    private void expectRead(final String key, final byte[] serializedValue, Struct deserializedValue) {
        LinkedHashMap<String, byte[]> serializedData = new LinkedHashMap<>();
        serializedData.put(key, serializedValue);
        expectRead(serializedData, Collections.singletonMap(key, deserializedValue));
    }

    private void expectConvert(Schema valueSchema, Struct valueStruct, byte[] serialized) {
        EasyMock.expect(converter.fromConnectData(EasyMock.eq(TOPIC), EasyMock.eq(valueSchema), EasyMock.eq(valueStruct)))
                .andReturn(serialized);
    }

    // Expect a conversion & write to the underlying log, followed by a subsequent read when the data is consumed back
    // from the log. Validate the data that is captured when the conversion is performed matches the specified data
    // (by checking a single field's value)
    private void expectConvertWriteRead(final String configKey, final Schema valueSchema, final byte[] serialized,
                                        final String dataFieldName, final Object dataFieldValue) throws Exception {
        final Capture<Struct> capturedRecord = EasyMock.newCapture();
        if (serialized != null)
            EasyMock.expect(converter.fromConnectData(EasyMock.eq(TOPIC), EasyMock.eq(valueSchema), EasyMock.capture(capturedRecord)))
                    .andReturn(serialized);

        storeLog.sendWithReceipt(EasyMock.eq(configKey), EasyMock.aryEq(serialized));
        EasyMock.expectLastCall().andReturn(producerFuture);

        producerFuture.get(EasyMock.anyLong(), EasyMock.anyObject());
        EasyMock.expectLastCall().andReturn(null);

        EasyMock.expect(converter.toConnectData(EasyMock.eq(TOPIC), EasyMock.aryEq(serialized)))
                .andAnswer(() -> {
                    if (dataFieldName != null)
                        assertEquals(dataFieldValue, capturedRecord.getValue().get(dataFieldName));
                    // Note null schema because default settings for internal serialization are schema-less
                    return new SchemaAndValue(null, serialized == null ? null : structToMap(capturedRecord.getValue()));
                });
    }

    private void expectConvertRead(final String configKey, final Struct struct, final byte[] serialized) {
        EasyMock.expect(converter.toConnectData(EasyMock.eq(TOPIC), EasyMock.aryEq(serialized)))
                .andAnswer(() -> new SchemaAndValue(null, serialized == null ? null : structToMap(struct)));
        LinkedHashMap<String, byte[]> recordsToRead = new LinkedHashMap<>();
        recordsToRead.put(configKey, serialized);
        expectReadToEnd(recordsToRead);
    }

    // This map needs to maintain ordering
    private void expectReadToEnd(final LinkedHashMap<String, byte[]> serializedConfigs) {
        EasyMock.expect(storeLog.readToEnd())
                .andAnswer(() -> {
                    TestFuture<Void> future = new TestFuture<>();
                    for (Map.Entry<String, byte[]> entry : serializedConfigs.entrySet()) {
                        capturedConsumedCallback.getValue().onCompletion(null,
                            new ConsumerRecord<>(TOPIC, 0, logOffset++, 0L, TimestampType.CREATE_TIME, 0, 0,
                                entry.getKey(), entry.getValue(), new RecordHeaders(), Optional.empty()));
                    }
                    future.resolveOnGet((Void) null);
                    return future;
                });
    }

    private void expectConnectorRemoval(String configKey, String targetStateKey) throws Exception {
        expectConvertWriteRead(configKey, KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, null, null, null);
        expectConvertWriteRead(targetStateKey, KafkaConfigBackingStore.TARGET_STATE_V0, null, null, null);

        LinkedHashMap<String, byte[]> recordsToRead = new LinkedHashMap<>();
        recordsToRead.put(configKey, null);
        recordsToRead.put(targetStateKey, null);
        expectReadToEnd(recordsToRead);
    }

    private void expectConvertWriteAndRead(final String configKey, final Schema valueSchema, final byte[] serialized,
                                           final String dataFieldName, final Object dataFieldValue) throws Exception {
        expectConvertWriteRead(configKey, valueSchema, serialized, dataFieldName, dataFieldValue);
        LinkedHashMap<String, byte[]> recordsToRead = new LinkedHashMap<>();
        recordsToRead.put(configKey, serialized);
        expectReadToEnd(recordsToRead);
    }

    // Manually insert a connector into config storage, updating the task configs, connector config, and root config
    private void whiteboxAddConnector(String connectorName, Map<String, String> connectorConfig, List<Map<String, String>> taskConfigs) {
        Map<ConnectorTaskId, Map<String, String>> storageTaskConfigs = Whitebox.getInternalState(configStorage, "taskConfigs");
        for (int i = 0; i < taskConfigs.size(); i++)
            storageTaskConfigs.put(new ConnectorTaskId(connectorName, i), taskConfigs.get(i));

        Map<String, Map<String, String>> connectorConfigs = Whitebox.getInternalState(configStorage, "connectorConfigs");
        connectorConfigs.put(connectorName, connectorConfig);

        Whitebox.<Map<String, Integer>>getInternalState(configStorage, "connectorTaskCounts").put(connectorName, taskConfigs.size());
    }

    // Generates a Map representation of Struct. Only does shallow traversal, so nested structs are not converted
    private Map<String, Object> structToMap(Struct struct) {
        if (struct == null)
            return null;

        HashMap<String, Object> result = new HashMap<>();
        for (Field field : struct.schema().fields())
            result.put(field.name(), struct.get(field));
        return result;
    }
}
