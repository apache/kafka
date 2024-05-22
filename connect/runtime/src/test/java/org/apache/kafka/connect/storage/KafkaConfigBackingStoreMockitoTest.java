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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.INCLUDE_TASKS_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.ONLY_FAILED_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.READ_WRITE_TOTAL_TIMEOUT_MS;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.RESTART_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KafkaConfigBackingStoreMockitoTest {
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

    private static final List<String> TARGET_STATE_KEYS = Arrays.asList("target-state-connector1", "target-state-connector2");
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
    private static final List<Struct> TASK_CONFIG_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.TASK_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(0)),
            new Struct(KafkaConfigBackingStore.TASK_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(1))
    );
    private static final Struct ONLY_FAILED_MISSING_STRUCT = new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(INCLUDE_TASKS_FIELD_NAME, false);
    private static final Struct INCLUDE_TASKS_MISSING_STRUCT = new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(ONLY_FAILED_FIELD_NAME, true);
    private static final List<Struct> RESTART_REQUEST_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(ONLY_FAILED_FIELD_NAME, true).put(INCLUDE_TASKS_FIELD_NAME, false),
            ONLY_FAILED_MISSING_STRUCT,
            INCLUDE_TASKS_MISSING_STRUCT);

    private static final List<Struct> CONNECTOR_CONFIG_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(0)),
            new Struct(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(1)),
            new Struct(KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0).put("properties", SAMPLE_CONFIGS.get(2))
    );

    private static final Struct TARGET_STATE_PAUSED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V1)
            .put("state", "PAUSED")
            .put("state.v2", "PAUSED");
    private static final Struct TARGET_STATE_PAUSED_LEGACY = new Struct(KafkaConfigBackingStore.TARGET_STATE_V0)
            .put("state", "PAUSED");
    private static final Struct TARGET_STATE_STOPPED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V1)
            .put("state", "PAUSED")
            .put("state.v2", "STOPPED");
    private static final List<Struct> CONNECTOR_TASK_COUNT_RECORD_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.TASK_COUNT_RECORD_V0).put("task-count", 6),
            new Struct(KafkaConfigBackingStore.TASK_COUNT_RECORD_V0).put("task-count", 9)
    );

    // The exact format doesn't matter here since both conversions are mocked
    private static final List<byte[]> CONFIGS_SERIALIZED = Arrays.asList(
            "config-bytes-1".getBytes(), "config-bytes-2".getBytes(), "config-bytes-3".getBytes(),
            "config-bytes-4".getBytes(), "config-bytes-5".getBytes(), "config-bytes-6".getBytes(),
            "config-bytes-7".getBytes(), "config-bytes-8".getBytes(), "config-bytes-9".getBytes()
    );
    private static final Struct TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 2);

    private static final Struct TASKS_COMMIT_STRUCT_ZERO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 0);

    private static final List<byte[]> TARGET_STATES_SERIALIZED = Arrays.asList(
            "started".getBytes(), "paused".getBytes(), "stopped".getBytes()
    );
    @Mock
    private Converter converter;
    @Mock
    private ConfigBackingStore.UpdateListener configUpdateListener;
    private Map<String, String> props = new HashMap<>(DEFAULT_CONFIG_STORAGE_PROPS);
    private DistributedConfig config;
    @Mock
    KafkaBasedLog<String, byte[]> configLog;
    @Mock
    Producer<String, byte[]> fencableProducer;
    @Mock
    Future<RecordMetadata> producerFuture;
    private KafkaConfigBackingStore configStorage;

    private final ArgumentCaptor<String> capturedTopic = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<Map<String, Object>> capturedConsumerProps = ArgumentCaptor.forClass(Map.class);
    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<Map<String, Object>> capturedProducerProps = ArgumentCaptor.forClass(Map.class);
    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<Supplier<TopicAdmin>> capturedAdminSupplier = ArgumentCaptor.forClass(Supplier.class);
    private final ArgumentCaptor<NewTopic> capturedNewTopic = ArgumentCaptor.forClass(NewTopic.class);
    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<Callback<ConsumerRecord<String, byte[]>>> capturedConsumedCallback = ArgumentCaptor.forClass(Callback.class);

    private final MockTime time = new MockTime();
    private long logOffset = 0;

    private void createStore() {
        config = Mockito.spy(new DistributedConfig(props));
        doReturn("test-cluster").when(config).kafkaClusterId();
        configStorage = Mockito.spy(
                new KafkaConfigBackingStore(
                        converter, config, null, () -> null, CLIENT_ID_BASE, time)
        );
        configStorage.setConfigLog(configLog);
        configStorage.setUpdateListener(configUpdateListener);
    }

    @Before
    public void setUp() {
        createStore();
    }

    @Test
    public void testStartStop() {
        props.put("config.storage.min.insync.replicas", "3");
        props.put("config.storage.max.message.bytes", "1001");
        createStore();

        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);

        verifyConfigure();
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

        verify(configLog).start();
        verify(configLog).stop();
    }

    @Test
    public void testSnapshotCannotMutateInternalState() {
        props.put("config.storage.min.insync.replicas", "3");
        props.put("config.storage.max.message.bytes", "1001");
        createStore();

        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

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
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));

        String configKey = CONNECTOR_CONFIG_KEYS.get(1);
        String targetStateKey = TARGET_STATE_KEYS.get(1);

        doAnswer(expectReadToEnd(Collections.singletonMap(CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0))))
                .doAnswer(expectReadToEnd(Collections.singletonMap(CONNECTOR_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(1))))
                // Config deletion
                .doAnswer(expectReadToEnd(new LinkedHashMap<String, byte[]>() {{
                            put(configKey, null);
                            put(targetStateKey, null);
                        }})
                ).when(configLog).readToEnd();

        // Writing should block until it is written and read back from Kafka
        expectConvertWriteRead(
                CONNECTOR_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));

        configStorage.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        configState = configStorage.snapshot();

        assertEquals(1, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));
        verify(configUpdateListener).onConnectorConfigUpdate(CONNECTOR_IDS.get(0));

        // Second should also block and all configs should still be available
        expectConvertWriteRead(
                CONNECTOR_CONFIG_KEYS.get(1), KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(1),
                "properties", SAMPLE_CONFIGS.get(1));

        configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(1), null);
        configState = configStorage.snapshot();

        assertEquals(2, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.connectorConfig(CONNECTOR_IDS.get(1)));
        verify(configUpdateListener).onConnectorConfigUpdate(CONNECTOR_IDS.get(1));

        // Config deletion
        when(producerFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);
        when(converter.toConnectData(TOPIC, null)).thenReturn(new SchemaAndValue(null, null));
        when(configLog.sendWithReceipt(AdditionalMatchers.or(Mockito.eq(configKey), Mockito.eq(targetStateKey)),
                Mockito.isNull())).thenReturn(producerFuture);

        // Deletion should remove the second one we added
        configStorage.removeConnectorConfig(CONNECTOR_IDS.get(1));
        configState = configStorage.snapshot();

        assertEquals(4, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));
        assertNull(configState.targetState(CONNECTOR_IDS.get(1)));
        verify(configUpdateListener).onConnectorConfigRemove(CONNECTOR_IDS.get(1));

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testPutConnectorConfigWithTargetState() throws Exception {
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.targetState(CONNECTOR_IDS.get(0)));

        doAnswer(expectReadToEnd(new LinkedHashMap<String, byte[]>() {{
                    put(TARGET_STATE_KEYS.get(0), TARGET_STATES_SERIALIZED.get(2));
                    put(CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
                }})
        ).when(configLog).readToEnd();

        // We expect to write the target state first, followed by the config write and then a read to end
        expectConvertWriteRead(
                TARGET_STATE_KEYS.get(0), KafkaConfigBackingStore.TARGET_STATE_V1, TARGET_STATES_SERIALIZED.get(2),
                "state.v2", TargetState.STOPPED.name());

        expectConvertWriteRead(
                CONNECTOR_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));

        // Writing should block until it is written and read back from Kafka
        configStorage.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), TargetState.STOPPED);
        configState = configStorage.snapshot();
        assertEquals(2, configState.offset());
        assertEquals(TargetState.STOPPED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));

        // We don't expect the config update listener's onConnectorTargetStateChange hook to be invoked
        verify(configUpdateListener, never()).onConnectorTargetStateChange(anyString());

        verify(configUpdateListener).onConnectorConfigUpdate(CONNECTOR_IDS.get(0));

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testPutConnectorConfigProducerError() throws Exception {
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        when(converter.fromConnectData(TOPIC, KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONNECTOR_CONFIG_STRUCTS.get(0)))
                .thenReturn(CONFIGS_SERIALIZED.get(0));
        when(configLog.sendWithReceipt(anyString(), any(byte[].class))).thenReturn(producerFuture);

        // Verify initial state
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertEquals(0, configState.connectors().size());

        Exception thrownException = new ExecutionException(new TopicAuthorizationException(Collections.singleton("test")));
        when(producerFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(thrownException);

        // verify that the producer exception from KafkaBasedLog::send is propagated
        ConnectException e = assertThrows(ConnectException.class, () -> configStorage.putConnectorConfig(CONNECTOR_IDS.get(0),
                SAMPLE_CONFIGS.get(0), null));
        assertTrue(e.getMessage().contains("Error writing connector configuration to Kafka"));
        assertEquals(thrownException, e.getCause());

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testRemoveConnectorConfigSlowProducer() throws Exception {
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> connectorConfigProducerFuture = mock(Future.class);

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> targetStateProducerFuture = mock(Future.class);

        when(configLog.sendWithReceipt(anyString(), isNull()))
                // tombstone for the connector config
                .thenReturn(connectorConfigProducerFuture)
                // tombstone for the connector target state
                .thenReturn(targetStateProducerFuture);

        when(connectorConfigProducerFuture.get(eq(READ_WRITE_TOTAL_TIMEOUT_MS), any(TimeUnit.class)))
                .thenAnswer((Answer<RecordMetadata>) invocation -> {
                    time.sleep(READ_WRITE_TOTAL_TIMEOUT_MS - 1000);
                    return null;
                });

        // the future get timeout is expected to be reduced according to how long the previous Future::get took
        when(targetStateProducerFuture.get(eq(1000L), any(TimeUnit.class)))
                .thenAnswer((Answer<RecordMetadata>) invocation -> {
                    time.sleep(1000);
                    return null;
                });

        @SuppressWarnings("unchecked")
        Future<Void> future = mock(Future.class);
        when(configLog.readToEnd()).thenReturn(future);

        // the Future::get calls on the previous two producer futures exhausted the overall timeout; so expect the
        // timeout on the log read future to be 0
        when(future.get(eq(0L), any(TimeUnit.class))).thenReturn(null);

        configStorage.removeConnectorConfig("test-connector");
        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWritePrivileges() throws Exception {
        // With exactly.once.source.support = preparing (or also, "enabled"), we need to use a transactional producer
        // to write some types of messages to the config topic
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        createStore();

        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // Try and fail to write a task count record to the config topic without write privileges
        when(converter.fromConnectData(TOPIC, KafkaConfigBackingStore.TASK_COUNT_RECORD_V0, CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(0)))
                .thenReturn(CONFIGS_SERIALIZED.get(0));

        // Should fail the first time since we haven't claimed write privileges
        assertThrows(IllegalStateException.class, () -> configStorage.putTaskCountRecord(CONNECTOR_IDS.get(0), 6));

        // Claim write privileges
        doReturn(fencableProducer).when(configStorage).createFencableProducer();
        // And write the task count record successfully
        when(fencableProducer.send(any(ProducerRecord.class))).thenReturn(null);
        doAnswer(expectReadToEnd(Collections.singletonMap(CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0))))
                .doAnswer(expectReadToEnd(Collections.singletonMap(CONNECTOR_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(2))))
                .when(configLog).readToEnd();
        when(converter.toConnectData(TOPIC, CONFIGS_SERIALIZED.get(0)))
                .thenReturn(new SchemaAndValue(null, structToMap(CONNECTOR_TASK_COUNT_RECORD_STRUCTS.get(0))));

        // Should succeed now
        configStorage.claimWritePrivileges();
        configStorage.putTaskCountRecord(CONNECTOR_IDS.get(0), 6);

        verify(fencableProducer).beginTransaction();
        verify(fencableProducer).commitTransaction();

        // Try to write a connector config
        when(converter.fromConnectData(TOPIC, KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, CONNECTOR_CONFIG_STRUCTS.get(0)))
                .thenReturn(CONFIGS_SERIALIZED.get(1));
        // Get fenced out
        doThrow(new ProducerFencedException("Better luck next time"))
                .doNothing()
                .when(fencableProducer).commitTransaction();

        // Should fail again when we get fenced out
        assertThrows(PrivilegedWriteException.class, () -> configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(0), null));

        verify(fencableProducer, times(2)).beginTransaction();
        verify(fencableProducer).close(Duration.ZERO);

        // Should fail if we retry without reclaiming write privileges
        assertThrows(IllegalStateException.class, () -> configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(0), null));

        // In the meantime, write a target state (which doesn't require write privileges)
        when(converter.fromConnectData(TOPIC, KafkaConfigBackingStore.TARGET_STATE_V1, TARGET_STATE_PAUSED))
                .thenReturn(CONFIGS_SERIALIZED.get(1));
        when(configLog.sendWithReceipt("target-state-" + CONNECTOR_IDS.get(1), CONFIGS_SERIALIZED.get(1)))
                .thenReturn(producerFuture);
        when(producerFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);

        // Should succeed even without write privileges (target states can be written by anyone)
        configStorage.putTargetState(CONNECTOR_IDS.get(1), TargetState.PAUSED);

        // Reclaim write privileges and successfully write the config
        when(converter.toConnectData(TOPIC, CONFIGS_SERIALIZED.get(2)))
                .thenReturn(new SchemaAndValue(null, structToMap(CONNECTOR_CONFIG_STRUCTS.get(0))));

        // Should succeed if we re-claim write privileges
        configStorage.claimWritePrivileges();
        configStorage.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(0), null);

        verify(fencableProducer, times(3)).beginTransaction();
        verify(fencableProducer, times(3)).commitTransaction();
        verify(configUpdateListener).onConnectorConfigUpdate(CONNECTOR_IDS.get(1));

        configStorage.stop();
        verify(configLog).stop();
        verify(configStorage, times(2)).createFencableProducer();
        verify(fencableProducer, times(2)).close(Duration.ZERO);
    }

    @Test
    public void testRestoreTargetStateUnexpectedDeletion() {
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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // The target state deletion should reset the state to STARTED
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(5, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Collections.singletonList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testRestoreTargetState() {
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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // Should see a single connector with initial state paused
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(6, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Collections.singletonList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        assertEquals(TargetState.PAUSED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(TargetState.STOPPED, configState.targetState(CONNECTOR_IDS.get(1)));

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testRestore() {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.

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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // Should see a single connector and its config should be the last one seen anywhere in the log
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(logOffset, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Collections.singletonList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
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

        // Shouldn't see any callbacks since this is during startup
        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testRestoreConnectorDeletion() {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.

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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
        configStorage.start();

        // Should see a single connector and its config should be the last one seen anywhere in the log
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(6, configState.offset()); // Should always be next to be read, even if uncommitted
        assertTrue(configState.connectors().isEmpty());

        // Shouldn't see any callbacks since this is during startup
        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testRestoreZeroTasks()  {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.

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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
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

        // Shouldn't see any callbacks since this is during startup
        configStorage.stop();
        verify(configLog).stop();
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
    public void testFencableProducerPropertiesOverrideUserSuppliedValues() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        String groupId = "my-other-connect-cluster";
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(TRANSACTIONAL_ID_CONFIG, "my-custom-transactional-id");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "false");
        createStore();

        Map<String, Object> fencableProducerProperties = configStorage.fencableProducerProps(config);
        assertEquals("connect-cluster-" + groupId, fencableProducerProperties.get(TRANSACTIONAL_ID_CONFIG));
        assertEquals("true", fencableProducerProperties.get(ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    public void testConsumerPropertiesDoNotOverrideUserSuppliedValuesWithoutExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        props.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString());
        createStore();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        assertEquals(
                IsolationLevel.READ_UNCOMMITTED.toString(),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );
    }

    @Test
    public void testClientIds() {
        props = new HashMap<>(DEFAULT_CONFIG_STORAGE_PROPS);
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        createStore();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        Map<String, Object> fencableProducerProps = configStorage.fencableProducerProps(config);

        final String expectedClientId = CLIENT_ID_BASE + "configs";
        assertEquals(expectedClientId, capturedProducerProps.getValue().get(CLIENT_ID_CONFIG));
        assertEquals(expectedClientId, capturedConsumerProps.getValue().get(CLIENT_ID_CONFIG));
        assertEquals(expectedClientId + "-leader", fencableProducerProps.get(CLIENT_ID_CONFIG));
    }

    @Test
    public void testExceptionOnStartWhenConfigTopicHasMultiplePartitions() {
        when(configLog.partitionCount()).thenReturn(2);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        ConfigException e = assertThrows(ConfigException.class, () -> configStorage.start());
        assertTrue(e.getMessage().contains("required to have a single partition"));
    }

    @Test
    public void testFencableProducerPropertiesInsertedByDefault() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        String groupId = "my-connect-cluster";
        props.put(GROUP_ID_CONFIG, groupId);
        props.remove(TRANSACTIONAL_ID_CONFIG);
        props.remove(ENABLE_IDEMPOTENCE_CONFIG);
        createStore();

        Map<String, Object> fencableProducerProperties = configStorage.fencableProducerProps(config);
        assertEquals("connect-cluster-" + groupId, fencableProducerProperties.get(TRANSACTIONAL_ID_CONFIG));
        assertEquals("true", fencableProducerProperties.get(ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    public void testConsumerPropertiesInsertedByDefaultWithExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        props.remove(ISOLATION_LEVEL_CONFIG);
        createStore();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        assertEquals(
                IsolationLevel.READ_COMMITTED.toString(),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );
    }

    @Test
    public void testConsumerPropertiesOverrideUserSuppliedValuesWithExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        props.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString());
        createStore();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        assertEquals(
                IsolationLevel.READ_COMMITTED.toString(),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );
    }

    @Test
    public void testConsumerPropertiesNotInsertedByDefaultWithoutExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        props.remove(ISOLATION_LEVEL_CONFIG);
        createStore();

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        assertNull(capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void testBackgroundConnectorDeletion() throws Exception {
        // verify that we handle connector deletions correctly when they come up through the log
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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        configStorage.start();
        verify(configLog).start();

        // Should see a single connector with initial state paused
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(new ConnectorTaskId(CONNECTOR_IDS.get(0), 0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.taskConfig(new ConnectorTaskId(CONNECTOR_IDS.get(0), 1)));
        assertEquals(2, configState.taskCount(CONNECTOR_IDS.get(0)));

        LinkedHashMap<String, byte[]> serializedData = new LinkedHashMap<>();
        serializedData.put(CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedData.put(TARGET_STATE_KEYS.get(0), CONFIGS_SERIALIZED.get(1));
        doAnswer(expectReadToEnd(serializedData)).when(configLog).readToEnd();

        Map<String, Struct> deserializedData = new HashMap<>();
        deserializedData.put(CONNECTOR_CONFIG_KEYS.get(0), null);
        deserializedData.put(TARGET_STATE_KEYS.get(0), null);
        expectRead(serializedData, deserializedData);

        configStorage.refresh(0, TimeUnit.SECONDS);
        verify(configUpdateListener).onConnectorConfigRemove(CONNECTOR_IDS.get(0));

        configState = configStorage.snapshot();
        // Connector should now be removed from the snapshot
        assertFalse(configState.contains(CONNECTOR_IDS.get(0)));
        assertEquals(0, configState.taskCount(CONNECTOR_IDS.get(0)));
        // Ensure that the deleted connector's deferred task updates have been cleaned up
        // in order to prevent unbounded growth of the map
        assertEquals(Collections.emptyMap(), configStorage.deferredTaskUpdates);

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testPutTaskConfigsDoesNotResolveAllInconsistencies() throws Exception {
        // Test a case where a failure and compaction has left us in an inconsistent state when reading the log.
        // We start out by loading an initial configuration where we started to write a task update, and then
        // compaction cleaned up the earlier record.
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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();
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

        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(2));
        // Successful attempt to write new task config
        doAnswer(expectReadToEnd(new LinkedHashMap<>()))
                .doAnswer(expectReadToEnd(new LinkedHashMap<>()))
                .doAnswer(expectReadToEnd(serializedConfigs))
                .when(configLog).readToEnd();
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectConvertWriteRead(
                COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(2),
                "tasks", 1); // Updated to just 1 task

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

        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        verify(configUpdateListener).onTaskConfigUpdate(Collections.singletonList(TASK_IDS.get(0)));

        configStorage.stop();
        verify(configLog).stop();
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
        expectStart(Collections.emptyList(), Collections.emptyMap());
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        configStorage.start();
        verify(configLog).start();

        expectConvertWriteRead(
                RESTART_CONNECTOR_KEYS.get(0), KafkaConfigBackingStore.RESTART_REQUEST_V0, CONFIGS_SERIALIZED.get(0),
                ONLY_FAILED_FIELD_NAME, restartRequest.onlyFailed());

        LinkedHashMap<String, byte[]> recordsToRead = new LinkedHashMap<>();
        recordsToRead.put(RESTART_CONNECTOR_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        doAnswer(expectReadToEnd(recordsToRead)).when(configLog).readToEnd();

        // Writing should block until it is written and read back from Kafka
        configStorage.putRestartRequest(restartRequest);

        final ArgumentCaptor<RestartRequest> restartRequestCaptor = ArgumentCaptor.forClass(RestartRequest.class);
        verify(configUpdateListener).onRestartRequest(restartRequestCaptor.capture());

        assertEquals(restartRequest.connectorName(), restartRequestCaptor.getValue().connectorName());
        assertEquals(restartRequest.onlyFailed(), restartRequestCaptor.getValue().onlyFailed());
        assertEquals(restartRequest.includeTasks(), restartRequestCaptor.getValue().includeTasks());

        configStorage.stop();
        verify(configLog).stop();
    }

    @Test
    public void testRestoreRestartRequestInconsistentState() {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state doesn't prevent startup.
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
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        configStorage.start();
        verify(configLog).start();

        // Shouldn't see any callbacks since this is during startup
        verify(configUpdateListener, never()).onConnectorConfigRemove(anyString());
        verify(configUpdateListener, never()).onConnectorConfigUpdate(anyString());
        verify(configUpdateListener, never()).onTaskConfigUpdate(anyCollection());
        verify(configUpdateListener, never()).onConnectorTargetStateChange(anyString());
        verify(configUpdateListener, never()).onSessionKeyUpdate(any(SessionKey.class));
        verify(configUpdateListener, never()).onRestartRequest(any(RestartRequest.class));
        verify(configUpdateListener, never()).onLoggingLevelUpdate(anyString(), anyString());

        configStorage.stop();
        verify(configLog).stop();
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

        expectStart(existingRecords, deserialized);
        when(configLog.partitionCount()).thenReturn(1);

        configStorage.setupAndCreateKafkaBasedLog(TOPIC, config);
        verifyConfigure();

        configStorage.start();
        verify(configLog).start();

        expectConvertWriteRead(
                "logger-cluster-" + logger3, KafkaConfigBackingStore.LOGGER_LEVEL_V0, CONFIGS_SERIALIZED.get(2),
                "level", level3);
        configStorage.putLoggerLevel(logger3, level3);

        expectConvertWriteRead(
                "logger-cluster-" + logger4, KafkaConfigBackingStore.LOGGER_LEVEL_V0, CONFIGS_SERIALIZED.get(3),
                "level", level4);
        configStorage.putLoggerLevel(logger4, level4);

        LinkedHashMap<String, byte[]> newRecords = new LinkedHashMap<>();
        newRecords.put("logger-cluster-" + logger3, CONFIGS_SERIALIZED.get(2));
        newRecords.put("logger-cluster-" + logger4, CONFIGS_SERIALIZED.get(3));
        doAnswer(expectReadToEnd(newRecords)).when(configLog).readToEnd();

        configStorage.refresh(0, TimeUnit.SECONDS);
        verify(configUpdateListener).onLoggingLevelUpdate(logger3, level3);
        verify(configUpdateListener).onLoggingLevelUpdate(logger4, level4);

        configStorage.stop();
        verify(configLog).stop();
    }

    private void verifyConfigure() {
        verify(configStorage).createKafkaBasedLog(capturedTopic.capture(), capturedProducerProps.capture(),
                capturedConsumerProps.capture(), capturedConsumedCallback.capture(),
                capturedNewTopic.capture(), capturedAdminSupplier.capture(),
                any(WorkerConfig.class), any(Time.class));
    }

    // If non-empty, deserializations should be a LinkedHashMap
    private void expectStart(final List<ConsumerRecord<String, byte[]>> preexistingRecords,
                             final Map<byte[], Struct> deserializations) {
        doAnswer(invocation -> {
            for (ConsumerRecord<String, byte[]> rec : preexistingRecords)
                capturedConsumedCallback.getValue().onCompletion(null, rec);
            return null;
        }).when(configLog).start();

        for (Map.Entry<byte[], Struct> deserializationEntry : deserializations.entrySet()) {
            // Note null schema because default settings for internal serialization are schema-less
            when(converter.toConnectData(TOPIC, deserializationEntry.getKey()))
                    .thenReturn(new SchemaAndValue(null, structToMap(deserializationEntry.getValue())));
        }
    }

    // Expect a conversion & write to the underlying log, followed by a subsequent read when the data is consumed back
    // from the log. Validate the data that is captured when the conversion is performed matches the specified data
    // (by checking a single field's value)
    private void expectConvertWriteRead(final String configKey, final Schema valueSchema, final byte[] serialized,
                                        final String dataFieldName, final Object dataFieldValue) throws Exception {
        final ArgumentCaptor<Struct> capturedRecord = ArgumentCaptor.forClass(Struct.class);
        when(converter.fromConnectData(eq(TOPIC), eq(valueSchema), capturedRecord.capture())).thenReturn(serialized);
        when(configLog.sendWithReceipt(configKey, serialized)).thenReturn(producerFuture);
        when(producerFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);
        when(converter.toConnectData(TOPIC, serialized)).thenAnswer(invocation -> {
            assertEquals(dataFieldValue, capturedRecord.getValue().get(dataFieldName));
            // Note null schema because default settings for internal serialization are schema-less
            return new SchemaAndValue(null, structToMap(capturedRecord.getValue()));
        });
    }

    private void expectRead(LinkedHashMap<String, byte[]> serializedValues,
                            Map<String, Struct> deserializedValues) {
        for (Map.Entry<String, Struct> deserializedValueEntry : deserializedValues.entrySet()) {
            byte[] serializedValue = serializedValues.get(deserializedValueEntry.getKey());
            when(converter.toConnectData(TOPIC, serializedValue))
                    .thenReturn(new SchemaAndValue(null, structToMap(deserializedValueEntry.getValue())));
        }
    }

    // This map needs to maintain ordering
    private Answer<Future<Void>> expectReadToEnd(final Map<String, byte[]> serializedConfigs) {
        return invocation -> {
            for (Map.Entry<String, byte[]> entry : serializedConfigs.entrySet()) {
                capturedConsumedCallback.getValue().onCompletion(null,
                        new ConsumerRecord<>(TOPIC, 0, logOffset++, 0L, TimestampType.CREATE_TIME, 0, 0,
                                entry.getKey(), entry.getValue(), new RecordHeaders(), Optional.empty()));
            }
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.complete(null);
            return f;
        };
    }

    // Generates a Map representation of Struct. Only does shallow traversal, so nested structs are not converted
    private Map<String, Object> structToMap(Struct struct) {
        if (struct == null)
            return null;
        Map<String, Object> result = new HashMap<>();
        for (Field field : struct.schema().fields()) result.put(field.name(), struct.get(field));
        return result;
    }
}
