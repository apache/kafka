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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TestFuture;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.RESTART_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
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
    private static final List<String> TARGET_STATE_KEYS = Arrays.asList("target-state-connector1", "target-state-connector2");


    private static final String CONNECTOR_1_NAME = "connector1";
    private static final String CONNECTOR_2_NAME = "connector2";
    private static final List<String> RESTART_CONNECTOR_KEYS = Arrays.asList(RESTART_KEY(CONNECTOR_1_NAME), RESTART_KEY(CONNECTOR_2_NAME));

    private static final Struct ONLY_FAILED_MISSING_STRUCT = new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(INCLUDE_TASKS_FIELD_NAME, false);
    private static final Struct INCLUDE_TASKS_MISSING_STRUCT = new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(ONLY_FAILED_FIELD_NAME, true);
    private static final List<Struct> RESTART_REQUEST_STRUCTS = Arrays.asList(
            new Struct(KafkaConfigBackingStore.RESTART_REQUEST_V0).put(ONLY_FAILED_FIELD_NAME, true).put(INCLUDE_TASKS_FIELD_NAME, false),
            ONLY_FAILED_MISSING_STRUCT,
            INCLUDE_TASKS_MISSING_STRUCT);


    // Need some placeholders -- the contents don't matter here, just that they are restored properly
    private static final List<Map<String, String>> SAMPLE_CONFIGS = Arrays.asList(
            Collections.singletonMap("config-key-one", "config-value-one"),
            Collections.singletonMap("config-key-two", "config-value-two"),
            Collections.singletonMap("config-key-three", "config-value-three")
    );

    // The exact format doesn't matter here since both conversions are mocked
    private static final List<byte[]> CONFIGS_SERIALIZED = Arrays.asList(
            "config-bytes-1".getBytes(), "config-bytes-2".getBytes(), "config-bytes-3".getBytes(),
            "config-bytes-4".getBytes(), "config-bytes-5".getBytes(), "config-bytes-6".getBytes(),
            "config-bytes-7".getBytes(), "config-bytes-8".getBytes(), "config-bytes-9".getBytes()
    );

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
                        converter, config, null, null, CLIENT_ID_BASE, time)
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

        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);

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
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);

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
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);

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
        expectConvertWriteRead(configKey, KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, null, null, null);
        expectConvertWriteRead(targetStateKey, KafkaConfigBackingStore.TARGET_STATE_V0, null, null, null);

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
        expectStart(Collections.emptyList(), Collections.emptyMap());
        expectPartitionCount(1);

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

    private void expectPartitionCount(int partitionCount) {
        when(configLog.partitionCount()).thenReturn(partitionCount);
    }

    // Expect a conversion & write to the underlying log, followed by a subsequent read when the data is consumed back
    // from the log. Validate the data that is captured when the conversion is performed matches the specified data
    // (by checking a single field's value)
    private void expectConvertWriteRead(final String configKey, final Schema valueSchema, final byte[] serialized,
                                        final String dataFieldName, final Object dataFieldValue) throws Exception {
        final ArgumentCaptor<Struct> capturedRecord = ArgumentCaptor.forClass(Struct.class);
        if (serialized != null)
            when(converter.fromConnectData(eq(TOPIC), eq(valueSchema), capturedRecord.capture()))
                    .thenReturn(serialized);

        when(configLog.sendWithReceipt(configKey, serialized)).thenReturn(producerFuture);
        when(producerFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);
        when(converter.toConnectData(TOPIC, serialized)).thenAnswer((Answer<SchemaAndValue>) invocation -> {
            if (dataFieldName != null)
                assertEquals(dataFieldValue, capturedRecord.getValue().get(dataFieldName));
            // Note null schema because default settings for internal serialization are schema-less
            return new SchemaAndValue(null, serialized == null ? null : structToMap(capturedRecord.getValue()));
        });
    }

    // This map needs to maintain ordering
    private Answer<Future<Void>> expectReadToEnd(final Map<String, byte[]> serializedConfigs) {
        return invocation -> {
            TestFuture<Void> future = new TestFuture<>();
            for (Map.Entry<String, byte[]> entry : serializedConfigs.entrySet()) {
                capturedConsumedCallback.getValue().onCompletion(null,
                        new ConsumerRecord<>(TOPIC, 0, logOffset++, 0L, TimestampType.CREATE_TIME, 0, 0,
                                entry.getKey(), entry.getValue(), new RecordHeaders(), Optional.empty()));
            }
            future.resolveOnGet((Void) null);
            return future;
        };
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
