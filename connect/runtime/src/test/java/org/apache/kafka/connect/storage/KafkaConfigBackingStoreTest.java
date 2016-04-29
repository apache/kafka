/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TestFuture;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaConfigBackingStore.class)
@PowerMockIgnore("javax.management.*")
@SuppressWarnings("unchecked")
public class KafkaConfigBackingStoreTest {
    private static final String TOPIC = "connect-configs";
    private static final Map<String, String> DEFAULT_CONFIG_STORAGE_PROPS = new HashMap<>();
    private static final DistributedConfig DEFAULT_DISTRIBUTED_CONFIG;

    static {
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.CONFIG_TOPIC_CONFIG, TOPIC);
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.GROUP_ID_CONFIG, "connect");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        DEFAULT_CONFIG_STORAGE_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_CONFIG_STORAGE_PROPS.put(DistributedConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_DISTRIBUTED_CONFIG = new DistributedConfig(DEFAULT_CONFIG_STORAGE_PROPS);
    }

    private static final List<String> CONNECTOR_IDS = Arrays.asList("connector1", "connector2");
    private static final List<String> CONNECTOR_CONFIG_KEYS = Arrays.asList("connector-connector1", "connector-connector2");
    private static final List<String> COMMIT_TASKS_CONFIG_KEYS = Arrays.asList("commit-connector1", "commit-connector2");
    private static final List<String> TARGET_STATE_KEYS =  Arrays.asList("target-state-connector1", "target-state-connector2");

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

    private static final Struct TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 2);

    private static final Struct TASKS_COMMIT_STRUCT_ZERO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 0);

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
    @Mock
    KafkaBasedLog<String, byte[]> storeLog;
    private KafkaConfigBackingStore configStorage;

    private Capture<String> capturedTopic = EasyMock.newCapture();
    private Capture<Map<String, Object>> capturedProducerProps = EasyMock.newCapture();
    private Capture<Map<String, Object>> capturedConsumerProps = EasyMock.newCapture();
    private Capture<Callback<ConsumerRecord<String, byte[]>>> capturedConsumedCallback = EasyMock.newCapture();

    private long logOffset = 0;

    @Before
    public void setUp() {
        configStorage = PowerMock.createPartialMock(KafkaConfigBackingStore.class, new String[]{"createKafkaBasedLog"}, converter);
        configStorage.setUpdateListener(configUpdateListener);
    }

    @Test
    public void testStartStop() throws Exception {
        expectConfigure();
        expectStart(Collections.EMPTY_LIST, Collections.EMPTY_MAP);
        expectStop();

        PowerMock.replayAll();

        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
        assertEquals(TOPIC, capturedTopic.getValue());
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", capturedProducerProps.getValue().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", capturedProducerProps.getValue().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        configStorage.start();
        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Exception {
        expectConfigure();
        expectStart(Collections.EMPTY_LIST, Collections.EMPTY_MAP);

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
        expectConvertWriteAndRead(
                CONNECTOR_CONFIG_KEYS.get(1), KafkaConfigBackingStore.CONNECTOR_CONFIGURATION_V0, null, null, null);
        configUpdateListener.onConnectorConfigRemove(CONNECTOR_IDS.get(1));
        EasyMock.expectLastCall();

        // Target state deletion
        storeLog.send(TARGET_STATE_KEYS.get(1), null);
        PowerMock.expectLastCall();

        expectStop();

        PowerMock.replayAll();

        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
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
        assertEquals(3, configState.offset());
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertNull(configState.connectorConfig(CONNECTOR_IDS.get(1)));

        configStorage.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testPutTaskConfigs() throws Exception {
        expectConfigure();
        expectStart(Collections.EMPTY_LIST, Collections.EMPTY_MAP);

        // Task configs should read to end, write to the log, read to end, write root, then read to end again
        expectReadToEnd(new LinkedHashMap<String, byte[]>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(1), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(1),
                "properties", SAMPLE_CONFIGS.get(1));
        expectReadToEnd(new LinkedHashMap<String, byte[]>());
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

        expectStop();

        PowerMock.replayAll();


        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
        configStorage.start();

        // Bootstrap as if we had already added the connector, but no tasks had been added yet
        whiteboxAddConnector(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), Collections.EMPTY_LIST);

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());
        assertNull(configState.taskConfig(TASK_IDS.get(0)));
        assertNull(configState.taskConfig(TASK_IDS.get(1)));

        // Writing task task configs should block until all the writes have been performed and the root record update
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
    public void testPutTaskConfigsZeroTasks() throws Exception {
        expectConfigure();
        expectStart(Collections.EMPTY_LIST, Collections.EMPTY_MAP);

        // Task configs should read to end, write to the log, read to end, write root.
        expectReadToEnd(new LinkedHashMap<String, byte[]>());
        expectConvertWriteRead(
            COMMIT_TASKS_CONFIG_KEYS.get(0), KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0, CONFIGS_SERIALIZED.get(0),
            "tasks", 0); // We have 0 tasks
        // As soon as root is rewritten, we should see a callback notifying us that we reconfigured some tasks
        configUpdateListener.onTaskConfigUpdate(Collections.<ConnectorTaskId>emptyList());
        EasyMock.expectLastCall();

        // Records to be read by consumer as it reads to the end of the log
        LinkedHashMap<String, byte[]> serializedConfigs = new LinkedHashMap<>();
        serializedConfigs.put(COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0));
        expectReadToEnd(serializedConfigs);

        expectStop();

        PowerMock.replayAll();

        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
        configStorage.start();

        // Bootstrap as if we had already added the connector, but no tasks had been added yet
        whiteboxAddConnector(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), Collections.EMPTY_LIST);

        // Null before writing
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(-1, configState.offset());

        // Writing task task configs should block until all the writes have been performed and the root record update
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
    public void testRestore() throws Exception {
        // Restoring data should notify only of the latest values after loading is complete. This also validates
        // that inconsistent state is ignored.

        expectConfigure();
        // Overwrite each type at least once to ensure we see the latest data after loading
        List<ConsumerRecord<String, byte[]>> existingRecords = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0)),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(1)),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(2)),
                new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(3)),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(4)),
                // Connector after root update should make it through, task update shouldn't
                new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(5)),
                new ConsumerRecord<>(TOPIC, 0, 6, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(6)));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(1), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(3), CONNECTOR_CONFIG_STRUCTS.get(1));
        deserialized.put(CONFIGS_SERIALIZED.get(4), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        deserialized.put(CONFIGS_SERIALIZED.get(5), CONNECTOR_CONFIG_STRUCTS.get(2));
        deserialized.put(CONFIGS_SERIALIZED.get(6), TASK_CONFIG_STRUCTS.get(1));
        logOffset = 7;
        expectStart(existingRecords, deserialized);

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
        configStorage.start();

        // Should see a single connector and its config should be the last one seen anywhere in the log
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(7, configState.offset()); // Should always be next to be read, even if uncommitted
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        // CONNECTOR_CONFIG_STRUCTS[2] -> SAMPLE_CONFIGS[2]
        assertEquals(SAMPLE_CONFIGS.get(2), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        // Should see 2 tasks for that connector. Only config updates before the root key update should be reflected
        assertEquals(Arrays.asList(TASK_IDS.get(0), TASK_IDS.get(1)), configState.tasks(CONNECTOR_IDS.get(0)));
        // Both TASK_CONFIG_STRUCTS[0] -> SAMPLE_CONFIGS[0]
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.taskConfig(TASK_IDS.get(1)));
        assertEquals(Collections.EMPTY_SET, configState.inconsistentConnectors());

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
            new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0)),
            new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(1)),
            new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(2)),
            new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(3)),
            new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(4)),
            // Connector after root update should make it through, task update shouldn't
            new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(5)),
            new ConsumerRecord<>(TOPIC, 0, 6, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(6)),
            new ConsumerRecord<>(TOPIC, 0, 7, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(7)));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap();
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

        // Shouldn't see any callbacks since this is during startup

        expectStop();

        PowerMock.replayAll();

        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
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
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, CONNECTOR_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(0)),
                // This is the record that has been compacted:
                //new ConsumerRecord<>(TOPIC, 0, 1, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(1)),
                new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(1), CONFIGS_SERIALIZED.get(2)),
                new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, COMMIT_TASKS_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(4)),
                new ConsumerRecord<>(TOPIC, 0, 5, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TASK_CONFIG_KEYS.get(0), CONFIGS_SERIALIZED.get(5)));
        LinkedHashMap<byte[], Struct> deserialized = new LinkedHashMap();
        deserialized.put(CONFIGS_SERIALIZED.get(0), CONNECTOR_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(2), TASK_CONFIG_STRUCTS.get(0));
        deserialized.put(CONFIGS_SERIALIZED.get(4), TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR);
        deserialized.put(CONFIGS_SERIALIZED.get(5), TASK_CONFIG_STRUCTS.get(1));
        logOffset = 6;
        expectStart(existingRecords, deserialized);

        // Successful attempt to write new task config
        expectReadToEnd(new LinkedHashMap<String, byte[]>());
        expectConvertWriteRead(
                TASK_CONFIG_KEYS.get(0), KafkaConfigBackingStore.TASK_CONFIGURATION_V0, CONFIGS_SERIALIZED.get(0),
                "properties", SAMPLE_CONFIGS.get(0));
        expectReadToEnd(new LinkedHashMap<String, byte[]>());
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

        configStorage.configure(DEFAULT_DISTRIBUTED_CONFIG);
        configStorage.start();
        // After reading the log, it should have been in an inconsistent state
        ClusterConfigState configState = configStorage.snapshot();
        assertEquals(6, configState.offset()); // Should always be next to be read, not last committed
        assertEquals(Arrays.asList(CONNECTOR_IDS.get(0)), new ArrayList<>(configState.connectors()));
        // Inconsistent data should leave us with no tasks listed for the connector and an entry in the inconsistent list
        assertEquals(Collections.EMPTY_LIST, configState.tasks(CONNECTOR_IDS.get(0)));
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

    private void expectConfigure() throws Exception {
        PowerMock.expectPrivate(configStorage, "createKafkaBasedLog",
                EasyMock.capture(capturedTopic), EasyMock.capture(capturedProducerProps),
                EasyMock.capture(capturedConsumerProps), EasyMock.capture(capturedConsumedCallback))
                .andReturn(storeLog);
    }

    // If non-empty, deserializations should be a LinkedHashMap
    private void expectStart(final List<ConsumerRecord<String, byte[]>> preexistingRecords,
                             final Map<byte[], Struct> deserializations) throws Exception {
        storeLog.start();
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                for (ConsumerRecord<String, byte[]> rec : preexistingRecords)
                    capturedConsumedCallback.getValue().onCompletion(null, rec);
                return null;
            }
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

    // Expect a conversion & write to the underlying log, followed by a subsequent read when the data is consumed back
    // from the log. Validate the data that is captured when the conversion is performed matches the specified data
    // (by checking a single field's value)
    private void expectConvertWriteRead(final String configKey, final Schema valueSchema, final byte[] serialized,
                                        final String dataFieldName, final Object dataFieldValue) {
        final Capture<Struct> capturedRecord = EasyMock.newCapture();
        if (serialized != null)
            EasyMock.expect(converter.fromConnectData(EasyMock.eq(TOPIC), EasyMock.eq(valueSchema), EasyMock.capture(capturedRecord)))
                    .andReturn(serialized);
        storeLog.send(EasyMock.eq(configKey), EasyMock.aryEq(serialized));
        PowerMock.expectLastCall();
        EasyMock.expect(converter.toConnectData(EasyMock.eq(TOPIC), EasyMock.aryEq(serialized)))
                .andAnswer(new IAnswer<SchemaAndValue>() {
                    @Override
                    public SchemaAndValue answer() throws Throwable {
                        if (dataFieldName != null)
                            assertEquals(dataFieldValue, capturedRecord.getValue().get(dataFieldName));
                        // Note null schema because default settings for internal serialization are schema-less
                        return new SchemaAndValue(null, serialized == null ? null : structToMap(capturedRecord.getValue()));
                    }
                });
    }

    // This map needs to maintain ordering
    private void expectReadToEnd(final LinkedHashMap<String, byte[]> serializedConfigs) {
        EasyMock.expect(storeLog.readToEnd())
                .andAnswer(new IAnswer<Future<Void>>() {
                    @Override
                    public Future<Void> answer() throws Throwable {
                        TestFuture<Void> future = new TestFuture<Void>();
                        for (Map.Entry<String, byte[]> entry : serializedConfigs.entrySet())
                            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 0, logOffset++, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, entry.getKey(), entry.getValue()));
                        future.resolveOnGet((Void) null);
                        return future;
                    }
                });
    }


    private void expectConvertWriteAndRead(final String configKey, final Schema valueSchema, final byte[] serialized,
                                           final String dataFieldName, final Object dataFieldValue) {
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
        HashMap<String, Object> result = new HashMap<>();
        for (Field field : struct.schema().fields())
            result.put(field.name(), struct.get(field));
        return result;
    }

}
