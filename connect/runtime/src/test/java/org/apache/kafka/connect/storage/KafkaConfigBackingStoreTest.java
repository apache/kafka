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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.INCLUDE_TASKS_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.ONLY_FAILED_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.RESTART_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
    private static final Struct TARGET_STATE_STARTED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V0).put("state", "STARTED");
    private static final Struct TARGET_STATE_PAUSED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V1)
            .put("state", "PAUSED")
            .put("state.v2", "PAUSED");
    private static final Struct TARGET_STATE_STOPPED = new Struct(KafkaConfigBackingStore.TARGET_STATE_V1)
            .put("state", "PAUSED")
            .put("state.v2", "STOPPED");

    private static final Struct TASKS_COMMIT_STRUCT_TWO_TASK_CONNECTOR
            = new Struct(KafkaConfigBackingStore.CONNECTOR_TASKS_COMMIT_V0).put("tasks", 2);

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
        Supplier<TopicAdmin> topicAdminSupplier = () -> null;
        configStorage = PowerMock.createPartialMock(
                KafkaConfigBackingStore.class,
                new String[]{"createKafkaBasedLog", "createFencableProducer"},
                converter, config, null, topicAdminSupplier, CLIENT_ID_BASE, time);
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

    private void expectConfigure() throws Exception {
        PowerMock.expectPrivate(configStorage, "createKafkaBasedLog",
                EasyMock.capture(capturedTopic), EasyMock.capture(capturedProducerProps),
                EasyMock.capture(capturedConsumerProps), EasyMock.capture(capturedConsumedCallback),
                EasyMock.capture(capturedNewTopic), EasyMock.capture(capturedAdminSupplier),
                EasyMock.anyObject(WorkerConfig.class), EasyMock.anyObject(Time.class))
                .andReturn(storeLog);
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
