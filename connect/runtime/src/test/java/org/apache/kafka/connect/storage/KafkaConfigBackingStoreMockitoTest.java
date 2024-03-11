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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.INCLUDE_TASKS_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.ONLY_FAILED_FIELD_NAME;
import static org.apache.kafka.connect.storage.KafkaConfigBackingStore.RESTART_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

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

    private static final String CONNECTOR_1_NAME = "connector1";
    private static final String CONNECTOR_2_NAME = "connector2";
    private static final List<String> RESTART_CONNECTOR_KEYS = Arrays.asList(RESTART_KEY(CONNECTOR_1_NAME), RESTART_KEY(CONNECTOR_2_NAME));

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
    private final Map<String, String> props = new HashMap<>(DEFAULT_CONFIG_STORAGE_PROPS);
    private DistributedConfig config;
    @Mock
    KafkaBasedLog<String, byte[]> configLog;
    private KafkaConfigBackingStore configStorage;
    private final MockTime time = new MockTime();

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
