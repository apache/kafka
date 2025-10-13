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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FileOffsetBackingStoreTest {

    private FileOffsetBackingStore store;
    private StandaloneConfig config;
    private File tempFile;
    private Converter converter;


    private static final Map<ByteBuffer, ByteBuffer> FIRST_SET = new HashMap<>();
    private static final Runnable EMPTY_RUNNABLE = () -> {
    };

    static {
        FIRST_SET.put(buffer("key"), buffer("value"));
        FIRST_SET.put(null, null);
    }

    @BeforeEach
    public void setup() {
        converter = mock(Converter.class);
        // This is only needed for storing deserialized connector partitions, which we don't test in most of the cases here
        when(converter.toConnectData(anyString(), any(byte[].class))).thenReturn(new SchemaAndValue(null,
                List.of("connector", Map.of("partitionKey", "dummy"))));
        store = new FileOffsetBackingStore(converter);
        tempFile = assertDoesNotThrow(() -> File.createTempFile("fileoffsetbackingstore", null));
        Map<String, String> props = new HashMap<>();
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, tempFile.getAbsolutePath());
        props.put(StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config = new StandaloneConfig(props);
        store.configure(config);
        store.start();
        assertTrue(((ThreadPoolExecutor) store.executor).getThreadFactory()
                .newThread(EMPTY_RUNNABLE).getName().startsWith(FileOffsetBackingStore.class.getSimpleName()));
    }

    @AfterEach
    public void teardown() throws IOException {
        Files.deleteIfExists(tempFile.toPath());
    }

    @Test
    public void testGetSet() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = mock(Callback.class);

        store.set(FIRST_SET, setCallback).get();

        Map<ByteBuffer, ByteBuffer> values = store.get(List.of(buffer("key"), buffer("bad"))).get();
        assertEquals(buffer("value"), values.get(buffer("key")));
        assertNull(values.get(buffer("bad")));
        verify(setCallback).onCompletion(isNull(), isNull());
    }

    @Test
    public void testSaveRestore() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = mock(Callback.class);

        store.set(FIRST_SET, setCallback).get();
        store.stop();

        // Restore into a new store to ensure correct reload from scratch
        FileOffsetBackingStore restore = new FileOffsetBackingStore(converter);
        restore.configure(config);
        restore.start();
        Map<ByteBuffer, ByteBuffer> values = restore.get(List.of(buffer("key"))).get();
        assertEquals(buffer("value"), values.get(buffer("key")));
        verify(setCallback).onCompletion(isNull(), isNull());
    }

    @Test
    public void testConnectorPartitions() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = mock(Callback.class);

        // This test actually requires the offset store to track deserialized source partitions, so we can't use the member variable mock converter
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Map.of(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);

        Map<ByteBuffer, ByteBuffer> serializedPartitionOffsets = new HashMap<>();
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Map.of("partitionKey", "partitionValue1")),
                serialize(jsonConverter, Map.of("offsetKey", "offsetValue"))
        );
        store.set(serializedPartitionOffsets, setCallback).get();

        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Map.of("partitionKey", "partitionValue1")),
                serialize(jsonConverter, Map.of("offsetKey", "offsetValue2"))
        );
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Map.of("partitionKey", "partitionValue2")),
                serialize(jsonConverter, Map.of("offsetKey", "offsetValue"))
        );
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector2", Map.of("partitionKey", "partitionValue")),
                serialize(jsonConverter, Map.of("offsetKey", "offsetValue"))
        );

        store.set(serializedPartitionOffsets, setCallback).get();
        store.stop();

        // Restore into a new store to ensure correct reload from scratch
        FileOffsetBackingStore restore = new FileOffsetBackingStore(jsonConverter);
        restore.configure(config);
        restore.start();

        Set<Map<String, Object>> connectorPartitions1 = restore.connectorPartitions("connector1");
        Set<Map<String, Object>> expectedConnectorPartition1 = new HashSet<>();
        expectedConnectorPartition1.add(Map.of("partitionKey", "partitionValue1"));
        expectedConnectorPartition1.add(Map.of("partitionKey", "partitionValue2"));
        assertEquals(expectedConnectorPartition1, connectorPartitions1);

        Set<Map<String, Object>> connectorPartitions2 = restore.connectorPartitions("connector2");
        Set<Map<String, Object>> expectedConnectorPartition2 = Set.of(Map.of("partitionKey", "partitionValue"));
        assertEquals(expectedConnectorPartition2, connectorPartitions2);

        serializedPartitionOffsets.clear();
        // Null valued offset for a partition key should remove that partition for the connector
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Map.of("partitionKey", "partitionValue1")),
                null
        );
        restore.set(serializedPartitionOffsets, setCallback).get();
        connectorPartitions1 = restore.connectorPartitions("connector1");
        assertEquals(Set.of(Map.of("partitionKey", "partitionValue2")), connectorPartitions1);

        verify(setCallback, times(3)).onCompletion(isNull(), isNull());
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

    private static ByteBuffer serializeKey(Converter converter, String connectorName, Map<String, Object> sourcePartition) {
        List<Object> nameAndPartition = List.of(connectorName, sourcePartition);
        return serialize(converter, nameAndPartition);
    }

    private static ByteBuffer serialize(Converter converter, Object value) {
        byte[] serialized = converter.fromConnectData("", null, value);
        return ByteBuffer.wrap(serialized);
    }

}
