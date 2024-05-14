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
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class FileOffsetBackingStoreTest {

    private FileOffsetBackingStore store;
    private StandaloneConfig config;
    private File tempFile;
    private Converter converter;


    private static Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();
    private static final Runnable EMPTY_RUNNABLE = () -> {
    };

    static {
        firstSet.put(buffer("key"), buffer("value"));
        firstSet.put(null, null);
    }

    @Before
    public void setup() throws IOException {
        converter = mock(Converter.class);
        // This is only needed for storing deserialized connector partitions, which we don't test in most of the cases here
        when(converter.toConnectData(anyString(), any(byte[].class))).thenReturn(new SchemaAndValue(null,
                Arrays.asList("connector", Collections.singletonMap("partitionKey", "dummy"))));
        store = new FileOffsetBackingStore(converter);
        tempFile = File.createTempFile("fileoffsetbackingstore", null);
        Map<String, String> props = new HashMap<>();
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, tempFile.getAbsolutePath());
        props.put(StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        config = new StandaloneConfig(props);
        store.configure(config);
        store.start();
    }

    @After
    public void teardown() throws IOException {
        Files.deleteIfExists(tempFile.toPath());
    }

    @Test
    public void testGetSet() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = mock(Callback.class);

        store.set(firstSet, setCallback).get();

        Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(buffer("key"), buffer("bad"))).get();
        assertEquals(buffer("value"), values.get(buffer("key")));
        assertNull(values.get(buffer("bad")));
        verify(setCallback).onCompletion(isNull(), isNull());
    }

    @Test
    public void testSaveRestore() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = mock(Callback.class);

        store.set(firstSet, setCallback).get();
        store.stop();

        // Restore into a new store to ensure correct reload from scratch
        FileOffsetBackingStore restore = new FileOffsetBackingStore(converter);
        restore.configure(config);
        restore.start();
        Map<ByteBuffer, ByteBuffer> values = restore.get(Collections.singletonList(buffer("key"))).get();
        assertEquals(buffer("value"), values.get(buffer("key")));
        verify(setCallback).onCompletion(isNull(), isNull());
    }

    @Test
    public void testThreadName() {
        assertTrue(((ThreadPoolExecutor) store.executor).getThreadFactory()
                .newThread(EMPTY_RUNNABLE).getName().startsWith(FileOffsetBackingStore.class.getSimpleName()));
    }

    @Test
    public void testConnectorPartitions() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = mock(Callback.class);

        // This test actually requires the offset store to track deserialized source partitions, so we can't use the member variable mock converter
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);

        Map<ByteBuffer, ByteBuffer> serializedPartitionOffsets = new HashMap<>();
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Collections.singletonMap("partitionKey", "partitionValue1")),
                serialize(jsonConverter, Collections.singletonMap("offsetKey", "offsetValue"))
        );
        store.set(serializedPartitionOffsets, setCallback).get();

        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Collections.singletonMap("partitionKey", "partitionValue1")),
                serialize(jsonConverter, Collections.singletonMap("offsetKey", "offsetValue2"))
        );
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Collections.singletonMap("partitionKey", "partitionValue2")),
                serialize(jsonConverter, Collections.singletonMap("offsetKey", "offsetValue"))
        );
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector2", Collections.singletonMap("partitionKey", "partitionValue")),
                serialize(jsonConverter, Collections.singletonMap("offsetKey", "offsetValue"))
        );

        store.set(serializedPartitionOffsets, setCallback).get();
        store.stop();

        // Restore into a new store to ensure correct reload from scratch
        FileOffsetBackingStore restore = new FileOffsetBackingStore(jsonConverter);
        restore.configure(config);
        restore.start();

        Set<Map<String, Object>> connectorPartitions1 = restore.connectorPartitions("connector1");
        Set<Map<String, Object>> expectedConnectorPartition1 = new HashSet<>();
        expectedConnectorPartition1.add(Collections.singletonMap("partitionKey", "partitionValue1"));
        expectedConnectorPartition1.add(Collections.singletonMap("partitionKey", "partitionValue2"));
        assertEquals(expectedConnectorPartition1, connectorPartitions1);

        Set<Map<String, Object>> connectorPartitions2 = restore.connectorPartitions("connector2");
        Set<Map<String, Object>> expectedConnectorPartition2 = Collections.singleton(Collections.singletonMap("partitionKey", "partitionValue"));
        assertEquals(expectedConnectorPartition2, connectorPartitions2);

        serializedPartitionOffsets.clear();
        // Null valued offset for a partition key should remove that partition for the connector
        serializedPartitionOffsets.put(
                serializeKey(jsonConverter, "connector1", Collections.singletonMap("partitionKey", "partitionValue1")),
                null
        );
        restore.set(serializedPartitionOffsets, setCallback).get();
        connectorPartitions1 = restore.connectorPartitions("connector1");
        assertEquals(Collections.singleton(Collections.singletonMap("partitionKey", "partitionValue2")), connectorPartitions1);

        verify(setCallback, times(3)).onCompletion(isNull(), isNull());
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

    private static ByteBuffer serializeKey(Converter converter, String connectorName, Map<String, Object> sourcePartition) {
        List<Object> nameAndPartition = Arrays.asList(connectorName, sourcePartition);
        return serialize(converter, nameAndPartition);
    }

    private static ByteBuffer serialize(Converter converter, Object value) {
        byte[] serialized = converter.fromConnectData("", null, value);
        return ByteBuffer.wrap(serialized);
    }

}
