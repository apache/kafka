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
package org.apache.kafka.streams.state.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LogicalKeyValueSegmentTest {

    private static final String STORE_NAME = "physical-rocks";
    private static final String METRICS_SCOPE = "metrics-scope";
    private static final String DB_FILE_DIR = "rocksdb";
    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

    private RocksDBStore physicalStore;

    private LogicalKeyValueSegment segment1;
    private LogicalKeyValueSegment segment2;
    private LogicalKeyValueSegment segment3;

    @Before
    public void setUp() {
        physicalStore = new RocksDBStore(STORE_NAME, DB_FILE_DIR, new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME), false);
        physicalStore.init((StateStoreContext) new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        ), physicalStore);

        segment1 = new LogicalKeyValueSegment(1, "segment-1", physicalStore);
        segment2 = new LogicalKeyValueSegment(2, "segment-2", physicalStore);
        segment3 = new LogicalKeyValueSegment(3, "segment-3", physicalStore);
    }

    @After
    public void tearDown() {
        segment1.close();
        segment2.close();
        segment3.close();
        physicalStore.close();
    }

    @Test
    public void shouldPut() {
        final KeyValue<String, String> kv0 = new KeyValue<>("1", "a");
        final KeyValue<String, String> kv1 = new KeyValue<>("2", "b");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment2.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment2.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));

        assertEquals("a", getAndDeserialize(segment1, "1"));
        assertEquals("b", getAndDeserialize(segment1, "2"));
        assertEquals("a", getAndDeserialize(segment2, "1"));
        assertEquals("b", getAndDeserialize(segment2, "2"));
    }

    @Test
    public void shouldPutAll() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(serializeBytes("1")),
            serializeBytes("a")));
        entries.add(new KeyValue<>(
            new Bytes(serializeBytes("2")),
            serializeBytes("b")));
        entries.add(new KeyValue<>(
            new Bytes(serializeBytes("3")),
            serializeBytes("c")));

        segment1.putAll(entries);
        segment2.putAll(entries);

        assertEquals("a", getAndDeserialize(segment1, "1"));
        assertEquals("b", getAndDeserialize(segment1, "2"));
        assertEquals("c", getAndDeserialize(segment1, "3"));
        assertEquals("a", getAndDeserialize(segment2, "1"));
        assertEquals("b", getAndDeserialize(segment2, "2"));
        assertEquals("c", getAndDeserialize(segment2, "3"));
    }

    @Test
    public void shouldPutIfAbsent() {
        final Bytes keyBytes = new Bytes(serializeBytes("one"));
        final byte[] valueBytes = serializeBytes("A");
        final byte[] valueBytesUpdate = serializeBytes("B");

        segment1.putIfAbsent(keyBytes, valueBytes);
        segment1.putIfAbsent(keyBytes, valueBytesUpdate);
        segment2.putIfAbsent(keyBytes, valueBytesUpdate);

        assertEquals("A", STRING_DESERIALIZER.deserialize(null, segment1.get(keyBytes)));
        assertEquals("B", STRING_DESERIALIZER.deserialize(null, segment2.get(keyBytes)));
    }

    @Test
    public void shouldDelete() {
        final KeyValue<String, String> kv0 = new KeyValue<>("1", "a");
        final KeyValue<String, String> kv1 = new KeyValue<>("2", "b");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment2.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment2.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.delete(new Bytes(serializeBytes(kv0.key)));

        assertNull(segment1.get(new Bytes(serializeBytes(kv0.key))));
        assertEquals("b", getAndDeserialize(segment1, "2"));
        assertEquals("a", getAndDeserialize(segment2, "1"));
        assertEquals("b", getAndDeserialize(segment2, "2"));
    }

    @Test
    public void shouldReturnValuesOnRange() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        segment2.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment2.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment2.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment1.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
        segment3.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));

        // non-null bounds
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment2.range(new Bytes(serializeBytes("1")), new Bytes(serializeBytes("2")))) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null lower bound
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment2.range(null, new Bytes(serializeBytes("1")))) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv0);
            expectedContents.add(kv1);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null upper bound
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment2.range(new Bytes(serializeBytes("0")), null)) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv0);
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null upper and lower bounds
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment2.range(new Bytes(serializeBytes("0")), null)) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv0);
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }
    }

    @Test
    public void shouldReturnAll() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment2.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(kv0);
        expectedContents.add(kv1);
        expectedContents.add(kv2);

        try (final KeyValueIterator<Bytes, byte[]> iterator = segment1.all()) {
            assertEquals(expectedContents, getDeserializedList(iterator));
        }
    }

    @Test
    public void shouldDeleteRange() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment2.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
        segment1.deleteRange(new Bytes(serializeBytes(kv0.key)), new Bytes(serializeBytes(kv1.key)));

        assertNull(segment1.get(new Bytes(serializeBytes(kv0.key))));
        assertNull(segment1.get(new Bytes(serializeBytes(kv1.key))));
        assertEquals("two", getAndDeserialize(segment1, "2"));
        assertEquals("other", getAndDeserialize(segment2, "1"));
    }

    @Test
    public void shouldDestroy() {
        final KeyValue<String, String> kv0 = new KeyValue<>("1", "a");
        final KeyValue<String, String> kv1 = new KeyValue<>("2", "b");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment2.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment2.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));

        segment1.destroy();

        assertEquals("a", getAndDeserialize(segment2, "1"));
        assertEquals("b", getAndDeserialize(segment2, "2"));

        segment1 = new LogicalKeyValueSegment(1, "segment-1", physicalStore);

        assertNull(segment1.get(new Bytes(serializeBytes(kv0.key))));
        assertNull(segment1.get(new Bytes(serializeBytes(kv1.key))));
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreClosed() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment2.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment2.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));

        final KeyValueIterator<Bytes, byte[]> range1 = segment1.range(null, new Bytes(serializeBytes("1")));
        final KeyValueIterator<Bytes, byte[]> all1 = segment1.all();
        final KeyValueIterator<Bytes, byte[]> range2 = segment2.range(null, new Bytes(serializeBytes("1")));

        assertTrue(range1.hasNext());
        assertTrue(all1.hasNext());
        assertTrue(range2.hasNext());

        segment1.close();

        assertThrows(InvalidStateStoreException.class, range1::hasNext);
        assertThrows(InvalidStateStoreException.class, all1::hasNext);
        assertTrue(range2.hasNext());
    }

    private static byte[] serializeBytes(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static String getAndDeserialize(final LogicalKeyValueSegment segment, final String key) {
        return STRING_DESERIALIZER.deserialize(null, segment.get(new Bytes(serializeBytes(key))));
    }

    private static List<KeyValue<String, String>> getDeserializedList(final KeyValueIterator<Bytes, byte[]> iter) {
        final List<KeyValue<Bytes, byte[]>> bytes = Utils.toList(iter);
        return bytes.stream().map(kv -> new KeyValue<>(kv.key.toString(), STRING_DESERIALIZER.deserialize(null, kv.value))).collect(Collectors.toList());
    }
}