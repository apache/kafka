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

    private LogicalKeyValueSegment segment0;
    private LogicalKeyValueSegment segment1;
    private LogicalKeyValueSegment segment2;
    private LogicalKeyValueSegment negativeIdSegment;

    @Before
    public void setUp() {
        physicalStore = new RocksDBStore(STORE_NAME, DB_FILE_DIR, new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME), false);
        physicalStore.init((StateStoreContext) new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        ), physicalStore);

        segment0 = new LogicalKeyValueSegment(0, "segment-0", physicalStore);
        segment1 = new LogicalKeyValueSegment(1, "segment-1", physicalStore);
        segment2 = new LogicalKeyValueSegment(2, "segment-2", physicalStore);

        // segments with negative IDs are supported as well for use in "reserved segments" (see LogicalKeyValueSegments.java)
        negativeIdSegment = new LogicalKeyValueSegment(-1, "reserved-segment", physicalStore);
    }

    @After
    public void tearDown() {
        segment0.close();
        segment1.close();
        segment2.close();
        physicalStore.close();
    }

    @Test
    public void shouldPut() {
        final KeyValue<String, String> sharedKeyV1 = new KeyValue<>("shared", "v1");
        final KeyValue<String, String> sharedKeyV2 = new KeyValue<>("shared", "v2");
        final KeyValue<String, String> sharedKeyV3 = new KeyValue<>("shared", "v3");
        final KeyValue<String, String> segment0KeyOnly = new KeyValue<>("segment0_only", "foo");
        final KeyValue<String, String> segment1KeyOnly = new KeyValue<>("segment1_only", "bar");
        final KeyValue<String, String> negativeSegmentKeyOnly = new KeyValue<>("negative_segment_only", "baz");

        segment0.put(new Bytes(serializeBytes(sharedKeyV1.key)), serializeBytes(sharedKeyV1.value));
        segment0.put(new Bytes(serializeBytes(segment0KeyOnly.key)), serializeBytes(segment0KeyOnly.value));
        segment1.put(new Bytes(serializeBytes(sharedKeyV2.key)), serializeBytes(sharedKeyV2.value));
        segment1.put(new Bytes(serializeBytes(segment1KeyOnly.key)), serializeBytes(segment1KeyOnly.value));
        negativeIdSegment.put(new Bytes(serializeBytes(sharedKeyV3.key)), serializeBytes(sharedKeyV3.value));
        negativeIdSegment.put(new Bytes(serializeBytes(negativeSegmentKeyOnly.key)), serializeBytes(negativeSegmentKeyOnly.value));

        assertEquals("v1", getAndDeserialize(segment0, "shared"));
        assertEquals("v2", getAndDeserialize(segment1, "shared"));
        assertEquals("v3", getAndDeserialize(negativeIdSegment, "shared"));

        assertEquals("foo", getAndDeserialize(segment0, "segment0_only"));
        assertNull(getAndDeserialize(segment1, "segment0_only"));
        assertNull(getAndDeserialize(negativeIdSegment, "segment0_only"));

        assertNull(getAndDeserialize(segment0, "segment1_only"));
        assertEquals("bar", getAndDeserialize(segment1, "segment1_only"));
        assertNull(getAndDeserialize(negativeIdSegment, "segment1_only"));

        assertNull(getAndDeserialize(segment0, "negative_segment_only"));
        assertNull(getAndDeserialize(segment1, "negative_segment_only"));
        assertEquals("baz", getAndDeserialize(negativeIdSegment, "negative_segment_only"));
    }

    @Test
    public void shouldPutAll() {
        final List<KeyValue<Bytes, byte[]>> segment0Records = new ArrayList<>();
        segment0Records.add(new KeyValue<>(
            new Bytes(serializeBytes("shared")),
            serializeBytes("v1")));
        segment0Records.add(new KeyValue<>(
            new Bytes(serializeBytes("segment0_only")),
            serializeBytes("foo")));

        final List<KeyValue<Bytes, byte[]>> segment1Records = new ArrayList<>();
        segment1Records.add(new KeyValue<>(
            new Bytes(serializeBytes("shared")),
            serializeBytes("v2")));
        segment1Records.add(new KeyValue<>(
            new Bytes(serializeBytes("segment1_only")),
            serializeBytes("bar")));

        final List<KeyValue<Bytes, byte[]>> negativeSegmentRecords = new ArrayList<>();
        negativeSegmentRecords.add(new KeyValue<>(
            new Bytes(serializeBytes("shared")),
            serializeBytes("v3")));
        negativeSegmentRecords.add(new KeyValue<>(
            new Bytes(serializeBytes("negative_segment_only")),
            serializeBytes("baz")));

        segment0.putAll(segment0Records);
        segment1.putAll(segment1Records);
        negativeIdSegment.putAll(negativeSegmentRecords);

        assertEquals("v1", getAndDeserialize(segment0, "shared"));
        assertEquals("v2", getAndDeserialize(segment1, "shared"));
        assertEquals("v3", getAndDeserialize(negativeIdSegment, "shared"));

        assertEquals("foo", getAndDeserialize(segment0, "segment0_only"));
        assertNull(getAndDeserialize(segment1, "segment0_only"));
        assertNull(getAndDeserialize(negativeIdSegment, "segment0_only"));

        assertNull(getAndDeserialize(segment0, "segment1_only"));
        assertEquals("bar", getAndDeserialize(segment1, "segment1_only"));
        assertNull(getAndDeserialize(negativeIdSegment, "segment1_only"));

        assertNull(getAndDeserialize(segment0, "negative_segment_only"));
        assertNull(getAndDeserialize(segment1, "negative_segment_only"));
        assertEquals("baz", getAndDeserialize(negativeIdSegment, "negative_segment_only"));
    }

    @Test
    public void shouldPutIfAbsent() {
        final Bytes keyBytes = new Bytes(serializeBytes("one"));
        final byte[] valueBytes1 = serializeBytes("A");
        final byte[] valueBytes2 = serializeBytes("B");
        final byte[] valueBytesUpdate = serializeBytes("C");

        segment0.putIfAbsent(keyBytes, valueBytes1);
        negativeIdSegment.putIfAbsent(keyBytes, valueBytes2);

        assertEquals("A", STRING_DESERIALIZER.deserialize(null, segment0.get(keyBytes)));
        assertEquals("B", STRING_DESERIALIZER.deserialize(null, negativeIdSegment.get(keyBytes)));

        segment0.putIfAbsent(keyBytes, valueBytesUpdate);
        negativeIdSegment.putIfAbsent(keyBytes, valueBytesUpdate);

        assertEquals("A", STRING_DESERIALIZER.deserialize(null, segment0.get(keyBytes)));
        assertEquals("B", STRING_DESERIALIZER.deserialize(null, negativeIdSegment.get(keyBytes)));
    }

    @Test
    public void shouldDelete() {
        final KeyValue<String, String> kv0 = new KeyValue<>("1", "a");
        final KeyValue<String, String> kv1 = new KeyValue<>("2", "b");

        segment0.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment0.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment0.delete(new Bytes(serializeBytes(kv0.key)));

        assertNull(getAndDeserialize(segment0, kv0.key));
        assertEquals("b", getAndDeserialize(segment0, "2"));
        assertEquals("a", getAndDeserialize(segment1, "1"));
        assertEquals("b", getAndDeserialize(segment1, "2"));
    }

    @Test
    public void shouldDeleteFromSegmentWithNegativeId() {
        final KeyValue<String, String> kv0 = new KeyValue<>("1", "a");
        final KeyValue<String, String> kv1 = new KeyValue<>("2", "b");

        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment2.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment2.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.delete(new Bytes(serializeBytes(kv0.key)));

        assertNull(getAndDeserialize(negativeIdSegment, kv0.key));
        assertEquals("b", getAndDeserialize(negativeIdSegment, "2"));
        assertEquals("a", getAndDeserialize(segment2, "1"));
        assertEquals("b", getAndDeserialize(segment2, "2"));
    }

    @Test
    public void shouldReturnValuesOnRange() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment0.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
        segment2.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));

        // non-null bounds
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment1.range(new Bytes(serializeBytes("1")), new Bytes(serializeBytes("2")))) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null lower bound
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment1.range(null, new Bytes(serializeBytes("1")))) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv0);
            expectedContents.add(kv1);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null upper bound
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment1.range(new Bytes(serializeBytes("1")), null)) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null upper and lower bounds
        try (final KeyValueIterator<Bytes, byte[]> iterator = segment1.range(null, null)) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv0);
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }
    }

    @Test
    public void shouldReturnValuesOnRangeFromSegmentWithNegativeId() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment0.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));

        // non-null bounds
        try (final KeyValueIterator<Bytes, byte[]> iterator = negativeIdSegment.range(new Bytes(serializeBytes("1")), new Bytes(serializeBytes("2")))) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null lower bound
        try (final KeyValueIterator<Bytes, byte[]> iterator = negativeIdSegment.range(null, new Bytes(serializeBytes("1")))) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv0);
            expectedContents.add(kv1);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null upper bound
        try (final KeyValueIterator<Bytes, byte[]> iterator = negativeIdSegment.range(new Bytes(serializeBytes("1")), null)) {
            final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
            expectedContents.add(kv1);
            expectedContents.add(kv2);
            assertEquals(expectedContents, getDeserializedList(iterator));
        }

        // null upper and lower bounds
        try (final KeyValueIterator<Bytes, byte[]> iterator = negativeIdSegment.range(null, null)) {
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
        segment0.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
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
    public void shouldReturnAllFromSegmentWithNegativeId() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment0.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
        segment1.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(kv0);
        expectedContents.add(kv1);
        expectedContents.add(kv2);

        try (final KeyValueIterator<Bytes, byte[]> iterator = negativeIdSegment.all()) {
            assertEquals(expectedContents, getDeserializedList(iterator));
        }
    }

    @Test
    public void shouldDeleteRange() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        segment0.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment0.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment0.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment1.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
        segment0.deleteRange(new Bytes(serializeBytes(kv0.key)), new Bytes(serializeBytes(kv1.key)));

        assertNull(getAndDeserialize(segment0, "0"));
        assertNull(getAndDeserialize(segment0, "1"));
        assertEquals("two", getAndDeserialize(segment0, "2"));
        assertEquals("other", getAndDeserialize(segment1, "1"));
    }

    @Test
    public void shouldDeleteRangeFromSegmentWithNegativeId() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");
        final KeyValue<String, String> kvOther = new KeyValue<>("1", "other");

        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv2.key)), serializeBytes(kv2.value));
        segment1.put(new Bytes(serializeBytes(kvOther.key)), serializeBytes(kvOther.value));
        negativeIdSegment.deleteRange(new Bytes(serializeBytes(kv0.key)), new Bytes(serializeBytes(kv1.key)));

        assertNull(getAndDeserialize(negativeIdSegment, "0"));
        assertNull(getAndDeserialize(negativeIdSegment, "1"));
        assertEquals("two", getAndDeserialize(negativeIdSegment, "2"));
        assertEquals("other", getAndDeserialize(segment1, "1"));
    }

    @Test
    public void shouldDestroy() {
        final KeyValue<String, String> kv0 = new KeyValue<>("1", "a");
        final KeyValue<String, String> kv1 = new KeyValue<>("2", "b");

        segment0.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment0.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));

        segment0.destroy();

        assertEquals("a", getAndDeserialize(segment1, "1"));
        assertEquals("b", getAndDeserialize(segment1, "2"));
        assertEquals("a", getAndDeserialize(negativeIdSegment, "1"));
        assertEquals("b", getAndDeserialize(negativeIdSegment, "2"));

        segment0 = new LogicalKeyValueSegment(0, "segment-0", physicalStore);

        assertNull(getAndDeserialize(segment0, "1"));
        assertNull(getAndDeserialize(segment0, "2"));
    }

    @Test
    public void shouldNotDestroySegmentWithNegativeId() {
        // reserved segments should not be destroyed. they are cleaned up only when
        // an entire store is closed, via the close() method rather than destroy()
        assertThrows(IllegalStateException.class, () -> negativeIdSegment.destroy());
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreClosed() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");

        segment0.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment0.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));

        final KeyValueIterator<Bytes, byte[]> range0 = segment0.range(null, new Bytes(serializeBytes("1")));
        final KeyValueIterator<Bytes, byte[]> all0 = segment0.all();
        final KeyValueIterator<Bytes, byte[]> range1 = segment1.range(null, new Bytes(serializeBytes("1")));
        final KeyValueIterator<Bytes, byte[]> rangeNegative = negativeIdSegment.range(null, new Bytes(serializeBytes("1")));

        assertTrue(range0.hasNext());
        assertTrue(all0.hasNext());
        assertTrue(range1.hasNext());
        assertTrue(rangeNegative.hasNext());

        segment0.close();

        assertThrows(InvalidStateStoreException.class, range0::hasNext);
        assertThrows(InvalidStateStoreException.class, all0::hasNext);
        assertTrue(range1.hasNext());
        assertTrue(rangeNegative.hasNext());
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreWithNegativeIdClosed() {
        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");

        segment0.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment0.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        segment1.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        segment1.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv0.key)), serializeBytes(kv0.value));
        negativeIdSegment.put(new Bytes(serializeBytes(kv1.key)), serializeBytes(kv1.value));

        final KeyValueIterator<Bytes, byte[]> rangeNegative = negativeIdSegment.range(null, new Bytes(serializeBytes("1")));
        final KeyValueIterator<Bytes, byte[]> allNegative = negativeIdSegment.all();
        final KeyValueIterator<Bytes, byte[]> range0 = segment0.range(null, new Bytes(serializeBytes("1")));
        final KeyValueIterator<Bytes, byte[]> range1 = segment1.range(null, new Bytes(serializeBytes("1")));

        assertTrue(rangeNegative.hasNext());
        assertTrue(allNegative.hasNext());
        assertTrue(range0.hasNext());
        assertTrue(range1.hasNext());

        negativeIdSegment.close();

        assertThrows(InvalidStateStoreException.class, rangeNegative::hasNext);
        assertThrows(InvalidStateStoreException.class, allNegative::hasNext);
        assertTrue(range0.hasNext());
        assertTrue(range1.hasNext());
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