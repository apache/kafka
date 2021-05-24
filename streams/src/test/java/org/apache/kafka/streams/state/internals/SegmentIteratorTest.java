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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SegmentIteratorTest {

    private final RocksDBMetricsRecorder rocksDBMetricsRecorder =
        new RocksDBMetricsRecorder("metrics-scope", "store-name");
    private final KeyValueSegment segmentOne =
        new KeyValueSegment("one", "one", 0, rocksDBMetricsRecorder);
    private final KeyValueSegment segmentTwo =
        new KeyValueSegment("two", "window", 1, rocksDBMetricsRecorder);
    private final HasNextCondition hasNextCondition = Iterator::hasNext;

    private SegmentIterator<KeyValueSegment> iterator = null;

    @Before
    public void before() {
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            new MockRecordCollector(),
            new ThreadCache(
                new LogContext("testCache "),
                0,
                new MockStreamsMetrics(new Metrics())));
        segmentOne.init((StateStoreContext) context, segmentOne);
        segmentTwo.init((StateStoreContext) context, segmentTwo);
        segmentOne.put(Bytes.wrap("a".getBytes()), "1".getBytes());
        segmentOne.put(Bytes.wrap("b".getBytes()), "2".getBytes());
        segmentTwo.put(Bytes.wrap("c".getBytes()), "3".getBytes());
        segmentTwo.put(Bytes.wrap("d".getBytes()), "4".getBytes());
    }

    @After
    public void closeSegments() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
        segmentOne.close();
        segmentTwo.close();
    }

    @Test
    public void shouldIterateOverAllSegments() {
        iterator = new SegmentIterator<>(
            Arrays.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("z".getBytes()),
            true);

        assertTrue(iterator.hasNext());
        assertEquals("a", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("a", "1"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("b", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("b", "2"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("c", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("c", "3"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("d", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("d", "4"), toStringKeyValue(iterator.next()));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldIterateBackwardOverAllSegments() {
        iterator = new SegmentIterator<>(
            Arrays.asList(segmentTwo, segmentOne).iterator(), //store should pass the segments in the right order
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("z".getBytes()),
            false);

        assertTrue(iterator.hasNext());
        assertEquals("d", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("d", "4"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("c", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("c", "3"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("b", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("b", "2"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("a", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("a", "1"), toStringKeyValue(iterator.next()));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotThrowExceptionOnHasNextWhenStoreClosed() {
        iterator = new SegmentIterator<>(
            Collections.singletonList(segmentOne).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("z".getBytes()),
            true);

        iterator.currentIterator = segmentOne.all();
        segmentOne.close();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldOnlyIterateOverSegmentsInBackwardRange() {
        iterator = new SegmentIterator<>(
            Arrays.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("b".getBytes()),
            false);

        assertTrue(iterator.hasNext());
        assertEquals("b", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("b", "2"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("a", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("a", "1"), toStringKeyValue(iterator.next()));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldOnlyIterateOverSegmentsInRange() {
        iterator = new SegmentIterator<>(
            Arrays.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("a".getBytes()),
            Bytes.wrap("b".getBytes()),
            true);

        assertTrue(iterator.hasNext());
        assertEquals("a", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("a", "1"), toStringKeyValue(iterator.next()));

        assertTrue(iterator.hasNext());
        assertEquals("b", new String(iterator.peekNextKey().get()));
        assertEquals(KeyValue.pair("b", "2"), toStringKeyValue(iterator.next()));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldThrowNoSuchElementOnPeekNextKeyIfNoNext() {
        iterator = new SegmentIterator<>(
            Arrays.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("f".getBytes()),
            Bytes.wrap("h".getBytes()),
            true);

        assertThrows(NoSuchElementException.class, () -> iterator.peekNextKey());
    }

    @Test
    public void shouldThrowNoSuchElementOnNextIfNoNext() {
        iterator = new SegmentIterator<>(
            Arrays.asList(segmentOne, segmentTwo).iterator(),
            hasNextCondition,
            Bytes.wrap("f".getBytes()),
            Bytes.wrap("h".getBytes()),
            true);

        assertThrows(NoSuchElementException.class, () -> iterator.next());
    }

    private KeyValue<String, String> toStringKeyValue(final KeyValue<Bytes, byte[]> binaryKv) {
        return KeyValue.pair(new String(binaryKv.key.get()), new String(binaryKv.value));
    }
}
