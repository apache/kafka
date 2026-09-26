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
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingTimestampedKeyValueBytesStoreTest {

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InMemoryKeyValueStore root = new InMemoryKeyValueStore("kv");
    private final ChangeLoggingTimestampedKeyValueBytesStore store = new ChangeLoggingTimestampedKeyValueBytesStore(root);
    private final Bytes hi = Bytes.wrap("hi".getBytes());
    private final Bytes hello = Bytes.wrap("hello".getBytes());
    private final ValueAndTimestamp<byte[]> there = ValueAndTimestamp.make("there".getBytes(), 97L);
    // timestamp is 97 what is ASCII of 'a'
    private final byte[] rawThere = "\0\0\0\0\0\0\0athere".getBytes();
    private final ValueAndTimestamp<byte[]> world = ValueAndTimestamp.make("world".getBytes(), 98L);
    // timestamp is 98 what is ASCII of 'b'
    private final byte[] rawWorld = "\0\0\0\0\0\0\0bworld".getBytes();

    @BeforeEach
    public void before() {
        final InternalMockProcessorContext<String, Long> context = mockContext();
        context.setTime(0);
        store.init(context, store);
    }

    private InternalMockProcessorContext<String, Long> mockContext() {
        return new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
    }

    @AfterEach
    public void after() {
        store.close();
    }

    @Test
    public void shouldDelegateInit() {
        final InternalMockProcessorContext<String, Long> context = mockContext();
        final KeyValueStore<Bytes, byte[]> inner = mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingTimestampedKeyValueBytesStore(inner);

        outer.init(context, outer);
        verify(inner).init(context, outer);
    }

    @Test
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, rawThere);

        assertArrayEquals(rawThere, root.get(hi));
        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(there.timestamp(), collector.collected().get(0).timestamp());
    }

    @Test
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, rawThere),
                                   KeyValue.pair(hello, rawWorld)));
        assertArrayEquals(rawThere, root.get(hi));
        assertArrayEquals(rawWorld, root.get(hello));
    }

    @Test
    public void shouldLogChangesOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, rawThere),
                                   KeyValue.pair(hello, rawWorld)));

        assertEquals(2, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(there.timestamp(), collector.collected().get(0).timestamp());
        assertEquals(hello, collector.collected().get(1).key());
        assertArrayEquals(world.value(), (byte[]) collector.collected().get(1).value());
        assertEquals(world.timestamp(), collector.collected().get(1).timestamp());
    }

    @Test
    public void shouldPropagateDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        assertEquals(0L, root.approximateNumEntries());
        assertNull(root.get(hi));
    }

    @Test
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, rawThere);
        assertArrayEquals(rawThere, store.delete(hi));
    }

    @Test
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, rawThere);
        store.delete(hi);

        assertEquals(2, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(there.timestamp(), collector.collected().get(0).timestamp());
        assertEquals(hi, collector.collected().get(1).key());
        assertNull(collector.collected().get(1).value());
        assertEquals(0L, collector.collected().get(1).timestamp());

    }

    @Test
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        assertArrayEquals(rawThere, root.get(hi));
    }

    @Test
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        assertArrayEquals(rawThere, root.get(hi));
    }

    @Test
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(there.timestamp(), collector.collected().get(0).timestamp());
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(there.timestamp(), collector.collected().get(0).timestamp());
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, rawThere);
        assertArrayEquals(rawThere, store.putIfAbsent(hi, rawWorld));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        assertNull(store.putIfAbsent(hi, rawThere));
    }

    @Test
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, rawWorld);
        assertArrayEquals(rawWorld, store.get(hello));
    }

    @Test
    public void shouldReturnNullOnGetWhenDoesntExist() {
        assertNull(store.get(hello));
    }
}
