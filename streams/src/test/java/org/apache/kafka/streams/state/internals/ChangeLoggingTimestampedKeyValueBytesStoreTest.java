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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

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

    @Before
    public void before() {
        final InternalMockProcessorContext context = mockContext();
        context.setTime(0);
        store.init((StateStoreContext) context, store);
    }

    private InternalMockProcessorContext mockContext() {
        return new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
    }

    @After
    public void after() {
        store.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final InternalMockProcessorContext context = mockContext();
        final KeyValueStore<Bytes, byte[]> inner = EasyMock.mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingTimestampedKeyValueBytesStore(inner);
        inner.init((ProcessorContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        outer.init((ProcessorContext) context, outer);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        final InternalMockProcessorContext context = mockContext();
        final KeyValueStore<Bytes, byte[]> inner = EasyMock.mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingTimestampedKeyValueBytesStore(inner);
        inner.init((StateStoreContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        outer.init((StateStoreContext) context, outer);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, rawThere);

        assertThat(root.get(hi), equalTo(rawThere));
        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there.value()));
        assertThat(collector.collected().get(0).timestamp(), equalTo(there.timestamp()));
    }

    @Test
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, rawThere),
                                   KeyValue.pair(hello, rawWorld)));
        assertThat(root.get(hi), equalTo(rawThere));
        assertThat(root.get(hello), equalTo(rawWorld));
    }

    @Test
    public void shouldLogChangesOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, rawThere),
                                   KeyValue.pair(hello, rawWorld)));

        assertThat(collector.collected().size(), equalTo(2));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there.value()));
        assertThat(collector.collected().get(0).timestamp(), equalTo(there.timestamp()));
        assertThat(collector.collected().get(1).key(), equalTo(hello));
        assertThat(collector.collected().get(1).value(), equalTo(world.value()));
        assertThat(collector.collected().get(1).timestamp(), equalTo(world.timestamp()));
    }

    @Test
    public void shouldPropagateDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        assertThat(root.approximateNumEntries(), equalTo(0L));
        assertThat(root.get(hi), nullValue());
    }

    @Test
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, rawThere);
        assertThat(store.delete(hi), equalTo(rawThere));
    }

    @Test
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, rawThere);
        store.delete(hi);

        assertThat(collector.collected().size(), equalTo(2));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there.value()));
        assertThat(collector.collected().get(0).timestamp(), equalTo(there.timestamp()));
        assertThat(collector.collected().get(1).key(), equalTo(hi));
        assertThat(collector.collected().get(1).value(), nullValue());
        assertThat(collector.collected().get(1).timestamp(), equalTo(0L));

    }

    @Test
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        assertThat(root.get(hi), equalTo(rawThere));
    }

    @Test
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        assertThat(root.get(hi), equalTo(rawThere));
    }

    @Test
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there.value()));
        assertThat(collector.collected().get(0).timestamp(), equalTo(there.timestamp()));
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there.value()));
        assertThat(collector.collected().get(0).timestamp(), equalTo(there.timestamp()));
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, rawThere);
        assertThat(store.putIfAbsent(hi, rawWorld), equalTo(rawThere));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        assertThat(store.putIfAbsent(hi, rawThere), is(nullValue()));
    }

    @Test
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, rawWorld);
        assertThat(store.get(hello), equalTo(rawWorld));
    }

    @Test
    public void shouldReturnNullOnGetWhenDoesntExist() {
        assertThat(store.get(hello), is(nullValue()));
    }
}
