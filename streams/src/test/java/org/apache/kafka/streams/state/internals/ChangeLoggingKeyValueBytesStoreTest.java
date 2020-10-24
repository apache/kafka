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

public class ChangeLoggingKeyValueBytesStoreTest {

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InMemoryKeyValueStore inner = new InMemoryKeyValueStore("kv");
    private final ChangeLoggingKeyValueBytesStore store = new ChangeLoggingKeyValueBytesStore(inner);
    private final Bytes hi = Bytes.wrap("hi".getBytes());
    private final Bytes hello = Bytes.wrap("hello".getBytes());
    private final byte[] there = "there".getBytes();
    private final byte[] world = "world".getBytes();

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
        final KeyValueStore<Bytes, byte[]> innerMock = EasyMock.mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingKeyValueBytesStore(innerMock);
        innerMock.init((ProcessorContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(innerMock);
        outer.init((ProcessorContext) context, outer);
        EasyMock.verify(innerMock);
    }

    @Test
    public void shouldDelegateInit() {
        final InternalMockProcessorContext context = mockContext();
        final KeyValueStore<Bytes, byte[]> innerMock = EasyMock.mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingKeyValueBytesStore(innerMock);
        innerMock.init((StateStoreContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(innerMock);
        outer.init((StateStoreContext) context, outer);
        EasyMock.verify(innerMock);
    }

    @Test
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, there);
        assertThat(inner.get(hi), equalTo(there));
        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
    }

    @Test
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, there),
                                   KeyValue.pair(hello, world)));
        assertThat(inner.get(hi), equalTo(there));
        assertThat(inner.get(hello), equalTo(world));

        assertThat(collector.collected().size(), equalTo(2));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
        assertThat(collector.collected().get(1).key(), equalTo(hello));
        assertThat(collector.collected().get(1).value(), equalTo(world));
    }

    @Test
    public void shouldPropagateDelete() {
        store.put(hi, there);
        store.delete(hi);
        assertThat(inner.approximateNumEntries(), equalTo(0L));
        assertThat(inner.get(hi), nullValue());
    }

    @Test
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, there);
        assertThat(store.delete(hi), equalTo(there));
    }

    @Test
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, there);
        assertThat(store.delete(hi), equalTo(there));

        assertThat(collector.collected().size(), equalTo(2));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
        assertThat(collector.collected().get(1).key(), equalTo(hi));
        assertThat(collector.collected().get(1).value(), nullValue());
    }

    @Test
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, there);
        assertThat(inner.get(hi), equalTo(there));
    }

    @Test
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        assertThat(inner.get(hi), equalTo(there));
    }

    @Test
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, there);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(hi));
        assertThat(collector.collected().get(0).value(), equalTo(there));
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, there);
        assertThat(store.putIfAbsent(hi, world), equalTo(there));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        assertThat(store.putIfAbsent(hi, there), is(nullValue()));
    }

    @Test
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, world);
        assertThat(store.get(hello), equalTo(world));
    }

    @Test
    public void shouldReturnNullOnGetWhenDoesntExist() {
        assertThat(store.get(hello), is(nullValue()));
    }
}
