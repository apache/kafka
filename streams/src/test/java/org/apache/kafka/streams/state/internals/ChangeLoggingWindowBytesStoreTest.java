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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.NoOpRecordCollector;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static org.junit.Assert.assertArrayEquals;

@RunWith(EasyMockRunner.class)
public class ChangeLoggingWindowBytesStoreTest {

    private final TaskId taskId = new TaskId(0, 0);
    private final Map<Object, Object> sent = new HashMap<>();
    private final NoOpRecordCollector collector = new NoOpRecordCollector() {
        @Override
        public <K, V> void send(final String topic,
                                final K key,
                                final V value,
                                final Headers headers,
                                final Integer partition,
                                final Long timestamp,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valueSerializer) {
            sent.put(key, value);
        }
    };

    private final byte[] value1 = {0};
    private final Bytes bytesKey = Bytes.wrap(value1);

    @Mock(type = MockType.NICE)
    private WindowStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContextImpl context;
    private ChangeLoggingWindowBytesStore store;


    @Before
    public void setUp() {
        store = new ChangeLoggingWindowBytesStore(inner, false);
    }

    private void init() {
        EasyMock.expect(context.taskId()).andReturn(taskId);
        EasyMock.expect(context.recordCollector()).andReturn(collector);
        inner.init(context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(inner, context);

        store.init(context, store);
    }

    @Test
    public void shouldLogPuts() {
        inner.put(bytesKey, value1, 0);
        EasyMock.expectLastCall();

        init();

        store.put(bytesKey, value1);

        assertArrayEquals(value1, (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)));
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetching() {
        EasyMock.expect(inner.fetch(bytesKey, 0, 10)).andReturn(KeyValueIterators.<byte[]>emptyWindowStoreIterator());

        init();

        store.fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetchingRange() {
        EasyMock.expect(inner.fetch(bytesKey, bytesKey, 0, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());

        init();

        store.fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));
        EasyMock.verify(inner);
    }

    @Test
    public void shouldRetainDuplicatesWhenSet() {
        store = new ChangeLoggingWindowBytesStore(inner, true);
        inner.put(bytesKey, value1, 0);
        EasyMock.expectLastCall().times(2);

        init();
        store.put(bytesKey, value1);
        store.put(bytesKey, value1);

        assertArrayEquals(value1, (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)));
        assertArrayEquals(value1, (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)));

        EasyMock.verify(inner);
    }

}
