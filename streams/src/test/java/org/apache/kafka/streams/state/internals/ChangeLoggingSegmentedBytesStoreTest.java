/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.SegmentedBytesStoreStub;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ChangeLoggingSegmentedBytesStoreTest {

    private final SegmentedBytesStoreStub bytesStore = new SegmentedBytesStoreStub();
    private final ChangeLoggingSegmentedBytesStore store = new ChangeLoggingSegmentedBytesStore(bytesStore);
    private final Map sent = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        final NoOpRecordCollector collector = new NoOpRecordCollector() {
            @Override
            public <K, V> void send(final String topic,
                                    K key,
                                    V value,
                                    Integer partition,
                                    Long timestamp,
                                    Serializer<K> keySerializer,
                                    Serializer<V> valueSerializer) {
                sent.put(key, value);
            }
        };
        final MockProcessorContext context = new MockProcessorContext(TestUtils.tempDirectory(),
                                                                      Serdes.String(),
                                                                      Serdes.Long(),
                                                                      collector,
                                                                      new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
        context.setTime(0);
        store.init(context, store);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldLogPuts() throws Exception {
        final byte[] value1 = {0};
        final byte[] value2 = {1};
        final Bytes key1 = Bytes.wrap(value1);
        final Bytes key2 = Bytes.wrap(value2);
        store.put(key1, value1);
        store.put(key2, value2);
        store.flush();
        assertArrayEquals(value1, (byte[]) sent.get(key1));
        assertArrayEquals(value2, (byte[]) sent.get(key2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldLogRemoves() throws Exception {
        final Bytes key1 = Bytes.wrap(new byte[]{0});
        final Bytes key2 = Bytes.wrap(new byte[]{1});
        store.remove(key1);
        store.remove(key2);
        store.flush();
        assertTrue(sent.containsKey(key1));
        assertTrue(sent.containsKey(key2));
        assertNull(sent.get(key1));
        assertNull(sent.get(key2));
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetching() throws Exception {
        store.fetch(Bytes.wrap(new byte[0]), 1, 1);
        assertTrue(bytesStore.fetchCalled);
    }

    @Test
    public void shouldFlushUnderlyingStore() throws Exception {
        store.flush();
        assertTrue(bytesStore.flushed);
    }

    @Test
    public void shouldCloseUnderlyingStore() throws Exception {
        store.close();
        assertTrue(bytesStore.closed);
    }

    @Test
    public void shouldInitUnderlyingStore() throws Exception {
        assertTrue(bytesStore.initialized);
    }

}