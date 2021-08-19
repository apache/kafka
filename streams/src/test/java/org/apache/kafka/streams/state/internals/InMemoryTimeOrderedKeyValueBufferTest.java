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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

public class InMemoryTimeOrderedKeyValueBufferTest {

    @Test
    public void bufferShouldAllowCacheEnablement() {
        new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null).withCachingEnabled();
    }

    @Test
    public void bufferShouldAllowCacheDisablement() {
        new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null).withCachingDisabled();
    }

    @Test
    public void bufferShouldAllowLoggingEnablement() {
        final String expect = "3";
        final Map<String, String> logConfig = new HashMap<>();
        logConfig.put("min.insync.replicas", expect);
        final StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<Object, Object>> builder =
            new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null)
                .withLoggingEnabled(logConfig);

        assertThat(builder.logConfig(), is(singletonMap("min.insync.replicas", expect)));
        assertThat(builder.loggingEnabled(), is(true));
    }

    @Test
    public void bufferShouldAllowLoggingDisablement() {
        final StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<Object, Object>> builder
            = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null)
                .withLoggingDisabled();

        assertThat(builder.logConfig(), is(emptyMap()));
        assertThat(builder.loggingEnabled(), is(false));
    }

    @Test
    public void shouldDeleteAllDuplicates() {
        final WindowStore<Integer, String> windowStore = new TimeOrderedWindowStoreBuilder<>(
            new InMemoryWindowBytesStoreSupplier(
                "store",
                60_000L,
                3L,
                true),
            Serdes.Integer(),
            Serdes.String(),
            Time.SYSTEM)
            .build();

        final InternalMockProcessorContext<Integer, String> context =
            new InternalMockProcessorContext<>(
                TestUtils.tempDirectory("test"),
                Serdes.Integer(),
                Serdes.String(),
                new MockRecordCollector(),
                new ThreadCache(
                    new LogContext("testCache"),
                    0,
                    new MockStreamsMetrics(new Metrics())
                )
            );
        context.setTime(1L);

        windowStore.init((StateStoreContext) context, windowStore);

        windowStore.put(0, "zero1", 0);
        windowStore.put(0, "zero2", 1);
        windowStore.put(0, "zero3", 1);
        windowStore.put(0, "zero4", 1);
        windowStore.put(0, "zero5", 2);
        windowStore.put(1, "one1", 1);
        windowStore.put(1, "one2", 1);

        final KeyValue<Windowed<Integer>, String> zero1 = windowedPair(0, "zero1", 0);
        final KeyValue<Windowed<Integer>, String> zero2 = windowedPair(0, "zero2", 1);
        final KeyValue<Windowed<Integer>, String> zero3 = windowedPair(0, "zero3", 1);
        final KeyValue<Windowed<Integer>, String> zero4 = windowedPair(0, "zero4", 1);
        final KeyValue<Windowed<Integer>, String> zero5 = windowedPair(0, "zero5", 2);
        final KeyValue<Windowed<Integer>, String> one1 = windowedPair(1, "one1", 1);
        final KeyValue<Windowed<Integer>, String> one2 = windowedPair(1, "one2", 1);

        assertEquals(
            asList(zero1, zero2, zero3, zero4, one1, one2, zero5),
            toList(windowStore.all())
        );

        windowStore.put(0, null, 1);

        assertEquals(
            asList(zero1, one1, one2, zero5),
            toList(windowStore.all())
        );
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(final K key, final V value, final long timestamp) {
        return KeyValue.pair(new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, 3L)), value);
    }
}
