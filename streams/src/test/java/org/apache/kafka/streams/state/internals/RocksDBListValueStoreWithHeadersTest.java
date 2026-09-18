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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that a persistent outer-join list-value store written by a pre-headers (PLAIN) version can
 * be reopened as a HEADERS-format {@link RocksDBListValueStoreWithHeaders} and read back correctly via
 * the dual-column-family migration — without the silent corruption / {@code SerializationException}
 * that a plain read-through {@code AggregationWithHeadersSerde} would produce.
 */
public class RocksDBListValueStoreWithHeadersTest {

    private static final String STORE_NAME = "outer-join-list-store";

    private final File baseDir = TestUtils.tempDirectory("list-upgrade");

    private InternalMockProcessorContext<?, ?> newContext() {
        final MockRecordCollector recordCollector = new MockRecordCollector();
        final ThreadCache cache = new ThreadCache(new LogContext("test"), 0, new MockStreamsMetrics(new Metrics()));
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>(
            baseDir, Serdes.String(), null, recordCollector, cache);
        context.setTime(1L);
        return context;
    }

    private KeyValueStore<String, LeftOrRightValue<String, String>> buildPlainStore() {
        return new ListValueStoreBuilder<String, LeftOrRightValue<String, String>>(
            Stores.persistentKeyValueStore(STORE_NAME),
            Serdes.String(),
            new LeftOrRightValueSerde<>(Serdes.String(), Serdes.String()),
            Time.SYSTEM)
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();
    }

    private KeyValueStore<String, AggregationWithHeaders<LeftOrRightValue<String, String>>> buildHeadersStore() {
        return new ListValueStoreBuilder<String, AggregationWithHeaders<LeftOrRightValue<String, String>>>(
            new RocksDBListValueHeadersBytesStoreSupplier(STORE_NAME),
            Serdes.String(),
            new AggregationWithHeadersSerde<>(new LeftOrRightValueSerde<>(Serdes.String(), Serdes.String())),
            Time.SYSTEM,
            true)
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();
    }

    @Test
    public void shouldReadPlainDataAfterUpgradeToHeadersStore() {
        // Phase 1 — write with the pre-headers PLAIN list store.
        final KeyValueStore<String, LeftOrRightValue<String, String>> plainStore = buildPlainStore();
        final InternalMockProcessorContext<?, ?> ctx1 = newContext();
        plainStore.init(ctx1, plainStore);
        // "right" is the value that got silently truncated pre-fix; "left" is the one that threw.
        plainStore.put("k1", LeftOrRightValue.makeRightValue("right"));
        plainStore.put("k1", LeftOrRightValue.makeLeftValue("left"));   // second element, same key (list)
        plainStore.put("k2", LeftOrRightValue.makeLeftValue("solo"));
        plainStore.flush();
        plainStore.close();

        // Phase 2 — reopen the SAME directory as a HEADERS dual-CF store and read back.
        final KeyValueStore<String, AggregationWithHeaders<LeftOrRightValue<String, String>>> headersStore = buildHeadersStore();
        final InternalMockProcessorContext<?, ?> ctx2 = newContext();
        headersStore.init(ctx2, headersStore);
        try {
            final List<KeyValue<String, AggregationWithHeaders<LeftOrRightValue<String, String>>>> all =
                toListAndCloseIterator(headersStore.all());

            assertEquals(3, all.size(), "all values from the old plain store should be readable");

            // k1 retains both list elements, in insertion order, uncorrupted.
            assertEquals("k1", all.get(0).key);
            assertEquals("right", all.get(0).value.aggregation().rightValue());
            assertNull(all.get(0).value.aggregation().leftValue());
            assertFalse(all.get(0).value.headers().iterator().hasNext(), "migrated headers should be empty");

            assertEquals("k1", all.get(1).key);
            assertEquals("left", all.get(1).value.aggregation().leftValue());

            assertEquals("k2", all.get(2).key);
            assertEquals("solo", all.get(2).value.aggregation().leftValue());
        } finally {
            headersStore.close();
        }
    }

    @Test
    public void shouldAppendAfterUpgradeAndKeepUniformFormat() {
        final KeyValueStore<String, LeftOrRightValue<String, String>> plainStore = buildPlainStore();
        final InternalMockProcessorContext<?, ?> ctx1 = newContext();
        plainStore.init(ctx1, plainStore);
        plainStore.put("k1", LeftOrRightValue.makeRightValue("old"));
        plainStore.flush();
        plainStore.close();

        final KeyValueStore<String, AggregationWithHeaders<LeftOrRightValue<String, String>>> headersStore = buildHeadersStore();
        final InternalMockProcessorContext<?, ?> ctx2 = newContext();
        headersStore.init(ctx2, headersStore);
        try {
            // Append a new headers-format element to a key that still lives in the legacy DEFAULT CF.
            headersStore.put("k1", AggregationWithHeaders.makeAllowNullable(
                LeftOrRightValue.makeLeftValue("new"), new org.apache.kafka.common.header.internals.RecordHeaders()));

            final List<KeyValue<String, AggregationWithHeaders<LeftOrRightValue<String, String>>>> all =
                toListAndCloseIterator(headersStore.all());

            assertEquals(2, all.size());
            assertEquals("old", all.get(0).value.aggregation().rightValue()); // migrated legacy element
            assertEquals("new", all.get(1).value.aggregation().leftValue());  // freshly appended element
        } finally {
            headersStore.close();
        }
    }
}
