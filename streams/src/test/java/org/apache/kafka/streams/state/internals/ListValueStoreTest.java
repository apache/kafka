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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.junit.Assert.assertEquals;

public class ListValueStoreTest {
    private static final String STORE_NAME = "rocksDB list value store";

    MockRecordCollector recordCollector;
    KeyValueStore<Integer, String> listStore;
    InternalMockProcessorContext<Integer, String> context;

    final File baseDir = TestUtils.tempDirectory("test");

    @Before
    public void setup() {
        listStore = buildStore(Serdes.Integer(), Serdes.String());

        recordCollector = new MockRecordCollector();
        context = new InternalMockProcessorContext<>(
            baseDir,
            Serdes.String(),
            Serdes.Integer(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));
        context.setTime(1L);

        listStore.init((StateStoreContext) context, listStore);
    }

    @After
    public void after() {
        listStore.close();
    }

    <K, V> KeyValueStore<K, V> buildStore(final Serde<K> keySerde,
                                          final Serde<V> valueSerde) {
        return new ListValueStoreBuilder<>(
            new RocksDbKeyValueBytesStoreSupplier(STORE_NAME, false),
            keySerde,
            valueSerde,
            Time.SYSTEM)
            .build();
    }

    @Test
    public void shouldGetAll() {
        listStore.put(0, "zero");
        // should retain duplicates
        listStore.put(0, "zero again");
        listStore.put(1, "one");
        listStore.put(2, "two");

        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> zeroAgain = KeyValue.pair(0, "zero again");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");
        final KeyValue<Integer, String> two = KeyValue.pair(2, "two");

        assertEquals(
            asList(zero, zeroAgain, one, two),
            toList(listStore.all())
        );
    }

    @Test
    public void shouldGetAllNonDeletedRecords() {
        // Add some records
        listStore.put(0, "zero");
        listStore.put(1, "one");
        listStore.put(1, "one again");
        listStore.put(2, "two");
        listStore.put(3, "three");
        listStore.put(4, "four");

        // Delete some records
        listStore.put(1, null);
        listStore.put(3, null);

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> two = KeyValue.pair(2, "two");
        final KeyValue<Integer, String> four = KeyValue.pair(4, "four");

        assertEquals(
            asList(zero, two, four),
            toList(listStore.all())
        );
    }

    @Test
    public void shouldGetAllReturnTimestampOrderedRecords() {
        // Add some records in different order
        listStore.put(4, "four");
        listStore.put(0, "zero");
        listStore.put(2, "two1");
        listStore.put(3, "three");
        listStore.put(1, "one");

        // Add duplicates
        listStore.put(2, "two2");

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");
        final KeyValue<Integer, String> two1 = KeyValue.pair(2, "two1");
        final KeyValue<Integer, String> two2 = KeyValue.pair(2, "two2");
        final KeyValue<Integer, String> three = KeyValue.pair(3, "three");
        final KeyValue<Integer, String> four = KeyValue.pair(4, "four");

        assertEquals(
            asList(zero, one, two1, two2, three, four),
            toList(listStore.all())
        );
    }

    @Test
    public void shouldEarlyClosedIteratorStillGetAllRecords() {
        listStore.put(0, "zero");
        listStore.put(1, "one");

        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");

        final KeyValueIterator<Integer, String> it = listStore.all();
        assertEquals(zero, it.next());
        it.close();

        // A new all() iterator after a previous all() iterator was closed should return all elements.
        assertEquals(
            asList(zero, one),
            toList(listStore.all())
        );
    }

}
