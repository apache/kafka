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

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.toStoreKeyBinary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Test;

public class InMemoryWindowStoreTest extends WindowBytesStoreTest {

    private final static String STORE_NAME = "InMemoryWindowStore";

    @Override
    <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
        final long windowSize,
        final boolean retainDuplicates,
        final Serde<K> keySerde,
        final Serde<V> valueSerde) {
        return Stores.windowStoreBuilder(
            Stores.inMemoryWindowStore(
                STORE_NAME,
                ofMillis(retentionPeriod),
                ofMillis(windowSize),
                retainDuplicates),
            keySerde,
            valueSerde)
            .build();
    }

    @Override
    String getMetricsScope() {
        return new InMemoryWindowBytesStoreSupplier(null, 0, 0, false).metricsScope();
    }

    @Override
    void setClassLoggerToDebug() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore.class);
    }

    @Test
    public void shouldRestore() {
        // should be empty initially
        assertFalse(windowStore.all().hasNext());

        final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(),
            Serdes.String());

        final List<KeyValue<byte[], byte[]>> restorableEntries = new LinkedList<>();

        restorableEntries
            .add(new KeyValue<>(toStoreKeyBinary(1, 0L, 0, serdes).get(), serdes.rawValue("one")));
        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(2, WINDOW_SIZE, 0, serdes).get(),
            serdes.rawValue("two")));
        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(3, 2 * WINDOW_SIZE, 0, serdes).get(),
            serdes.rawValue("three")));

        context.restore(STORE_NAME, restorableEntries);
        final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore
            .fetchAll(0L, 2 * WINDOW_SIZE);

        assertEquals(windowedPair(1, "one", 0L), iterator.next());
        assertEquals(windowedPair(2, "two", WINDOW_SIZE), iterator.next());
        assertEquals(windowedPair(3, "three", 2 * WINDOW_SIZE), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotExpireFromOpenIterator() {

        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", 10L);

        windowStore.put(2, "one", 5L);
        windowStore.put(2, "two", 15L);

        final WindowStoreIterator<String> iterator1 = windowStore.fetch(1, 0L, 50L);
        final WindowStoreIterator<String> iterator2 = windowStore.fetch(2, 0L, 50L);

        // This put expires all four previous records, but they should still be returned from already open iterators
        windowStore.put(1, "four", 2 * RETENTION_PERIOD);

        assertEquals(new KeyValue<>(0L, "one"), iterator1.next());
        assertEquals(new KeyValue<>(5L, "one"), iterator2.next());

        assertEquals(new KeyValue<>(15L, "two"), iterator2.next());
        assertEquals(new KeyValue<>(10L, "two"), iterator1.next());

        assertFalse(iterator1.hasNext());
        assertFalse(iterator2.hasNext());

        iterator1.close();
        iterator2.close();

        // Make sure expired records are removed now that open iterators are closed
        assertFalse(windowStore.fetch(1, 0L, 50L).hasNext());
    }

    @Test
    public void testExpiration() {

        long currentTime = 0;
        setCurrentTime(currentTime);
        windowStore.put(1, "one");

        currentTime += RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "two");

        currentTime += RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "three");

        currentTime += RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "four");

        // increase current time to the full RETENTION_PERIOD to expire first record
        currentTime = currentTime + RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "five");

        KeyValueIterator<Windowed<Integer>, String> iterator = windowStore
            .fetchAll(0L, currentTime);

        // effect of this put (expires next oldest record, adds new one) should not be reflected in the already fetched results
        currentTime = currentTime + RETENTION_PERIOD / 4;
        setCurrentTime(currentTime);
        windowStore.put(1, "six");

        // should only have middle 4 values, as (only) the first record was expired at the time of the fetch
        // and the last was inserted after the fetch
        assertEquals(windowedPair(1, "two", RETENTION_PERIOD / 4), iterator.next());
        assertEquals(windowedPair(1, "three", RETENTION_PERIOD / 2), iterator.next());
        assertEquals(windowedPair(1, "four", 3 * (RETENTION_PERIOD / 4)), iterator.next());
        assertEquals(windowedPair(1, "five", RETENTION_PERIOD), iterator.next());
        assertFalse(iterator.hasNext());

        iterator = windowStore.fetchAll(0L, currentTime);

        // If we fetch again after the last put, the second oldest record should have expired and newest should appear in results
        assertEquals(windowedPair(1, "three", RETENTION_PERIOD / 2), iterator.next());
        assertEquals(windowedPair(1, "four", 3 * (RETENTION_PERIOD / 4)), iterator.next());
        assertEquals(windowedPair(1, "five", RETENTION_PERIOD), iterator.next());
        assertEquals(windowedPair(1, "six", 5 * (RETENTION_PERIOD / 4)), iterator.next());
        assertFalse(iterator.hasNext());
    }
    
}
