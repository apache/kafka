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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RocksDBSegmentedBytesStoreTest {

    private final long retention = 60000L;
    private final int numSegments = 3;
    private final String storeName = "bytes-store";
    private RocksDBSegmentedBytesStore bytesStore;
    private File stateDir;

    @Before
    public void before() {

        bytesStore = new RocksDBSegmentedBytesStore(storeName,
                                                    retention,
                                                    numSegments,
                                                    new SessionKeySchema());

        stateDir = TestUtils.tempDirectory();
        final MockProcessorContext context = new MockProcessorContext(stateDir,
                                                                      Serdes.String(),
                                                                      Serdes.Long(),
                                                                      new NoOpRecordCollector(),
                                                                      new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
        bytesStore.init(context, bytesStore);
    }

    @After
    public void close() {
        bytesStore.close();
    }

    @Test
    public void shouldPutAndFetch() throws Exception {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(10, 10L))), serializeValue(10L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(500L, 1000L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(1500L, 2000L))), serializeValue(100L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(2500L, 3000L))), serializeValue(200L));

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(10, 10)), 10L),
                                                                                    KeyValue.pair(new Windowed<>(key, new SessionWindow(500, 1000)), 50L));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1000L);
        assertEquals(expected, toList(values));
    }

    @Test
    public void shouldFindValuesWithinRange() throws Exception {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(1000L, 1000L))), serializeValue(10L));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1L, 1999L);
        assertEquals(Collections.singletonList(KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 10L)), toList(results));
    }

    @Test
    public void shouldRemove() throws Exception {
        bytesStore.put(serializeKey(new Windowed<>("a", new SessionWindow(0, 1000))), serializeValue(30L));
        bytesStore.put(serializeKey(new Windowed<>("a", new SessionWindow(1500, 2500))), serializeValue(50L));

        bytesStore.remove(serializeKey(new Windowed<>("a", new SessionWindow(0, 1000))));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 1000L);
        assertFalse(value.hasNext());
    }

    @Test
    public void shouldRollSegments() throws Exception {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(61000L, 120000L))), serializeValue(200L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1),
                                 segments.segmentName(2)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(121000L, 180000L))), serializeValue(300L));
        assertEquals(Utils.mkSet(segments.segmentName(1),
                                 segments.segmentName(2),
                                 segments.segmentName(3)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(181000L, 240000L))), serializeValue(400L));
        assertEquals(Utils.mkSet(segments.segmentName(2),
                                 segments.segmentName(3),
                                 segments.segmentName(4)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 240000));
        assertEquals(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(61000L, 120000L)), 200L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(121000L, 180000L)), 300L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(181000L, 240000L)), 400L)
                                                 ), results);

    }

    private Set<String> segmentDirs() {
        File windowDir = new File(stateDir, storeName);

        return new HashSet<>(Arrays.asList(windowDir.list()));
    }

    private byte[] serializeValue(final long value) {
        return Serdes.Long().serializer().serialize("", value);
    }

    private Bytes serializeKey(final Windowed<String> key) {
        return SessionKeySerde.toBinary(key, Serdes.String().serializer());
    }

    private List<KeyValue<Windowed<String>, Long>> toList(final KeyValueIterator<Bytes, byte[]> iterator) {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            final KeyValue<Bytes, byte[]> next = iterator.next();
            final KeyValue<Windowed<String>, Long> deserialized
                    = KeyValue.pair(SessionKeySerde.from(next.key.get(), Serdes.String().deserializer()), Serdes.Long().deserializer().deserialize("", next.value));
            results.add(deserialized);
        }
        return results;
    }

}
