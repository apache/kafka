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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeyBinaryConverter;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RocksDBSegmentedBytesStoreTest {

    private RocksDBSegmentedBytesStore sessionStore;

    @Before
    public void before() {
        sessionStore = new RocksDBSegmentedBytesStore("bytes-store",
                                                 10000L,
                                                 3,
                                                 new RocksDBSessionStore.SessionKeySchema());

        final MockProcessorContext context = new MockProcessorContext(null,
                                                                      TestUtils.tempDirectory(),
                                                                      Serdes.String(),
                                                                      Serdes.Long(),
                                                                      new NoOpRecordCollector(),
                                                                      new ThreadCache(0));
        sessionStore.init(context, sessionStore);
    }

    @After
    public void close() {
        sessionStore.close();
    }

    @Test
    public void shouldPutAndFetch() throws Exception {
        final String key = "a";
        sessionStore.put(serializeKey(new Windowed<>(key, new TimeWindow(10, 10L))), serializeValue(10L));
        sessionStore.put(serializeKey(new Windowed<>(key, new TimeWindow(500L, 1000L))), serializeValue(50L));
        sessionStore.put(serializeKey(new Windowed<>(key, new TimeWindow(1500L, 2000L))), serializeValue(100L));
        sessionStore.put(serializeKey(new Windowed<>(key, new TimeWindow(2500L, 3000L))), serializeValue(200L));

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>(key, new TimeWindow(10, 10)), 10L),
                                                                                    KeyValue.pair(new Windowed<>(key, new TimeWindow(500, 1000)), 50L));

        final KeyValueIterator<Bytes, byte[]> values = sessionStore.fetch(Bytes.wrap(key.getBytes()), 0, 1000L);
        assertEquals(expected, toList(values));
    }

    @Test
    public void shouldFindValuesWithinRange() throws Exception {
        final String key = "a";
        sessionStore.put(serializeKey(new Windowed<>(key, new TimeWindow(0L, 0L))), serializeValue(50L));
        sessionStore.put(serializeKey(new Windowed<>(key, new TimeWindow(1000L, 1000L))), serializeValue(10L));
        final KeyValueIterator<Bytes, byte[]> results = sessionStore.fetch(Bytes.wrap(key.getBytes()), 1L, 1999L);
        assertEquals(Collections.singletonList(KeyValue.pair(new Windowed<>(key, new TimeWindow(1000L, 1000L)), 10L)), toList(results));
    }

    @Test
    public void shouldRemove() throws Exception {
        sessionStore.put(serializeKey(new Windowed<>("a", new TimeWindow(0, 1000))), serializeValue(30L));
        sessionStore.put(serializeKey(new Windowed<>("a", new TimeWindow(1500, 2500))), serializeValue(50L));

        sessionStore.remove(serializeKey(new Windowed<>("a", new TimeWindow(0, 1000))));
        final KeyValueIterator<Bytes, byte[]> value = sessionStore.fetch(Bytes.wrap("a".getBytes()), 0, 1000L);
        assertFalse(value.hasNext());
    }

    private byte[] serializeValue(final long value) {
        return Serdes.Long().serializer().serialize("", value);
    }

    private Bytes serializeKey(final Windowed<String> key) {
        return SessionKeyBinaryConverter.toBinary(key, Serdes.String().serializer());
    }

    private List<KeyValue<Windowed<String>, Long>> toList(final KeyValueIterator<Bytes, byte[]> iterator) {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            final KeyValue<Bytes, byte[]> next = iterator.next();
            final KeyValue<Windowed<String>, Long> deserialized
                    = KeyValue.pair(SessionKeyBinaryConverter.from(next.key.get(), Serdes.String().deserializer()), Serdes.Long().deserializer().deserialize("", next.value));
            results.add(deserialized);
        }
        return results;
    }



}