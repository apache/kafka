/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RocksDBWindowStoreTest {

    private final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
    private final String windowName = "window";
    private final int numSegments = 3;
    private final long segmentSize = RocksDBWindowStore.MIN_SEGMENT_INTERVAL;
    private final long retentionPeriod = segmentSize * (numSegments - 1);
    private final long windowSize = 3;
    private final Serde<Integer> intSerde = Serdes.Integer();
    private final Serde<String> stringSerde = Serdes.String();
    private final StateSerdes<Integer, String> serdes = new StateSerdes<>("", intSerde, stringSerde);

    @SuppressWarnings("unchecked")
    protected <K, V> WindowStore<K, V> createWindowStore(ProcessorContext context) {
        StateStoreSupplier supplier = new RocksDBWindowStoreSupplier<>(windowName, retentionPeriod, numSegments, true, intSerde, stringSerde);

        WindowStore<K, V> store = (WindowStore<K, V>) supplier.get();
        store.init(context, store);
        return store;
    }

    @Test
    public void testPutAndFetch() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            try {
                long startTime = segmentSize - 4L;

                context.setTime(startTime + 0L);
                store.put(0, "zero");
                context.setTime(startTime + 1L);
                store.put(1, "one");
                context.setTime(startTime + 2L);
                store.put(2, "two");
                context.setTime(startTime + 3L);
                // (3, "three") is not put
                context.setTime(startTime + 4L);
                store.put(4, "four");
                context.setTime(startTime + 5L);
                store.put(5, "five");

                assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime + 0L - windowSize, startTime + 0L + windowSize)));
                assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + 1L - windowSize, startTime + 1L + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + 3L - windowSize, startTime + 3L + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + 4L - windowSize, startTime + 4L + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + 5L - windowSize, startTime + 5L + windowSize)));

                context.setTime(startTime + 3L);
                store.put(2, "two+1");
                context.setTime(startTime + 4L);
                store.put(2, "two+2");
                context.setTime(startTime + 5L);
                store.put(2, "two+3");
                context.setTime(startTime + 6L);
                store.put(2, "two+4");
                context.setTime(startTime + 7L);
                store.put(2, "two+5");
                context.setTime(startTime + 8L);
                store.put(2, "two+6");

                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime - 2L - windowSize, startTime - 2L + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime - 1L - windowSize, startTime - 1L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1"), toList(store.fetch(2, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(store.fetch(2, startTime + 1L - windowSize, startTime + 1L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4"), toList(store.fetch(2, startTime + 3L - windowSize, startTime + 3L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5"), toList(store.fetch(2, startTime + 4L - windowSize, startTime + 4L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 5L - windowSize, startTime + 5L + windowSize)));
                assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 6L - windowSize, startTime + 6L + windowSize)));
                assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 7L - windowSize, startTime + 7L + windowSize)));
                assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 8L - windowSize, startTime + 8L + windowSize)));
                assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 9L - windowSize, startTime + 9L + windowSize)));
                assertEquals(Utils.mkList("two+5", "two+6"), toList(store.fetch(2, startTime + 10L - windowSize, startTime + 10L + windowSize)));
                assertEquals(Utils.mkList("two+6"), toList(store.fetch(2, startTime + 11L - windowSize, startTime + 11L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 12L - windowSize, startTime + 12L + windowSize)));

                // Flush the store and verify all current entries were properly flushed ...
                store.flush();

                Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

                assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
                assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
                assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
                assertNull(entriesByKey.get(3));
                assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
                assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
                assertNull(entriesByKey.get(6));

            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testPutAndFetchBefore() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            try {
                long startTime = segmentSize - 4L;

                context.setTime(startTime + 0L);
                store.put(0, "zero");
                context.setTime(startTime + 1L);
                store.put(1, "one");
                context.setTime(startTime + 2L);
                store.put(2, "two");
                context.setTime(startTime + 3L);
                // (3, "three") is not put
                context.setTime(startTime + 4L);
                store.put(4, "four");
                context.setTime(startTime + 5L);
                store.put(5, "five");

                assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime + 0L - windowSize, startTime + 0L)));
                assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + 1L - windowSize, startTime + 1L)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + 3L - windowSize, startTime + 3L)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + 4L - windowSize, startTime + 4L)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + 5L - windowSize, startTime + 5L)));

                context.setTime(startTime + 3L);
                store.put(2, "two+1");
                context.setTime(startTime + 4L);
                store.put(2, "two+2");
                context.setTime(startTime + 5L);
                store.put(2, "two+3");
                context.setTime(startTime + 6L);
                store.put(2, "two+4");
                context.setTime(startTime + 7L);
                store.put(2, "two+5");
                context.setTime(startTime + 8L);
                store.put(2, "two+6");

                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime - 1L - windowSize, startTime - 1L)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 0L - windowSize, startTime + 0L)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 1L - windowSize, startTime + 1L)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L - windowSize, startTime + 2L)));
                assertEquals(Utils.mkList("two", "two+1"), toList(store.fetch(2, startTime + 3L - windowSize, startTime + 3L)));
                assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(store.fetch(2, startTime + 4L - windowSize, startTime + 4L)));
                assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(store.fetch(2, startTime + 5L - windowSize, startTime + 5L)));
                assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(store.fetch(2, startTime + 6L - windowSize, startTime + 6L)));
                assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(store.fetch(2, startTime + 7L - windowSize, startTime + 7L)));
                assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 8L - windowSize, startTime + 8L)));
                assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 9L - windowSize, startTime + 9L)));
                assertEquals(Utils.mkList("two+5", "two+6"), toList(store.fetch(2, startTime + 10L - windowSize, startTime + 10L)));
                assertEquals(Utils.mkList("two+6"), toList(store.fetch(2, startTime + 11L - windowSize, startTime + 11L)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 12L - windowSize, startTime + 12L)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 13L - windowSize, startTime + 13L)));

                // Flush the store and verify all current entries were properly flushed ...
                store.flush();

                Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

                assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
                assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
                assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
                assertNull(entriesByKey.get(3));
                assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
                assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
                assertNull(entriesByKey.get(6));

            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testPutAndFetchAfter() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            try {
                long startTime = segmentSize - 4L;

                context.setTime(startTime + 0L);
                store.put(0, "zero");
                context.setTime(startTime + 1L);
                store.put(1, "one");
                context.setTime(startTime + 2L);
                store.put(2, "two");
                context.setTime(startTime + 3L);
                // (3, "three") is not put
                context.setTime(startTime + 4L);
                store.put(4, "four");
                context.setTime(startTime + 5L);
                store.put(5, "five");

                assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime + 0L, startTime + 0L + windowSize)));
                assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + 1L, startTime + 1L + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + 2L, startTime + 2L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + 3L, startTime + 3L + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + 4L, startTime + 4L + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + 5L, startTime + 5L + windowSize)));

                context.setTime(startTime + 3L);
                store.put(2, "two+1");
                context.setTime(startTime + 4L);
                store.put(2, "two+2");
                context.setTime(startTime + 5L);
                store.put(2, "two+3");
                context.setTime(startTime + 6L);
                store.put(2, "two+4");
                context.setTime(startTime + 7L);
                store.put(2, "two+5");
                context.setTime(startTime + 8L);
                store.put(2, "two+6");

                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime - 2L, startTime - 2L + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime - 1L, startTime - 1L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1"), toList(store.fetch(2, startTime, startTime + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(store.fetch(2, startTime + 1L, startTime + 1L + windowSize)));
                assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(store.fetch(2, startTime + 2L, startTime + 2L + windowSize)));
                assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(store.fetch(2, startTime + 3L, startTime + 3L + windowSize)));
                assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(store.fetch(2, startTime + 4L, startTime + 4L + windowSize)));
                assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 5L, startTime + 5L + windowSize)));
                assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(store.fetch(2, startTime + 6L, startTime + 6L + windowSize)));
                assertEquals(Utils.mkList("two+5", "two+6"), toList(store.fetch(2, startTime + 7L, startTime + 7L + windowSize)));
                assertEquals(Utils.mkList("two+6"), toList(store.fetch(2, startTime + 8L, startTime + 8L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 9L, startTime + 9L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 10L, startTime + 10L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 11L, startTime + 11L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + 12L, startTime + 12L + windowSize)));

                // Flush the store and verify all current entries were properly flushed ...
                store.flush();

                Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

                assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
                assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
                assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
                assertNull(entriesByKey.get(3));
                assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
                assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
                assertNull(entriesByKey.get(6));

            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testPutSameKeyTimestamp() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            try {
                long startTime = segmentSize - 4L;

                context.setTime(startTime);
                store.put(0, "zero");

                assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));

                context.setTime(startTime);
                store.put(0, "zero");
                context.setTime(startTime);
                store.put(0, "zero+");
                context.setTime(startTime);
                store.put(0, "zero++");

                assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime + 1L - windowSize, startTime + 1L + windowSize)));
                assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime + 2L - windowSize, startTime + 2L + windowSize)));
                assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(store.fetch(0, startTime + 3L - windowSize, startTime + 3L + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(0, startTime + 4L - windowSize, startTime + 4L + windowSize)));

                // Flush the store and verify all current entries were properly flushed ...
                store.flush();

                Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

                assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));

            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testRolling() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            RocksDBWindowStore<Integer, String> inner =
                    (RocksDBWindowStore<Integer, String>) ((MeteredWindowStore<Integer, String>) store).inner();
            try {
                long startTime = segmentSize * 2;
                long incr = segmentSize / 2;

                context.setTime(startTime);
                store.put(0, "zero");
                assertEquals(Utils.mkSet(2L), inner.segmentIds());

                context.setTime(startTime + incr);
                store.put(1, "one");
                assertEquals(Utils.mkSet(2L), inner.segmentIds());

                context.setTime(startTime + incr * 2);
                store.put(2, "two");
                assertEquals(Utils.mkSet(2L, 3L), inner.segmentIds());

                context.setTime(startTime + incr * 3);
                // (3, "three") is not put
                assertEquals(Utils.mkSet(2L, 3L), inner.segmentIds());

                context.setTime(startTime + incr * 4);
                store.put(4, "four");
                assertEquals(Utils.mkSet(2L, 3L, 4L), inner.segmentIds());

                context.setTime(startTime + incr * 5);
                store.put(5, "five");
                assertEquals(Utils.mkSet(2L, 3L, 4L), inner.segmentIds());

                assertEquals(Utils.mkList("zero"), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList("one"), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));

                context.setTime(startTime + incr * 6);
                store.put(6, "six");
                assertEquals(Utils.mkSet(3L, 4L, 5L), inner.segmentIds());

                assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
                assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));


                context.setTime(startTime + incr * 7);
                store.put(7, "seven");
                assertEquals(Utils.mkSet(3L, 4L, 5L), inner.segmentIds());

                assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
                assertEquals(Utils.mkList("two"), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
                assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
                assertEquals(Utils.mkList("seven"), toList(store.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));

                context.setTime(startTime + incr * 8);
                store.put(8, "eight");
                assertEquals(Utils.mkSet(4L, 5L, 6L), inner.segmentIds());

                assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
                assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
                assertEquals(Utils.mkList("seven"), toList(store.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));
                assertEquals(Utils.mkList("eight"), toList(store.fetch(8, startTime + incr * 8 - windowSize, startTime + incr * 8 + windowSize)));

                // check segment directories
                store.flush();
                assertEquals(
                        Utils.mkSet(inner.segmentName(4L), inner.segmentName(5L), inner.segmentName(6L)),
                        segmentDirs(baseDir)
                );
            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testRestore() throws IOException {
        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        long startTime = segmentSize * 2;
        long incr = segmentSize / 2;

        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            try {
                context.setTime(startTime);
                store.put(0, "zero");
                context.setTime(startTime + incr);
                store.put(1, "one");
                context.setTime(startTime + incr * 2);
                store.put(2, "two");
                context.setTime(startTime + incr * 3);
                store.put(3, "three");
                context.setTime(startTime + incr * 4);
                store.put(4, "four");
                context.setTime(startTime + incr * 5);
                store.put(5, "five");
                context.setTime(startTime + incr * 6);
                store.put(6, "six");
                context.setTime(startTime + incr * 7);
                store.put(7, "seven");
                context.setTime(startTime + incr * 8);
                store.put(8, "eight");
                store.flush();

            } finally {
                store.close();
            }


        } finally {
            Utils.delete(baseDir);
        }

        File baseDir2 = Files.createTempDirectory("test").toFile();
        try {
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    changeLog.add(new KeyValue<>(
                                    keySerializer.serialize(record.topic(), record.key()),
                                    valueSerializer.serialize(record.topic(), record.value()))
                    );
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            RocksDBWindowStore<Integer, String> inner =
                    (RocksDBWindowStore<Integer, String>) ((MeteredWindowStore<Integer, String>) store).inner();

            try {
                context.restore(windowName, changeLog);

                assertEquals(Utils.mkSet(4L, 5L, 6L), inner.segmentIds());

                assertEquals(Utils.mkList(), toList(store.fetch(0, startTime - windowSize, startTime + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(1, startTime + incr - windowSize, startTime + incr + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(2, startTime + incr * 2 - windowSize, startTime + incr * 2 + windowSize)));
                assertEquals(Utils.mkList(), toList(store.fetch(3, startTime + incr * 3 - windowSize, startTime + incr * 3 + windowSize)));
                assertEquals(Utils.mkList("four"), toList(store.fetch(4, startTime + incr * 4 - windowSize, startTime + incr * 4 + windowSize)));
                assertEquals(Utils.mkList("five"), toList(store.fetch(5, startTime + incr * 5 - windowSize, startTime + incr * 5 + windowSize)));
                assertEquals(Utils.mkList("six"), toList(store.fetch(6, startTime + incr * 6 - windowSize, startTime + incr * 6 + windowSize)));
                assertEquals(Utils.mkList("seven"), toList(store.fetch(7, startTime + incr * 7 - windowSize, startTime + incr * 7 + windowSize)));
                assertEquals(Utils.mkList("eight"), toList(store.fetch(8, startTime + incr * 8 - windowSize, startTime + incr * 8 + windowSize)));

                // check segment directories
                store.flush();
                assertEquals(
                        Utils.mkSet(inner.segmentName(4L), inner.segmentName(5L), inner.segmentName(6L)),
                        segmentDirs(baseDir)
                );
            } finally {
                store.close();
            }


        } finally {
            Utils.delete(baseDir2);
        }
    }

    @Test
    public void testSegmentMaintenance() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    // do nothing
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            WindowStore<Integer, String> store = createWindowStore(context);
            RocksDBWindowStore<Integer, String> inner =
                    (RocksDBWindowStore<Integer, String>) ((MeteredWindowStore<Integer, String>) store).inner();

            try {

                context.setTime(0L);
                store.put(0, "v");
                assertEquals(
                        Utils.mkSet(inner.segmentName(0L)),
                        segmentDirs(baseDir)
                );

                context.setTime(59999L);
                store.put(0, "v");
                context.setTime(59999L);
                store.put(0, "v");
                assertEquals(
                        Utils.mkSet(inner.segmentName(0L)),
                        segmentDirs(baseDir)
                );

                context.setTime(60000L);
                store.put(0, "v");
                assertEquals(
                        Utils.mkSet(inner.segmentName(0L), inner.segmentName(1L)),
                        segmentDirs(baseDir)
                );

                WindowStoreIterator iter;
                int fetchedCount;

                iter = store.fetch(0, 0L, 240000L);
                fetchedCount = 0;
                while (iter.hasNext()) {
                    iter.next();
                    fetchedCount++;
                }
                assertEquals(4, fetchedCount);

                assertEquals(
                        Utils.mkSet(inner.segmentName(0L), inner.segmentName(1L)),
                        segmentDirs(baseDir)
                );

                context.setTime(180000L);
                store.put(0, "v");

                iter = store.fetch(0, 0L, 240000L);
                fetchedCount = 0;
                while (iter.hasNext()) {
                    iter.next();
                    fetchedCount++;
                }
                assertEquals(2, fetchedCount);

                assertEquals(
                        Utils.mkSet(inner.segmentName(1L), inner.segmentName(2L), inner.segmentName(3L)),
                        segmentDirs(baseDir)
                );

                context.setTime(300000L);
                store.put(0, "v");

                iter = store.fetch(0, 240000L, 1000000L);
                fetchedCount = 0;
                while (iter.hasNext()) {
                    iter.next();
                    fetchedCount++;
                }
                assertEquals(1, fetchedCount);

                assertEquals(
                        Utils.mkSet(inner.segmentName(3L), inner.segmentName(4L), inner.segmentName(5L)),
                        segmentDirs(baseDir)
                );

            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testInitialLoading() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            Producer<byte[], byte[]> producer = new MockProducer<>(true, byteArraySerde.serializer(), byteArraySerde.serializer());
            RecordCollector recordCollector = new RecordCollector(producer) {
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    // do nothing
                }
            };

            MockProcessorContext context = new MockProcessorContext(
                    null, baseDir,
                    byteArraySerde, byteArraySerde,
                    recordCollector);

            File storeDir = new File(baseDir, windowName);

            WindowStore<Integer, String> store = createWindowStore(context);
            RocksDBWindowStore<Integer, String> inner =
                    (RocksDBWindowStore<Integer, String>) ((MeteredWindowStore<Integer, String>) store).inner();

            try {
                new File(storeDir, inner.segmentName(0L)).mkdir();
                new File(storeDir, inner.segmentName(1L)).mkdir();
                new File(storeDir, inner.segmentName(2L)).mkdir();
                new File(storeDir, inner.segmentName(3L)).mkdir();
                new File(storeDir, inner.segmentName(4L)).mkdir();
                new File(storeDir, inner.segmentName(5L)).mkdir();
                new File(storeDir, inner.segmentName(6L)).mkdir();
            } finally {
                store.close();
            }

            store = createWindowStore(context);
            inner = (RocksDBWindowStore<Integer, String>) ((MeteredWindowStore<Integer, String>) store).inner();

            try {
                assertEquals(
                        Utils.mkSet(inner.segmentName(4L), inner.segmentName(5L), inner.segmentName(6L)),
                        segmentDirs(baseDir)
                );

                WindowStoreIterator iter = store.fetch(0, 0L, 1000000L);
                while (iter.hasNext()) {
                    iter.next();
                }

                assertEquals(
                        Utils.mkSet(inner.segmentName(4L), inner.segmentName(5L), inner.segmentName(6L)),
                        segmentDirs(baseDir)
                );

            } finally {
                store.close();
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    private <E> List<E> toList(WindowStoreIterator<E> iterator) {
        ArrayList<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next().value);
        }
        return list;
    }

    private Set<String> segmentDirs(File baseDir) {
        File windowDir = new File(baseDir, windowName);

        return new HashSet<>(Arrays.asList(windowDir.list()));
    }

    private Map<Integer, Set<String>> entriesByKey(List<KeyValue<byte[], byte[]>> changeLog, long startTime) {
        HashMap<Integer, Set<String>> entriesByKey = new HashMap<>();

        for (KeyValue<byte[], byte[]> entry : changeLog) {
            long timestamp = WindowStoreUtils.timestampFromBinaryKey(entry.key);
            Integer key = WindowStoreUtils.keyFromBinaryKey(entry.key, serdes);
            String value = entry.value == null ? null : serdes.valueFrom(entry.value);

            Set<String> entries = entriesByKey.get(key);
            if (entries == null) {
                entries = new HashSet<>();
                entriesByKey.put(key, entries);
            }
            entries.add(value + "@" + (timestamp - startTime));
        }

        return entriesByKey;
    }
}
