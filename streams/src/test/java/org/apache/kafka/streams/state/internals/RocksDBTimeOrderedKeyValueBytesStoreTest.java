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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


class RocksDBTimeOrderedKeyValueBytesStoreTest {

    private InternalMockProcessorContext<?, ?> context;
    private RocksDBTimeOrderedKeyValueBytesStore bytesStore;
    private File stateDir;
    final String storeName = "bytes-store";
    private static final String METRICS_SCOPE = "metrics-scope";
    private final String topic = "changelog";


    @BeforeEach
    public void before() {
        bytesStore = new RocksDBTimeOrderedKeyValueBytesStore(storeName, METRICS_SCOPE);

        stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            stateDir,
            Serdes.String(),
            Serdes.Long(),
            new MockRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        bytesStore.init(context, bytesStore);
    }

    @AfterEach
    public void close() {
        bytesStore.close();
    }

    @Test
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(key, 0, 0L).get(), serializeValue(50L)));
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(key, 1, 1L).get(), serializeValue(100L)));
        final Map<KeyValueSegment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(1, writeBatchMap.size());

        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(2, batch.count());
        }
    }

    @Test
    public void shouldCreateEmptyWriteBatches() {
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        final Map<KeyValueSegment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(0, writeBatchMap.size());
    }

    @SuppressWarnings("resource")
    private byte[] serializeValue(final Long value) {
        final Serde<Long> valueSerde = new Serdes.LongSerde();
        final byte[] valueBytes = valueSerde.serializer().serialize(topic, value);
        final BufferValue buffered = new BufferValue(null, null, valueBytes, new ProcessorRecordContext(0, 0, 0, topic, new RecordHeaders()));
        return buffered.serialize(0).array();
    }

    @SuppressWarnings("resource")
    private Bytes serializeKey(final String key, final int seqnum, final long timestamp) {
        final Serde<String> keySerde = new Serdes.StringSerde();
        return Bytes.wrap(
            PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.toStoreKeyBinary(keySerde.serializer().serialize(topic, key),
                timestamp,
                seqnum).get());
    }
}
