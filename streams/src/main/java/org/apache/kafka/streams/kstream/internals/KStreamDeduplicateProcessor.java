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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

class KStreamDeduplicateProcessor<K, V, KR> extends ContextualProcessor<K, V, K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamDeduplicateProcessor.class);

    private final KeyValueMapper<? super K, ? super V, ? extends KR> idSelector;
    private final Duration deduplicationInterval;
    private final String baseStoreName;
    private final String timeIndexStoreName;
    private final Serde<K> keySerde;
    private final Serde<KR> idSerde;
    private Serializer<K> keySerializer;


    private KeyValueStore<Bytes, TimestampAndOffset> baseStore;
    private TimeOrderedKeyValueBuffer<Bytes, V, V> timeIndexStore;
    private InternalProcessorContext<K, V> internalProcessorContext;
    private Sensor droppedRecordsSensor;
    protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    KStreamDeduplicateProcessor(final KeyValueMapper<? super K, ? super V, ? extends KR> idSelector,
                                final Duration deduplicationInterval,
                                final Serde<K> keySerde,
                                final Serde<KR> idSerde,
                                final String baseStoreName,
                                final String timeIndexStoreName) {
        this.idSelector = idSelector;
        this.deduplicationInterval = deduplicationInterval;
        this.keySerde = keySerde;
        this.idSerde = idSerde;
        this.baseStoreName = baseStoreName;
        this.timeIndexStoreName = timeIndexStoreName;
    }

    @SuppressWarnings({"unchecked", "resource"})
    @Override
    public void init(final ProcessorContext<K, V> context) {
        super.init(context);
        final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
        droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
        baseStore = context.getStateStore(baseStoreName);
        timeIndexStore = context.getStateStore(timeIndexStoreName);
        timeIndexStore.setSerdesIfNull(new SerdeGetter(context));
        internalProcessorContext = asInternalProcessorContext(context);

        if (keySerde == null || keySerde.serializer() == null) {
            keySerializer = (Serializer<K>) context.keySerde().serializer();
        } else {
            keySerializer = keySerde.serializer();
        }

        context.schedule(
            Duration.ofMillis(Math.max(1, deduplicationInterval.toMillis() / 10)),
            PunctuationType.STREAM_TIME,
            timestamp -> timeIndexStore.evictWhile(() -> true, eviction -> {
                final TimestampAndOffset stored = baseStore.get(eviction.key());
                // Only delete if the stored entry is still the expired one.
                // If a newer record overwrote the entry, stored.timestamp >= threshold, we don't delete.
                if (stored != null && stored.timestamp < observedStreamTime - deduplicationInterval.toMillis()) {
                    baseStore.delete(eviction.key());
                }
            })
        );
    }

    @Override
    public void process(final Record<K, V> record) {
        observedStreamTime = Math.max(observedStreamTime, record.timestamp());

        final KR id = idSelector.apply(record.key(), record.value());

        if (record.key() == null || id == null) {
            context().forward(record);
            return;
        }

        final Bytes dedupKey = computeDedupKey(record.key(), id);

        final TimestampAndOffset storedEntry = baseStore.get(dedupKey);

        if (storedEntry == null
            || (observedStreamTime - storedEntry.timestamp) > deduplicationInterval.toMillis()
            || (storedEntry.timestamp - record.timestamp()) > deduplicationInterval.toMillis()) {
            // - No active duplicate entry
            // - stale duplicate entry not yet cleaned up by punctuator
            // - record arrives more than deduplicationInterval before the stored entry (out-of-order late event).
            context().forward(record);

            // buffer.put returns false if the record is older than deduplicationInterval (late record).
            // In that case, forward it but don't start a dedup window — no baseStore entry needed.
            final boolean withinWindow = timeIndexStore.put(
                observedStreamTime,
                new Record<>(dedupKey, record.value(), record.timestamp()).withHeaders(record.headers()),
                internalProcessorContext.recordContext()
            );
            if (withinWindow) {
                baseStore.put(dedupKey, new TimestampAndOffset(record.timestamp(), currentOffset()));
            }
        } else {
            // Active entry exists and is within the deduplication interval.
            final Optional<Long> currentOffset = currentOffset();

            if (currentOffset.isPresent() && currentOffset.equals(storedEntry.offset)) {
                // Reprocessing a record that was put in the dedup store but wasn't committed -> forward (see KIP-655)
                context().forward(record);
            } else if (currentOffset.isEmpty()) {
                // Punctuated record (null offset)
                droppedRecordsSensor.record();
                LOG.debug("Dropping punctuated duplicate record for dedupKey=[{}]", dedupKey);
            } else {
                // Normal duplicate within the active window — drop.
                droppedRecordsSensor.record();
                LOG.debug("Dropping duplicate record for dedupKey=[{}]", dedupKey);
            }
        }
    }

    private Bytes computeDedupKey(final K key, final KR id) {
        final byte[] keyBytes = keySerializer.serialize(null, key);
        if (idSerde == null) {
            return Bytes.wrap(keyBytes);
        }
        final byte[] idBytes = idSerde.serializer().serialize(null, id);
        return Bytes.wrap(ByteBuffer.allocate(4 + keyBytes.length + idBytes.length)
            .putInt(keyBytes.length)
            .put(keyBytes)
            .put(idBytes)
            .array());
    }

    private Optional<Long> currentOffset() {
        return context().recordMetadata().map(RecordMetadata::offset);
    }
}
