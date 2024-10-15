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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.DeserializationResult;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV0;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV1;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV3;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.duckTypeV2;

public final class InMemoryTimeOrderedKeyValueChangeBuffer<K, V, T> implements TimeOrderedKeyValueBuffer<K, V, Change<V>> {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
    private static final byte[] V_1_CHANGELOG_HEADER_VALUE = {(byte) 1};
    private static final byte[] V_2_CHANGELOG_HEADER_VALUE = {(byte) 2};
    private static final byte[] V_3_CHANGELOG_HEADER_VALUE = {(byte) 3};
    static final RecordHeaders CHANGELOG_HEADERS =
        new RecordHeaders(new Header[] {new RecordHeader("v", V_3_CHANGELOG_HEADER_VALUE)});
    private static final String METRIC_SCOPE = "in-memory-suppression";

    private final Map<Bytes, BufferKey> index = new HashMap<>();
    private final TreeMap<BufferKey, BufferValue> sortedMap = new TreeMap<>();

    private final Set<Bytes> dirtyKeys = new HashSet<>();
    private final String storeName;
    private final boolean loggingEnabled;

    private Serde<K> keySerde;
    private FullChangeSerde<V> valueSerde;

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;
    private InternalProcessorContext context;
    private String changelogTopic;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;
    private StreamsMetricsImpl streamsMetrics;
    private String taskId;

    private volatile boolean open;

    private int partition;

    public static class Builder<K, V> implements StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> {

        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private boolean loggingEnabled = true;
        private Map<String, String> logConfig = new HashMap<>();

        public Builder(final String storeName, final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.storeName = storeName;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         * <p>
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withCachingEnabled() {
            return this;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         * <p>
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withCachingDisabled() {
            return this;
        }

        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withLoggingEnabled(final Map<String, String> config) {
            logConfig = config;
            return this;
        }

        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withLoggingDisabled() {
            loggingEnabled = false;
            return this;
        }

        @Override
        public InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>> build() {
            return new InMemoryTimeOrderedKeyValueChangeBuffer<>(storeName, loggingEnabled, keySerde, valueSerde);
        }

        @Override
        public Map<String, String> logConfig() {
            return loggingEnabled() ? Collections.unmodifiableMap(logConfig) : Collections.emptyMap();
        }

        @Override
        public boolean loggingEnabled() {
            return loggingEnabled;
        }

        @Override
        public String name() {
            return storeName;
        }
    }

    private InMemoryTimeOrderedKeyValueChangeBuffer(final String storeName,
                                                    final boolean loggingEnabled,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde) {
        this.storeName = storeName;
        this.loggingEnabled = loggingEnabled;
        this.keySerde = keySerde;
        this.valueSerde = FullChangeSerde.wrap(valueSerde);
    }

    @Override
    public String name() {
        return storeName;
    }


    @Override
    public boolean persistent() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setSerdesIfNull(final SerdeGetter getter) {
        keySerde = keySerde == null ? (Serde<K>) getter.keySerde() : keySerde;
        valueSerde = valueSerde == null ? FullChangeSerde.wrap((Serde<V>) getter.valueSerde()) : valueSerde;
    }
    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        this.context = ProcessorContextUtils.asInternalProcessorContext(stateStoreContext);
        changelogTopic = ProcessorContextUtils.changelogFor(stateStoreContext, name(), Boolean.TRUE);
        taskId = context.taskId().toString();
        streamsMetrics = context.metrics();

        bufferSizeSensor = StateStoreMetrics.suppressionBufferSizeSensor(
            taskId,
            METRIC_SCOPE,
            storeName,
            streamsMetrics
        );
        bufferCountSensor = StateStoreMetrics.suppressionBufferCountSensor(
            taskId,
            METRIC_SCOPE,
            storeName,
            streamsMetrics
        );

        this.context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        updateBufferMetrics();
        open = true;
        partition = context.taskId().partition();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("This store does not keep track of the position.");
    }

    @Override
    public void close() {
        open = false;
        index.clear();
        sortedMap.clear();
        dirtyKeys.clear();
        memBufferSize = 0;
        minTimestamp = Long.MAX_VALUE;
        updateBufferMetrics();
        streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId, storeName);
    }

    @Override
    public void flush() {
        if (loggingEnabled) {
            // counting on this getting called before the record collector's flush
            for (final Bytes key : dirtyKeys) {

                final BufferKey bufferKey = index.get(key);

                if (bufferKey == null) {
                    // The record was evicted from the buffer. Send a tombstone.
                    logTombstone(key);
                } else {
                    final BufferValue value = sortedMap.get(bufferKey);

                    logValue(key, bufferKey, value);
                }
            }
            dirtyKeys.clear();
        }
    }

    private void logValue(final Bytes key, final BufferKey bufferKey, final BufferValue value) {

        final int sizeOfBufferTime = Long.BYTES;
        final ByteBuffer buffer = value.serialize(sizeOfBufferTime);
        buffer.putLong(bufferKey.time());
        final byte[] array = buffer.array();
        ((RecordCollector.Supplier) context).recordCollector().send(
            changelogTopic,
            key,
            array,
            CHANGELOG_HEADERS,
            partition,
            null,
            KEY_SERIALIZER,
            VALUE_SERIALIZER,
            null,
            null);
    }

    private void logTombstone(final Bytes key) {
        ((RecordCollector.Supplier) context).recordCollector().send(
            changelogTopic,
            key,
            null,
            null,
            partition,
            null,
            KEY_SERIALIZER,
            VALUE_SERIALIZER,
            null,
            null);
    }

    private void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> batch) {
        for (final ConsumerRecord<byte[], byte[]> record : batch) {
            if (record.partition() != partition) {
                throw new IllegalStateException(
                    String.format(
                        "record partition [%d] is being restored by the wrong suppress partition [%d]",
                        record.partition(),
                        partition
                    )
                );
            }
            final Bytes key = Bytes.wrap(record.key());
            if (record.value() == null) {
                // This was a tombstone. Delete the record.
                final BufferKey bufferKey = index.remove(key);
                if (bufferKey != null) {
                    final BufferValue removed = sortedMap.remove(bufferKey);
                    if (removed != null) {
                        memBufferSize -= computeRecordSize(bufferKey.key(), removed);
                    }
                    if (bufferKey.time() == minTimestamp) {
                        minTimestamp = sortedMap.isEmpty() ? Long.MAX_VALUE : sortedMap.firstKey().time();
                    }
                }
            } else {
                final Header versionHeader = record.headers().lastHeader("v");
                if (versionHeader == null) {
                    // Version 0:
                    // value:
                    //  - buffer time
                    //  - old value
                    //  - new value
                    final byte[] previousBufferedValue = index.containsKey(key)
                        ? internalPriorValueForBuffered(key)
                        : null;
                    final DeserializationResult deserializationResult = deserializeV0(record, key, previousBufferedValue);
                    cleanPut(deserializationResult.time(), deserializationResult.key(), deserializationResult.bufferValue());
                } else if (Arrays.equals(versionHeader.value(), V_3_CHANGELOG_HEADER_VALUE)) {
                    // Version 3:
                    // value:
                    //  - record context
                    //  - prior value
                    //  - old value
                    //  - new value
                    //  - buffer time
                    final DeserializationResult deserializationResult = deserializeV3(record, key);
                    cleanPut(deserializationResult.time(), deserializationResult.key(), deserializationResult.bufferValue());

                } else if (Arrays.equals(versionHeader.value(), V_2_CHANGELOG_HEADER_VALUE)) {
                    // Version 2:
                    // value:
                    //  - record context
                    //  - old value
                    //  - new value
                    //  - prior value
                    //  - buffer time
                    // NOTE: 2.4.0, 2.4.1, and 2.5.0 actually encode Version 3 formatted data,
                    // but still set the Version 2 flag, so to deserialize, we have to duck type.
                    final DeserializationResult deserializationResult = duckTypeV2(record, key);
                    cleanPut(deserializationResult.time(), deserializationResult.key(), deserializationResult.bufferValue());
                } else if (Arrays.equals(versionHeader.value(), V_1_CHANGELOG_HEADER_VALUE)) {
                    // Version 1:
                    // value:
                    //  - buffer time
                    //  - record context
                    //  - old value
                    //  - new value
                    final byte[] previousBufferedValue = index.containsKey(key)
                        ? internalPriorValueForBuffered(key)
                        : null;
                    final DeserializationResult deserializationResult = deserializeV1(record, key, previousBufferedValue);
                    cleanPut(deserializationResult.time(), deserializationResult.key(), deserializationResult.bufferValue());
                } else {
                    throw new IllegalArgumentException("Restoring apparently invalid changelog record: " + record);
                }
            }
        }
        updateBufferMetrics();
    }


    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<Eviction<K, Change<V>>> callback) {
        final List<Map.Entry<BufferKey, BufferValue>> entries = new ArrayList<>(sortedMap.entrySet());
        int evictions = 0;

        if (predicate.get()) {
            for (Iterator<Map.Entry<BufferKey, BufferValue>> entryIterator = entries.iterator(); entryIterator.hasNext(); ) {
                final Map.Entry<BufferKey, BufferValue> next = entryIterator.next();

                if (!predicate.get()) {
                    break;
                }

                if (next.getKey().time() != minTimestamp) {
                    throw new IllegalStateException(
                        "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                            next.getKey().time() + "]"
                    );
                }
                final K key = keySerde.deserializer().deserialize(changelogTopic, next.getKey().key().get());
                final BufferValue bufferValue = next.getValue();
                final Change<V> value = valueSerde.deserializeParts(
                    changelogTopic,
                    new Change<>(bufferValue.newValue(), bufferValue.oldValue())
                );
                callback.accept(new Eviction<K, Change<V>>(key, value, bufferValue.context()));

                sortedMap.remove(next.getKey());
                index.remove(next.getKey().key());

                if (loggingEnabled) {
                    dirtyKeys.add(next.getKey().key());
                }

                memBufferSize -= computeRecordSize(next.getKey().key(), bufferValue);
                minTimestamp = entries.isEmpty() ? Long.MAX_VALUE : entries.get(0).getKey().time();
                evictions++;
            }
        }
        if (evictions > 0) {
            updateBufferMetrics();
        }
    }

    @Override
    public Maybe<ValueAndTimestamp<V>> priorValueForBuffered(final K key) {
        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, key));
        if (index.containsKey(serializedKey)) {
            final byte[] serializedValue = internalPriorValueForBuffered(serializedKey);

            final V deserializedValue = valueSerde.innerSerde().deserializer().deserialize(
                changelogTopic,
                serializedValue
            );

            // it's unfortunately not possible to know this, unless we materialize the suppressed result, since our only
            // knowledge of the prior value is what the upstream processor sends us as the "old value" when we first
            // buffer something.
            return Maybe.defined(ValueAndTimestamp.make(deserializedValue, RecordQueue.UNKNOWN));
        } else {
            return Maybe.undefined();
        }
    }

    private byte[] internalPriorValueForBuffered(final Bytes key) {
        final BufferKey bufferKey = index.get(key);
        if (bufferKey == null) {
            throw new NoSuchElementException("Key [" + key + "] is not in the buffer.");
        } else {
            final BufferValue bufferValue = sortedMap.get(bufferKey);
            return bufferValue.priorValue();
        }
    }

    @Override
    public boolean put(final long time,
                       final Record<K, Change<V>> record,
                       final ProcessorRecordContext recordContext) {
        requireNonNull(record.value(), "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");

        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, record.key()));
        final Change<byte[]> serialChange = valueSerde.serializeParts(changelogTopic, record.value());

        final BufferValue buffered = getBuffered(serializedKey);
        final byte[] serializedPriorValue;
        if (buffered == null) {
            serializedPriorValue = serialChange.oldValue;
        } else {
            serializedPriorValue = buffered.priorValue();
        }

        cleanPut(
            time,
            serializedKey,
            new BufferValue(serializedPriorValue, serialChange.oldValue, serialChange.newValue, recordContext)
        );
        if (loggingEnabled) {
            dirtyKeys.add(serializedKey);
        }
        updateBufferMetrics();
        return true;
    }

    private BufferValue getBuffered(final Bytes key) {
        final BufferKey bufferKey = index.get(key);
        return bufferKey == null ? null : sortedMap.get(bufferKey);
    }

    private void cleanPut(final long time, final Bytes key, final BufferValue value) {
        // non-resetting semantics:
        // if there was a previous version of the same record,
        // then insert the new record in the same place in the priority queue

        final BufferKey previousKey = index.get(key);
        if (previousKey == null) {
            final BufferKey nextKey = new BufferKey(time, key);
            index.put(key, nextKey);
            sortedMap.put(nextKey, value);
            minTimestamp = Math.min(minTimestamp, time);
            memBufferSize += computeRecordSize(key, value);
        } else {
            final BufferValue removedValue = sortedMap.put(previousKey, value);
            memBufferSize =
                memBufferSize
                    + computeRecordSize(key, value)
                    - (removedValue == null ? 0 : computeRecordSize(key, removedValue));
        }
    }

    @Override
    public int numRecords() {
        return index.size();
    }

    @Override
    public long bufferSize() {
        return memBufferSize;
    }

    @Override
    public long minTimestamp() {
        return minTimestamp;
    }

    private static long computeRecordSize(final Bytes key, final BufferValue value) {
        long size = 0L;
        size += 8; // buffer time
        size += key.get().length;
        if (value != null) {
            size += value.residentMemorySizeEstimate();
        }
        return size;
    }

    private void updateBufferMetrics() {
        bufferSizeSensor.record(memBufferSize, context.currentSystemTimeMs());
        bufferCountSensor.record(index.size(), context.currentSystemTimeMs());
    }

    @Override
    public String toString() {
        return "InMemoryTimeOrderedKeyValueChangeBuffer{" +
            "storeName='" + storeName + '\'' +
            ", changelogTopic='" + changelogTopic + '\'' +
            ", open=" + open +
            ", loggingEnabled=" + loggingEnabled +
            ", minTimestamp=" + minTimestamp +
            ", memBufferSize=" + memBufferSize +
            ", \n\tdirtyKeys=" + dirtyKeys +
            ", \n\tindex=" + index +
            ", \n\tsortedMap=" + sortedMap +
            '}';
    }
}
