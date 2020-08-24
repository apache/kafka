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
import org.apache.kafka.streams.MemoryBudget;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.DeserializationResult;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

public final class InMemoryTimeOrderedKeyValueBuffer<K, V> implements TimeOrderedKeyValueBuffer<K, V> {
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
    private MemoryBudget memoryBudget;
    private MemoryBudget.AllocationType memoryBudgetAllocationType;
    private long minTimestamp = Long.MAX_VALUE;
    private InternalProcessorContext context;
    private String changelogTopic;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;
    private StreamsMetricsImpl streamsMetrics;
    private String threadId;
    private String taskId;

    private volatile boolean open;

    private int partition;

    public static class Builder<K, V> implements StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> {

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
        public StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withCachingEnabled() {
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
        public StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withCachingDisabled() {
            return this;
        }

        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withLoggingEnabled(final Map<String, String> config) {
            logConfig = config;
            return this;
        }

        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withLoggingDisabled() {
            loggingEnabled = false;
            return this;
        }

        @Override
        public InMemoryTimeOrderedKeyValueBuffer<K, V> build() {
            return new InMemoryTimeOrderedKeyValueBuffer<>(storeName, loggingEnabled, keySerde, valueSerde);
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

    private InMemoryTimeOrderedKeyValueBuffer(final String storeName,
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

    @Override
    public void setSerdesIfNull(final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.keySerde = this.keySerde == null ? keySerde : this.keySerde;
        this.valueSerde = this.valueSerde == null ? FullChangeSerde.wrap(valueSerde) : this.valueSerde;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        taskId = context.taskId().toString();
        this.context = (InternalProcessorContext) context;
        streamsMetrics = this.context.metrics();

        threadId = Thread.currentThread().getName();
        bufferSizeSensor = StateStoreMetrics.suppressionBufferSizeSensor(
            threadId,
            taskId,
            METRIC_SCOPE,
            storeName,
            streamsMetrics
        );
        bufferCountSensor = StateStoreMetrics.suppressionBufferCountSensor(
            threadId,
            taskId,
            METRIC_SCOPE,
            storeName,
            streamsMetrics
        );

        context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        updateBufferMetrics();
        open = true;
        partition = context.taskId().partition;
        this.memoryBudget = this.context.getMemoryBudget();
    }

    @Override
    public void setMemoryBudget(final MemoryBudget memoryBudget, final MemoryBudget.AllocationType allocationType) {
        this.memoryBudget = memoryBudget;
        this.memoryBudgetAllocationType = allocationType;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
        index.clear();
        sortedMap.clear();
        dirtyKeys.clear();
        memoryBudget.free(memoryBudgetAllocationType, memBufferSize);
        memBufferSize = 0;
        minTimestamp = Long.MAX_VALUE;
        updateBufferMetrics();
        streamsMetrics.removeAllStoreLevelSensors(threadId, taskId, storeName);
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
            VALUE_SERIALIZER
        );
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
            VALUE_SERIALIZER
        );
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
                        final long recordSize = computeRecordSize(bufferKey.key(), removed);
                        memoryBudget.free(memoryBudgetAllocationType, recordSize);
                        memBufferSize -= recordSize;
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
                           final Consumer<Eviction<K, V>> callback) {
        final Iterator<Map.Entry<BufferKey, BufferValue>> delegate = sortedMap.entrySet().iterator();
        int evictions = 0;

        if (predicate.get()) {
            Map.Entry<BufferKey, BufferValue> next = null;
            if (delegate.hasNext()) {
                next = delegate.next();
            }

            // predicate being true means we read one record, call the callback, and then remove it
            while (next != null && predicate.get()) {
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
                callback.accept(new Eviction<>(key, value, bufferValue.context()));

                delegate.remove();
                index.remove(next.getKey().key());

                dirtyKeys.add(next.getKey().key());

                final long recordSize = computeRecordSize(next.getKey().key(), bufferValue);
                memoryBudget.free(memoryBudgetAllocationType, recordSize);
                memBufferSize -= recordSize;

                // peek at the next record so we can update the minTimestamp
                if (delegate.hasNext()) {
                    next = delegate.next();
                    minTimestamp = next == null ? Long.MAX_VALUE : next.getKey().time();
                } else {
                    next = null;
                    minTimestamp = Long.MAX_VALUE;
                }

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
    public void put(final long time,
                    final K key,
                    final Change<V> value,
                    final ProcessorRecordContext recordContext) {
        requireNonNull(value, "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");

        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, key));
        final Change<byte[]> serialChange = valueSerde.serializeParts(changelogTopic, value);

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
        dirtyKeys.add(serializedKey);
        updateBufferMetrics();
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
        long recordSizeDelta = 0;
        if (previousKey == null) {
            final BufferKey nextKey = new BufferKey(time, key);
            index.put(key, nextKey);
            sortedMap.put(nextKey, value);
            minTimestamp = Math.min(minTimestamp, time);
            recordSizeDelta = computeRecordSize(key, value);
        } else {
            final BufferValue removedValue = sortedMap.put(previousKey, value);
            recordSizeDelta =
                    computeRecordSize(key, value)
                    - (removedValue == null ? 0 : computeRecordSize(key, removedValue));
        }
        memBufferSize += recordSizeDelta;
        memoryBudget.transact(memoryBudgetAllocationType, recordSizeDelta);
    }

    @Override
    public int numRecords() {
        return index.size();
    }

    @Override
    public boolean nonEmpty() {
        return !index.isEmpty();
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
        return "InMemoryTimeOrderedKeyValueBuffer{" +
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
