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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.metrics.Sensors;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class InMemoryTimeOrderedKeyValueBuffer<K, V> implements TimeOrderedKeyValueBuffer<K, V> {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
    private static final RecordHeaders V_1_CHANGELOG_HEADERS =
        new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 1})});

    private final Map<Bytes, BufferKey> index = new HashMap<>();
    private final TreeMap<BufferKey, ContextualRecord> sortedMap = new TreeMap<>();

    private final Set<Bytes> dirtyKeys = new HashSet<>();
    private final String storeName;
    private final boolean loggingEnabled;

    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;
    private RecordCollector collector;
    private String changelogTopic;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;

    private volatile boolean open;

    private int partition;

    public static class Builder<K, V> implements StoreBuilder<StateStore> {

        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valSerde;
        private boolean loggingEnabled = true;

        public Builder(final String storeName, final Serde<K> keySerde, final Serde<V> valSerde) {
            this.storeName = storeName;
            this.keySerde = keySerde;
            this.valSerde = valSerde;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         *
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<StateStore> withCachingEnabled() {
            return this;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         *
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<StateStore> withCachingDisabled() {
            return this;
        }

        @Override
        public StoreBuilder<StateStore> withLoggingEnabled(final Map<String, String> config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoreBuilder<StateStore> withLoggingDisabled() {
            loggingEnabled = false;
            return this;
        }

        @Override
        public InMemoryTimeOrderedKeyValueBuffer<K, V> build() {
            return new InMemoryTimeOrderedKeyValueBuffer<>(storeName, loggingEnabled, keySerde, valSerde);
        }

        @Override
        public Map<String, String> logConfig() {
            return Collections.emptyMap();
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

    private static final class BufferKey implements Comparable<BufferKey> {
        private final long time;
        private final Bytes key;

        private BufferKey(final long time, final Bytes key) {
            this.time = time;
            this.key = key;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BufferKey bufferKey = (BufferKey) o;
            return time == bufferKey.time &&
                Objects.equals(key, bufferKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(time, key);
        }

        @Override
        public int compareTo(final BufferKey o) {
            // ordering of keys within a time uses hashCode.
            final int timeComparison = Long.compare(time, o.time);
            return timeComparison == 0 ? key.compareTo(o.key) : timeComparison;
        }

        @Override
        public String toString() {
            return "BufferKey{" +
                "key=" + key +
                ", time=" + time +
                '}';
        }
    }

    private InMemoryTimeOrderedKeyValueBuffer(final String storeName,
                                              final boolean loggingEnabled,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
        this.storeName = storeName;
        this.loggingEnabled = loggingEnabled;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
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
        this.valueSerde = this.valueSerde == null ? valueSerde : this.valueSerde;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        final InternalProcessorContext internalProcessorContext = (InternalProcessorContext) context;

        bufferSizeSensor = Sensors.createBufferSizeSensor(this, internalProcessorContext);
        bufferCountSensor = Sensors.createBufferCountSensor(this, internalProcessorContext);

        context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        if (loggingEnabled) {
            collector = ((RecordCollector.Supplier) context).recordCollector();
            changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        }
        updateBufferMetrics();
        open = true;
        partition = context.taskId().partition;
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
        memBufferSize = 0;
        minTimestamp = Long.MAX_VALUE;
        updateBufferMetrics();
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
                    final ContextualRecord value = sortedMap.get(bufferKey);

                    logValue(key, bufferKey, value);
                }
            }
            dirtyKeys.clear();
        }
    }

    private void logValue(final Bytes key, final BufferKey bufferKey, final ContextualRecord value) {
        final byte[] serializedContextualRecord = value.serialize();

        final int sizeOfBufferTime = Long.BYTES;
        final int sizeOfContextualRecord = serializedContextualRecord.length;

        final byte[] timeAndContextualRecord = ByteBuffer.wrap(new byte[sizeOfBufferTime + sizeOfContextualRecord])
                                                         .putLong(bufferKey.time)
                                                         .put(serializedContextualRecord)
                                                         .array();

        collector.send(
            changelogTopic,
            key,
            timeAndContextualRecord,
            V_1_CHANGELOG_HEADERS,
            partition,
            null,
            KEY_SERIALIZER,
            VALUE_SERIALIZER
        );
    }

    private void logTombstone(final Bytes key) {
        collector.send(changelogTopic,
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
            final Bytes key = Bytes.wrap(record.key());
            if (record.value() == null) {
                // This was a tombstone. Delete the record.
                final BufferKey bufferKey = index.remove(key);
                if (bufferKey != null) {
                    final ContextualRecord removed = sortedMap.remove(bufferKey);
                    if (removed != null) {
                        memBufferSize -= computeRecordSize(bufferKey.key, removed);
                    }
                    if (bufferKey.time == minTimestamp) {
                        minTimestamp = sortedMap.isEmpty() ? Long.MAX_VALUE : sortedMap.firstKey().time;
                    }
                }

                if (record.partition() != partition) {
                    throw new IllegalStateException(
                        String.format(
                            "record partition [%d] is being restored by the wrong suppress partition [%d]",
                            record.partition(),
                            partition
                        )
                    );
                }
            } else {
                final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
                final long time = timeAndValue.getLong();
                final byte[] value = new byte[record.value().length - 8];
                timeAndValue.get(value);
                if (record.headers().lastHeader("v") == null) {
                    cleanPut(
                        time,
                        key,
                        new ContextualRecord(
                            value,
                            new ProcessorRecordContext(
                                record.timestamp(),
                                record.offset(),
                                record.partition(),
                                record.topic(),
                                record.headers()
                            )
                        )
                    );
                } else if (V_1_CHANGELOG_HEADERS.lastHeader("v").equals(record.headers().lastHeader("v"))) {
                    final ContextualRecord contextualRecord = ContextualRecord.deserialize(ByteBuffer.wrap(value));

                    cleanPut(
                        time,
                        key,
                        contextualRecord
                    );
                } else {
                    throw new IllegalArgumentException("Restoring apparently invalid changelog record: " + record);
                }
            }
            if (record.partition() != partition) {
                throw new IllegalStateException(
                    String.format(
                        "record partition [%d] is being restored by the wrong suppress partition [%d]",
                        record.partition(),
                        partition
                    )
                );
            }
        }
        updateBufferMetrics();
    }


    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<Eviction<K, V>> callback) {
        final Iterator<Map.Entry<BufferKey, ContextualRecord>> delegate = sortedMap.entrySet().iterator();
        int evictions = 0;

        if (predicate.get()) {
            Map.Entry<BufferKey, ContextualRecord> next = null;
            if (delegate.hasNext()) {
                next = delegate.next();
            }

            // predicate being true means we read one record, call the callback, and then remove it
            while (next != null && predicate.get()) {
                if (next.getKey().time != minTimestamp) {
                    throw new IllegalStateException(
                        "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                            next.getKey().time + "]"
                    );
                }
                final K key = keySerde.deserializer().deserialize(changelogTopic, next.getKey().key.get());
                final V value = valueSerde.deserializer().deserialize(changelogTopic, next.getValue().value());
                callback.accept(new Eviction<>(key, value, next.getValue().recordContext()));

                delegate.remove();
                index.remove(next.getKey().key);

                dirtyKeys.add(next.getKey().key);

                memBufferSize -= computeRecordSize(next.getKey().key, next.getValue());

                // peek at the next record so we can update the minTimestamp
                if (delegate.hasNext()) {
                    next = delegate.next();
                    minTimestamp = next == null ? Long.MAX_VALUE : next.getKey().time;
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
    public void put(final long time,
                    final K key,
                    final V value,
                    final ProcessorRecordContext recordContext) {
        requireNonNull(value, "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");

        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, key));
        final byte[] serializedValue = valueSerde.serializer().serialize(changelogTopic, value);

        cleanPut(time, serializedKey, new ContextualRecord(serializedValue, recordContext));
        dirtyKeys.add(serializedKey);
        updateBufferMetrics();
    }

    private void cleanPut(final long time, final Bytes key, final ContextualRecord value) {
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
            final ContextualRecord removedValue = sortedMap.put(previousKey, value);
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

    private static long computeRecordSize(final Bytes key, final ContextualRecord value) {
        long size = 0L;
        size += 8; // buffer time
        size += key.get().length;
        if (value != null) {
            size += value.sizeBytes();
        }
        return size;
    }

    private void updateBufferMetrics() {
        bufferSizeSensor.record(memBufferSize);
        bufferCountSensor.record(index.size());
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
