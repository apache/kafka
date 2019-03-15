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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.suppress.BufferConfigInternal;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.metrics.Sensors;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.nio.ByteBuffer.allocate;

public final class RocksDBTimeOrderedKeyValueBuffer implements TimeOrderedKeyValueBuffer {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();

    private final Set<Bytes> dirtyKeys = new HashSet<>();
    private final String storeName;
    private final boolean loggingEnabled;
    private final RocksDBStore bytesStore;
    private final IndexFacade index;
    private final StorageFacade sortedMap;

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;
    private RecordCollector collector;
    private String changelogTopic;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;

    private volatile boolean open;

    private static final class IndexFacade {
        private static final byte INDEX_FLAGS = 0;
        private final KeyValueStore<Bytes, byte[]> bytesStore;
        private int entries = 0;

        private static Bytes packKey(final Bytes key) {
            final byte[] keyBytes = key.get();
            return Bytes.wrap(allocate(keyBytes.length + 1).put(INDEX_FLAGS).put(keyBytes).array());
        }

        private static BufferKey nullSafeDeserialize(final byte[] bufferKey) {
            if (bufferKey == null) {
                return null;
            } else {
                return BufferKey.deserialize(ByteBuffer.wrap(bufferKey));
            }
        }

        private IndexFacade(final KeyValueStore<Bytes, byte[]> bytesStore) {
            this.bytesStore = bytesStore;
        }

        public BufferKey get(final Bytes key) {
            return nullSafeDeserialize(bytesStore.get(packKey(key)));
        }

        public BufferKey remove(final Bytes key) {
            entries--;
            return nullSafeDeserialize(bytesStore.delete(packKey(key)));
        }

        /**
         * Index an entry **that is not already indexed**
         */
        public void put(final Bytes key, final BufferKey bufferKey) {
            bytesStore.put(packKey(key), bufferKey.serialize());
            entries++;
        }

        public int size() {
            return entries;
        }
    }

    private static final class StorageFacade {
        private static final byte STORAGE_FLAGS = 1;
        // concrete because we need to guarantee iteration in lexicographic order
        private final RocksDBStore bytesStore;

        private static Bytes packKey(final BufferKey key) {
            final byte[] keyBytes = key.serialize();
            return Bytes.wrap(allocate(keyBytes.length + 1).put(STORAGE_FLAGS).put(keyBytes).array());
        }

        private static Bytes unpackKey(final Bytes key) {
            final byte[] keyBytes = key.get();
            return Bytes.wrap(allocate(keyBytes.length - 1).put(keyBytes, 1, keyBytes.length - 1).array());
        }

        private static final class StorageFacadeIterator implements CloseableIterator<Map.Entry<BufferKey, ContextualRecord>> {
            private final KeyValueIterator<Bytes, byte[]> delegate;
            private final StorageFacade storageFacade;
            private BufferKey last = null;

            private StorageFacadeIterator(final KeyValueIterator<Bytes, byte[]> delegate, final StorageFacade storageFacade) {
                this.delegate = delegate;
                this.storageFacade = storageFacade;
            }

            @Override
            public void close() {
                delegate.close();
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Map.Entry<BufferKey, ContextualRecord> next() {
                final KeyValue<Bytes, byte[]> next = delegate.next();
                final BufferKey bufferKey = BufferKey.deserialize(ByteBuffer.wrap(unpackKey(next.key).get()));
                last = bufferKey;
                return new Utils.MapEntry<>(
                    bufferKey,
                    ContextualRecord.deserialize(ByteBuffer.wrap(next.value))
                );
            }

            @Override
            public void remove() {
                if (last == null) {
                    throw new IllegalStateException();
                } else {
                    storageFacade.remove(last);
                    last = null;
                }
            }
        }


        private StorageFacade(final RocksDBStore bytesStore) {
            this.bytesStore = bytesStore;
        }

        public ContextualRecord get(final BufferKey bufferKey) {
            return ContextualRecord.deserialize(ByteBuffer.wrap(bytesStore.get(packKey(bufferKey))));
        }

        public ContextualRecord remove(final BufferKey bufferKey) {
            final byte[] bytes = bytesStore.delete(packKey(bufferKey));
            if (bytes == null) {
                return null;
            } else {
                return ContextualRecord.deserialize(ByteBuffer.wrap(bytes));
            }
        }

        public CloseableIterator<Map.Entry<BufferKey, ContextualRecord>> iterator() {
            return new StorageFacadeIterator(
                bytesStore.range(
                    Bytes.wrap(new byte[] {STORAGE_FLAGS}),
                    Bytes.wrap(new byte[] {STORAGE_FLAGS + 1})
                ),
                this
            );
        }

        public void put(final BufferKey nextKey, final ContextualRecord value) {
            bytesStore.put(packKey(nextKey), value.serialize());
        }
    }

    public static class Builder implements StoreBuilder<StateStore> {

        private final String storeName;
        private final long memLimit;
        private boolean loggingEnabled = true;

        public Builder(final String storeName, final BufferConfigInternal<?> bufferConfigInternal) {
            this.storeName = storeName;
            memLimit = bufferConfigInternal.maxBytes();
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
        public StateStore build() {
            return new RocksDBTimeOrderedKeyValueBuffer(storeName, loggingEnabled, memLimit);
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

    private static final class BufferKey {
        private final long time;
        private final Bytes key;

        private BufferKey(final long time, final Bytes key) {
            this.time = time;
            this.key = key;
        }


        private byte[] serialize() {
            final byte[] keyBytes = key.get();
            return allocate(Long.BYTES + Integer.BYTES + keyBytes.length)
                .putLong(time)
                .putInt(keyBytes.length)
                .put(keyBytes)
                .array();
        }

        private static BufferKey deserialize(final ByteBuffer buffer) {
            final long time = buffer.getLong();
            final int keyLength = buffer.getInt();
            final byte[] key = new byte[keyLength];
            buffer.get(key);
            return new BufferKey(time, Bytes.wrap(key));
        }
    }

    private RocksDBTimeOrderedKeyValueBuffer(final String storeName, final boolean loggingEnabled, final long memLimit) {
        this.storeName = storeName;
        this.loggingEnabled = loggingEnabled;
        bytesStore = new RocksDBStore(storeName, "rocksdb-buffer", memLimit);
        index = new IndexFacade(bytesStore);
        sortedMap = new StorageFacade(bytesStore);
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
    public void init(final ProcessorContext context, final StateStore root) {
        final InternalProcessorContext internalProcessorContext = (InternalProcessorContext) context;
        bufferSizeSensor = Sensors.createBufferSizeSensor(this, internalProcessorContext);
        bufferCountSensor = Sensors.createBufferCountSensor(this, internalProcessorContext);


        // TODO: restore. For now, just delete the directory and re-create it.
        try {
            Utils.delete(new File(new File(context.stateDir(), "rocksdb-buffer"), bytesStore.name()));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        bytesStore.openDB(context);

        context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        if (loggingEnabled) {
            collector = ((RecordCollector.Supplier) context).recordCollector();
            changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        }
        updateBufferMetrics();
        open = true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
        bytesStore.flush();
        bytesStore.close();
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
                    collector.send(changelogTopic, key, null, null, null, null, KEY_SERIALIZER, VALUE_SERIALIZER);
                } else {
                    final ContextualRecord value = sortedMap.get(bufferKey);

                    final byte[] innerValue = value.value();
                    final byte[] timeAndValue = ByteBuffer.wrap(new byte[8 + innerValue.length])
                                                          .putLong(bufferKey.time)
                                                          .put(innerValue)
                                                          .array();

                    final ProcessorRecordContext recordContext = value.recordContext();
                    collector.send(
                        changelogTopic,
                        key,
                        timeAndValue,
                        recordContext.headers(),
                        recordContext.partition(),
                        recordContext.timestamp(),
                        KEY_SERIALIZER,
                        VALUE_SERIALIZER
                    );
                }
            }
            dirtyKeys.clear();
        }
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
                }
            } else {
                final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
                final long time = timeAndValue.getLong();
                final byte[] value = new byte[record.value().length - 8];
                timeAndValue.get(value);

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
            }
        }
        updateBufferMetrics();
    }


    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<KeyValue<Bytes, ContextualRecord>> callback) {
        try (final CloseableIterator<Map.Entry<BufferKey, ContextualRecord>> delegate = sortedMap.iterator()) {
            int evictions = 0;

            if (predicate.get()) {
                Map.Entry<BufferKey, ContextualRecord> next = null;
                if (delegate.hasNext()) {
                    next = delegate.next();
                }

                // predicate being true means we read one record, call the callback, and then remove it
                while (next != null && predicate.get()) {
                    callback.accept(new KeyValue<>(next.getKey().key, next.getValue()));

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
    }

    @Override
    public void put(final long time,
                    final Bytes key,
                    final ContextualRecord contextualRecord) {
        if (contextualRecord.value() == null) {
            throw new IllegalArgumentException("value cannot be null");
        } else if (contextualRecord.recordContext() == null) {
            throw new IllegalArgumentException("recordContext cannot be null");
        }
        cleanPut(time, key, contextualRecord);
        dirtyKeys.add(key);
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
            final ContextualRecord removedValue = sortedMap.get(previousKey);
            sortedMap.put(previousKey, value);
            memBufferSize = memBufferSize + computeRecordSize(key, value) - computeRecordSize(key, removedValue);
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
}
