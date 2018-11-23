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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StoreBuilder;

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

public class InMemoryTimeOrderedKeyValueBuffer implements TimeOrderedKeyValueBuffer {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();

    private final Map<Bytes, BufferKey> index = new HashMap<>();
    private final TreeMap<BufferKey, ContextualRecord> sortedMap = new TreeMap<>();

    private final Set<Bytes> dirtyKeys = new HashSet<>();
    private final String storeName;
    private final boolean loggingEnabled;

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;
    private RecordCollector collector;
    private String changelogTopic;

    private volatile boolean open;

    public static class Builder implements StoreBuilder<StateStore> {

        private final String storeName;
        private boolean loggingEnabled = true;

        public Builder(final String storeName) {
            this.storeName = storeName;
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
            return new InMemoryTimeOrderedKeyValueBuffer(storeName, loggingEnabled);
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

    private static class BufferKey implements Comparable<BufferKey> {
        private final long time;
        private final Bytes key;

        private BufferKey(final long time, final Bytes key) {
            this.time = time;
            this.key = key;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
    }

    private InMemoryTimeOrderedKeyValueBuffer(final String storeName, final boolean loggingEnabled) {
        this.storeName = storeName;
        this.loggingEnabled = loggingEnabled;
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
        context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        if (loggingEnabled) {
            collector = ((RecordCollector.Supplier) context).recordCollector();
            changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        }
        open = true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        index.clear();
        sortedMap.clear();
        dirtyKeys.clear();
        memBufferSize = 0;
        minTimestamp = Long.MAX_VALUE;
        open = false;
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
                    sortedMap.remove(bufferKey);
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
    }


    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<KeyValue<Bytes, ContextualRecord>> callback) {
        final Iterator<Map.Entry<BufferKey, ContextualRecord>> delegate = sortedMap.entrySet().iterator();

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

                memBufferSize = memBufferSize - computeRecordSize(next.getKey().key, next.getValue());

                // peek at the next record so we can update the minTimestamp
                if (delegate.hasNext()) {
                    next = delegate.next();
                    minTimestamp = next == null ? Long.MAX_VALUE : next.getKey().time;
                } else {
                    next = null;
                    minTimestamp = Long.MAX_VALUE;
                }
            }
        }
    }

    @Override
    public void put(final long time,
                    final Bytes key,
                    final ContextualRecord value) {
        cleanPut(time, key, value);
        dirtyKeys.add(key);
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
            memBufferSize = memBufferSize + computeRecordSize(key, value);
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

    private long computeRecordSize(final Bytes key, final ContextualRecord value) {
        long size = 0L;
        size += 8; // buffer time
        size += key.get().length;
        if (value != null) {
            size += value.sizeBytes();
        }
        return size;
    }
}
