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

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class RocksDBTimeOrderedKeyValueBuffer<K, V> implements TimeOrderedKeyValueBuffer<K, V, V> {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
    private final long gracePeriod;
    private final RocksDBTimeOrderedKeyValueBytesStore store;
    private long bufferSize;
    private long minTimestamp;
    private int numRecords;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private final String topic;
    private int seqnum;
    private final boolean loggingEnabled;
    private int partition;
    private String changelogTopic;
    private InternalProcessorContext context;
    private boolean minValid;

    public static class Builder<K, V> implements StoreBuilder<TimeOrderedKeyValueBuffer<K, V, V>> {

        private final String storeName;
        private boolean loggingEnabled = true;
        private Map<String, String> logConfig = new HashMap<>();
        private final Duration grace;
        private final String topic;

        public Builder(final String storeName, final Duration grace, final String topic) {
            this.storeName = storeName;
            this.grace = grace;
            this.topic = topic;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         * <p>
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<TimeOrderedKeyValueBuffer<K, V, V>> withCachingEnabled() {
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
        public StoreBuilder<TimeOrderedKeyValueBuffer<K, V, V>> withCachingDisabled() {
            return this;
        }

        @Override
        public StoreBuilder<TimeOrderedKeyValueBuffer<K, V, V>> withLoggingEnabled(final Map<String, String> config) {
            logConfig = config;
            return this;
        }

        @Override
        public StoreBuilder<TimeOrderedKeyValueBuffer<K, V, V>> withLoggingDisabled() {
            loggingEnabled = false;
            return this;
        }

        @Override
        public TimeOrderedKeyValueBuffer<K, V, V> build() {
            return new RocksDBTimeOrderedKeyValueBuffer<>(
                new RocksDBTimeOrderedKeyValueBytesStoreSupplier(storeName).get(),
                grace,
                topic,
                loggingEnabled);
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


    public RocksDBTimeOrderedKeyValueBuffer(final RocksDBTimeOrderedKeyValueBytesStore store,
                                            final Duration gracePeriod,
                                            final String topic,
                                            final boolean loggingEnabled) {
        this.store = store;
        this.gracePeriod = gracePeriod.toMillis();
        minTimestamp = store.minTimestamp();
        minValid = false;
        numRecords = 0;
        bufferSize = 0;
        seqnum = 0;
        this.topic = topic;
        this.loggingEnabled = loggingEnabled;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setSerdesIfNull(final SerdeGetter getter) {
        keySerde = keySerde == null ? (Serde<K>) getter.keySerde() : keySerde;
        valueSerde = valueSerde == null ? getter.valueSerde() : valueSerde;
    }

    private long observedStreamTime() {
        return store.observedStreamTime;
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        store.init(context, root);
        this.context = ProcessorContextUtils.asInternalProcessorContext(context);
        partition = context.taskId().partition();
        if (loggingEnabled) {
            changelogTopic = ProcessorContextUtils.changelogFor(context, name(), Boolean.TRUE);
        }
    }

    @Override
    public void flush() {
        store.flush();
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return store.persistent();
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public void evictWhile(final Supplier<Boolean> predicate, final Consumer<Eviction<K, V>> callback) {
        KeyValue<Bytes, byte[]> keyValue;

        if (predicate.get()) {
            long start = 0;
            if (minValid) {
                start = minTimestamp();
            }
            try (final KeyValueIterator<Bytes, byte[]> iterator = store
                .fetchAll(start, observedStreamTime() - gracePeriod)) {
                while (iterator.hasNext() && predicate.get()) {
                    keyValue = iterator.next();

                    final BufferValue bufferValue = BufferValue.deserialize(ByteBuffer.wrap(keyValue.value));
                    final K key = keySerde.deserializer().deserialize(topic,
                        PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.extractStoreKeyBytes(keyValue.key.get()));

                    if (bufferValue.context().timestamp() < minTimestamp && minValid) {
                        throw new IllegalStateException(
                            "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                                bufferValue.context().timestamp() + "]"
                        );
                    }
                    minTimestamp = bufferValue.context().timestamp();
                    minValid = true;

                    final V value = valueSerde.deserializer().deserialize(topic, bufferValue.newValue());

                    callback.accept(new Eviction<>(key, value, bufferValue.context()));

                    store.remove(keyValue.key);

                    if (loggingEnabled) {
                        logTombstone(keyValue.key);
                    }

                    numRecords--;
                    bufferSize = bufferSize - computeRecordSize(keyValue.key, bufferValue);
                }
                if (numRecords == 0) {
                    minTimestamp = Long.MAX_VALUE;
                } else {
                    minTimestamp = observedStreamTime() - gracePeriod + 1;
                }
            }
        }
    }


    @Override
    public Maybe<ValueAndTimestamp<V>> priorValueForBuffered(final K key) {
        return Maybe.undefined();
    }

    @Override
    public boolean put(final long time, final Record<K, V> record, final ProcessorRecordContext recordContext) {
        requireNonNull(record.value(), "value cannot be null");
        requireNonNull(record.key(), "key cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");
        if (observedStreamTime() - gracePeriod > record.timestamp()) {
            return false;
        }
        maybeUpdateSeqnumForDups();
        final Bytes serializedKey = Bytes.wrap(
            PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.toStoreKeyBinary(keySerde.serializer().serialize(topic, record.key()),
                record.timestamp(),
                seqnum).get());
        final byte[] valueBytes = valueSerde.serializer().serialize(topic, record.value());
        final BufferValue buffered = new BufferValue(null, null, valueBytes, recordContext);
        store.put(serializedKey, buffered.serialize(0).array());

        if (loggingEnabled) {
            final BufferKey key = new BufferKey(0L, serializedKey);
            logValue(serializedKey, key, buffered);
        }

        bufferSize += computeRecordSize(serializedKey, buffered);
        numRecords++;
        minTimestamp = Math.min(minTimestamp(), record.timestamp());
        return true;
    }

    @Override
    public int numRecords() {
        return numRecords;
    }

    @Override
    public long bufferSize() {
        return bufferSize;
    }

    @Override
    public long minTimestamp() {
        return minTimestamp;
    }

    private static long computeRecordSize(final Bytes key, final BufferValue value) {
        long size = 0L;
        size += key.get().length;
        if (value != null) {
            size += value.residentMemorySizeEstimate();
        }
        return size;
    }

    private void maybeUpdateSeqnumForDups() {
        seqnum = (seqnum + 1) & 0x7FFFFFFF;
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
            null,
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

}