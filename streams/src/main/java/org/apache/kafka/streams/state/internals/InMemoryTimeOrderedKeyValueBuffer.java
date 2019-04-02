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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class InMemoryTimeOrderedKeyValueBuffer implements TimeOrderedKeyValueBuffer {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();

    private final Map<Bytes, BufferKey> index = new HashMap<>();
    private final TreeMap<BufferKey, ContextualRecord> sortedMap = new TreeMap<>();

    //    private final Set<Bytes> dirtyKeys = new HashSet<>();
    private final String storeName;
    private final boolean loggingEnabled;
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final Deserializer<Windowed<String>> windowedDeserializer = WindowedSerdes.timeWindowedSerdeFrom(String.class).deserializer();

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;
    private RecordCollector collector;
    private String changelogTopic;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;

    private volatile boolean open;

    private int partition;

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
                "time=" + time +
                ", key=" + Arrays.toString(key.get()) +
                '}';
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
//        dirtyKeys.clear();
        memBufferSize = 0;
        minTimestamp = Long.MAX_VALUE;
        updateBufferMetrics();
    }

    @Override
    public void flush() {
//        if (loggingEnabled) {
//            int count = 0;
//            // counting on this getting called before the record collector's flush
//            for (final Bytes key : dirtyKeys) {
//
//                final BufferKey bufferKey = index.get(key);
//
//                if (bufferKey == null) {
//                    // The record was evicted from the buffer. Send a tombstone.
//                    logTombstone(key);
//                } else {
//                    final ContextualRecord value = sortedMap.get(bufferKey);
//
//                    logValue(key, bufferKey, value);
//                }
//                count++;
//            }
//            dirtyKeys.clear();
//            System.out.println("Flushed " + count);
//        }
    }

    private void logValue(final Bytes key, final BufferKey bufferKey, final ContextualRecord value) {
        final byte[] serialize = value.serialize();

        final byte[] timeAndValue = ByteBuffer.wrap(new byte[8 + serialize.length])
                                              .putLong(bufferKey.time)
                                              .put(serialize)
                                              .array();

        final ProcessorRecordContext recordContext = value.recordContext();
        recordContext.headers().add("changelog-provenance-thread", new StringSerializer().serialize(null, Thread.currentThread().getName()));
        recordContext.headers().add("changelog-provenance-offset", new LongSerializer().serialize(null, recordContext.offset()));
        recordContext.headers().add("changelog-provenance-topic", new StringSerializer().serialize(null, recordContext.topic()));
        recordContext.headers().add("changelog-provenance-partition", new IntegerSerializer().serialize(null, recordContext.partition()));

        System.out.println("FLUSH: " + windowedDeserializer.deserialize(null, key.get()) +
                               " aka " + Arrays.toString(key.get()) +
                               " VALUE");
        if (recordContext.partition() != partition) {
            throw new IllegalStateException(String.format("rp[%d] p[%d]", recordContext.partition(), partition));
        }

        collector.send(
            changelogTopic,
            key,
            timeAndValue,
            recordContext.headers(),
            partition,
            recordContext.timestamp(),
            KEY_SERIALIZER,
            VALUE_SERIALIZER
        );
    }

    private void logTombstone(final Bytes key) {
        System.out.println("FLUSH: " + windowedDeserializer.deserialize(null, key.get()) +
                               " aka " + Arrays.toString(key.get()) +
                               " TOMBSTONE");
        collector.send(changelogTopic, key, null, null, partition, null, KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    private void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> batch) {
        long offset = -1;
        for (final ConsumerRecord<byte[], byte[]> record : batch) {
            System.out.println("RESTORING " + record);
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
                        final BufferKey leastKey = sortedMap.firstKey();
                        if (leastKey != null) {
                            minTimestamp = leastKey.time;
                        } else {
                            minTimestamp = Long.MAX_VALUE;
                        }
                    }
                }
                System.out.println("RESTORED TOMBSTONE OF " + windowedDeserializer.deserialize(null, key.get()) + " tpo:" + record.topic() + "," + record.partition() + "," + record.offset());
                if (record.partition() != partition) {
                    throw new IllegalStateException(String.format("rp[%d] p[%d]", record.partition(), partition));
                }
            } else {
                final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
                final long time = timeAndValue.getLong();
                final byte[] value = new byte[record.value().length - 8];
                timeAndValue.get(value);
                final ContextualRecord contextualRecord = ContextualRecord.deserialize(ByteBuffer.wrap(value));

                final ProcessorRecordContext recordContext = contextualRecord.recordContext();
                recordContext.headers().add("changelog-restore-provenance-thread", new StringSerializer().serialize(null, Thread.currentThread().getName()));
                recordContext.headers().add("changelog-restore-provenance-offset", new LongSerializer().serialize(null, record.offset()));
                recordContext.headers().add("changelog-restore-provenance-topic", new StringSerializer().serialize(null, record.topic()));
                recordContext.headers().add("changelog-restore-provenance-partition", new IntegerSerializer().serialize(null, record.partition()));
                recordContext.headers().add("changelog-restore-provenance-buffer-time", new LongSerializer().serialize(null, time));

                cleanPut(
                    time,
                    key,
                    contextualRecord
                );
                System.out.println("RESTORED VALUE OF " + windowedDeserializer.deserialize(null, key.get()) + " tpo:" + record.topic() + "," + record.partition() + "," + record.offset() + " " + contextualRecord);
            }
            offset = Math.max(offset, record.offset());
            if (record.partition() != partition) {
                throw new IllegalStateException(String.format("rp[%d] p[%d]", record.partition(), partition));
            }
        }
        System.out.println("RESTORED up to " + offset);
        updateBufferMetrics();
    }


    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<KeyValue<Bytes, ContextualRecord>> callback) {
        final Iterator<Map.Entry<BufferKey, ContextualRecord>> delegate = sortedMap.entrySet().iterator();
        int evictions = 0;

        if (predicate.get()) {
            Map.Entry<BufferKey, ContextualRecord> next = null;
            if (delegate.hasNext()) {
                next = delegate.next();
            }

            // predicate being true means we read one record, call the callback, and then remove it
            while (next != null && predicate.get()) {
                System.out.println("BUFF IT " + next.getKey() + " : " + next.getValue());
                if (next.getKey().time != minTimestamp) {
                    throw new IllegalStateException("minTimestamp[" + minTimestamp + "] did not match the actual min timestamp [" + next.getKey().time + "]");
                }
                callback.accept(new KeyValue<>(next.getKey().key, next.getValue()));

                delegate.remove();
                index.remove(next.getKey().key);

//                dirtyKeys.add(next.getKey().key);
                logTombstone(next.getKey().key);

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
                    final Bytes key,
                    final ContextualRecord contextualRecord) {
        if (contextualRecord.value() == null) {
            throw new IllegalArgumentException("value cannot be null");
        } else if (contextualRecord.recordContext() == null) {
            throw new IllegalArgumentException("recordContext cannot be null");
        }
        final BufferKey bufferKey = cleanPut(time, key, contextualRecord);
//        dirtyKeys.add(key);
        logValue(key, bufferKey, contextualRecord);
        updateBufferMetrics();
    }

    private BufferKey cleanPut(final long time, final Bytes key, final ContextualRecord value) {
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
            System.out.println("cleanPut " + windowedDeserializer.deserialize(null, key.get()) + " at " + time + " used " + nextKey);
            return nextKey;
        } else {
            final ContextualRecord removedValue = sortedMap.put(previousKey, value);
            memBufferSize =
                memBufferSize
                    + computeRecordSize(key, value)
                    - (removedValue == null ? 0 : computeRecordSize(key, removedValue));
            System.out.println("cleanPut " + windowedDeserializer.deserialize(null, key.get()) + " at " + time + " re-used " + previousKey);
            return previousKey;
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
