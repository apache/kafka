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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.DeserializationResult;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV0;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV1;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV3;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.duckTypeV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryTimeOrderedKeyValueBuffer implements TimeOrderedKeyValueBuffer<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimeOrderedKeyValueBuffer.class);

    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
    private static final byte[] V_1_CHANGELOG_HEADER_VALUE = {(byte) 1};
    private static final byte[] V_2_CHANGELOG_HEADER_VALUE = {(byte) 2};
    private static final byte[] V_3_CHANGELOG_HEADER_VALUE = {(byte) 3};
    static final RecordHeaders CHANGELOG_HEADERS =
        new RecordHeaders(new Header[] {new RecordHeader("v", V_3_CHANGELOG_HEADER_VALUE)});

    // Map of serialized key to (serialized key, timestamp), ordered by serialized key.
    private final NavigableMap<Bytes, BufferKey> index = new TreeMap<>();

    // Map of (serialized key, timestamp) to buffer value, ordered by (timestamp, serialized key).
    private final SortedMap<BufferKey, BufferValue> sortedMap = new TreeMap<>();

    private final Set<Bytes> dirtyKeys = new HashSet<>();

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;

    private final String storeName;
    private final boolean loggingEnabled;

    private RecordCollector recordCollector;
    private String changelogTopic;

    private volatile boolean open;
    private int partition;

    public InMemoryTimeOrderedKeyValueBuffer(final String storeName,
                                             final boolean loggingEnabled) {
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

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        init(ProcessorContextUtils.asInternalProcessorContext(context));
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        init(ProcessorContextUtils.asInternalProcessorContext(context));
    }

    private void init(final InternalProcessorContext context) {
        this.recordCollector = ((RecordCollector.Supplier) context).recordCollector();
        this.changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);

        this.open = true;
        this.partition = context.taskId().partition;
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
        recordCollector.send(
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
        recordCollector.send(
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

    public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> batch) {
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
    }

    @Override
    public void setSerdesIfNull(final Serde<Bytes> keySerde, final Serde<byte[]> valueSerde) {
        throw new UnsupportedOperationException("setSerdesIfNull() not supported in " + getClass().getName());
    }

    @Override
    public void put(final long time,
                    final Bytes key,
                    final Change<byte[]> value,
                    final ProcessorRecordContext recordContext) {
        requireNonNull(value, "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");

        final BufferValue buffered = getBuffered(key);
        final byte[] serializedPriorValue;
        if (buffered == null) {
            serializedPriorValue = value.oldValue;
        } else {
            serializedPriorValue = buffered.priorValue();
        }

        cleanPut(
            time,
            key,
            new BufferValue(serializedPriorValue, value.oldValue, value.newValue, recordContext)
        );
        dirtyKeys.add(key);
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
    public Maybe<ValueAndTimestamp<byte[]>> priorValueForBuffered(final Bytes key) {
        if (index.containsKey(key)) {
            final byte[] serializedValue = internalPriorValueForBuffered(key);
            // it's unfortunately not possible to know this, unless we materialize the suppressed result, since our only
            // knowledge of the prior value is what the upstream processor sends us as the "old value" when we first
            // buffer something.
            return Maybe.defined(ValueAndTimestamp.make(serializedValue, RecordQueue.UNKNOWN));
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
    public int evictWhile(final Supplier<Boolean> predicate,
                          final Consumer<Eviction<Bytes, byte[]>> callback) {
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

                final Bytes key = next.getKey().key();
                final BufferValue value = next.getValue();
                callback.accept(new Eviction<>(key, new Change<>(value.newValue(), value.oldValue()), value.context()));

                delegate.remove();

                index.remove(next.getKey().key());
                dirtyKeys.add(next.getKey().key());
                memBufferSize -= computeRecordSize(key, value);

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

        return evictions;
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

    @Override
    public byte[] get(final Bytes key) {
        final BufferKey bufferKey = index.get(key);
        if (bufferKey != null) {
            final BufferValue bufferValue = sortedMap.get(bufferKey);

            if (bufferValue != null) {
                return bufferValue.priorValue();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return range(from, to, true);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        return range(from, to, false);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final boolean forward) {
        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new InMemoryTimeOrderedKeyValueBuffer.InMemoryKeyValueIterator(index.subMap(from, true, to, true).keySet(), forward));
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new InMemoryTimeOrderedKeyValueBuffer.InMemoryKeyValueIterator(index.keySet(), true));
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseAll() {
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new InMemoryTimeOrderedKeyValueBuffer.InMemoryKeyValueIterator(index.keySet(), false));
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

    private class InMemoryKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> iter;

        private InMemoryKeyValueIterator(final Set<Bytes> keySet, final boolean forward) {
            if (forward) {
                this.iter = new TreeSet<>(keySet).iterator();
            } else {
                this.iter = new TreeSet<>(keySet).descendingIterator();
            }
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final Bytes key = iter.next();
            return new KeyValue<>(key, get(key));
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }
}
