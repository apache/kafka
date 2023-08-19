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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class RocksDBTimeOrderedKeyValueBuffer<K, V> extends WrappedStateStore<RocksDBTimeOrderedKeyValueBytesStore, Object, Object> implements TimeOrderedKeyValueBuffer<K, V, V> {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
    private final long gracePeriod;
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

    public RocksDBTimeOrderedKeyValueBuffer(final RocksDBTimeOrderedKeyValueBytesStore store,
                                            final Duration gracePeriod,
                                            final String topic,
                                            final boolean loggingEnabled) {
        super(store);
        this.gracePeriod = gracePeriod.toMillis();
        minTimestamp = Long.MAX_VALUE;
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

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        wrapped().init(context, wrapped());
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        wrapped().init(context, wrapped());
        this.context = ProcessorContextUtils.asInternalProcessorContext(context);
        partition = context.taskId().partition();
        if (loggingEnabled) {
            changelogTopic = ProcessorContextUtils.changelogFor(context, name(), Boolean.TRUE);
        }
    }

    @Override
    public void evictWhile(final Supplier<Boolean> predicate, final Consumer<Eviction<K, V>> callback) {
        KeyValue<Bytes, byte[]> keyValue;

        if (predicate.get()) {
            try (final KeyValueIterator<Bytes, byte[]> iterator = wrapped()
                .fetchAll(0, wrapped().observedStreamTime - gracePeriod)) {
                while (iterator.hasNext() && predicate.get()) {
                    keyValue = iterator.next();

                    final BufferValue bufferValue = BufferValue.deserialize(ByteBuffer.wrap(keyValue.value));
                    final K key = keySerde.deserializer().deserialize(topic,
                        PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.extractStoreKeyBytes(keyValue.key.get()));

                    if (bufferValue.context().timestamp() < minTimestamp) {
                        throw new IllegalStateException(
                            "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                                bufferValue.context().timestamp() + "]"
                        );
                    }
                    minTimestamp = bufferValue.context().timestamp();

                    final V value = valueSerde.deserializer().deserialize(topic, bufferValue.newValue());

                    callback.accept(new Eviction<>(key, value, bufferValue.context()));

                    wrapped().remove(keyValue.key);

                    if (loggingEnabled) {
                        logTombstone(keyValue.key);
                    }

                    numRecords--;
                    bufferSize = bufferSize - computeRecordSize(keyValue.key, bufferValue);
                }
                if (numRecords == 0) {
                    minTimestamp = Long.MAX_VALUE;
                } else {
                    minTimestamp = wrapped().observedStreamTime - gracePeriod + 1;
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
        if (wrapped().observedStreamTime - gracePeriod > record.timestamp()) {
            return false;
        }
        maybeUpdateSeqnumForDups();
        final Bytes serializedKey = Bytes.wrap(
            PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.toStoreKeyBinary(keySerde.serializer().serialize(topic, record.key()),
                record.timestamp(),
                seqnum).get());
        final byte[] valueBytes = valueSerde.serializer().serialize(topic, record.value());
        final BufferValue buffered = new BufferValue(null, null, valueBytes, recordContext);
        wrapped().put(serializedKey, buffered.serialize(0).array());

        if (loggingEnabled) {
            final BufferKey key = new BufferKey(0L, serializedKey);
            logValue(serializedKey, key, buffered);
        }

        bufferSize += computeRecordSize(serializedKey, buffered);
        numRecords++;
        if (minTimestamp() > record.timestamp()) {
            minTimestamp = record.timestamp();
        }
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