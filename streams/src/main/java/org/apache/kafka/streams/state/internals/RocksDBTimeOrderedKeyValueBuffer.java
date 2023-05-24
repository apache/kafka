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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class RocksDBTimeOrderedKeyValueBuffer<K, V> extends WrappedStateStore<RocksDBTimeOrderedKeyValueSegmentedBytesStore, Object, Object> implements TimeOrderedKeyValueBuffer<K, V> {

    private final Duration gracePeriod;
    private long bufferSize;
    private long minTimestamp;
    private int numRec;
    private Serde<K> keySerde;
    private FullChangeSerde<V> valueSerde;
    private String topic;

    public RocksDBTimeOrderedKeyValueBuffer(final RocksDBTimeOrderedKeyValueSegmentedBytesStore store,
                                            final Duration gracePeriod,
                                            final String topic) {
        super(store);
        this.gracePeriod = gracePeriod;
        minTimestamp = Long.MAX_VALUE;
        numRec = 0;
        bufferSize = 0;
        this.topic = topic;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setSerdesIfNull(final SerdeGetter getter) {
        keySerde = keySerde == null ? (Serde<K>) getter.keySerde() : keySerde;
        valueSerde = valueSerde == null ? FullChangeSerde.wrap((Serde<V>) getter.valueSerde()) : valueSerde;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        wrapped().init(context, wrapped());
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        wrapped().init(context, wrapped());
    }

    @Override
    public void evictWhile(final Supplier<Boolean> predicate, final Consumer<Eviction<K, V>> callback) {
        KeyValue<Bytes, byte[]> keyValue = null;

        if (predicate.get()) {
            final KeyValueIterator<Bytes, byte[]> iterator = wrapped()
                .fetchAll(0, wrapped().observedStreamTime - gracePeriod.toMillis());
            if (iterator.hasNext()) {
                keyValue = iterator.next();
            }
            if (keyValue == null) {
                if (numRecords() == 0) {
                    minTimestamp = Long.MAX_VALUE;
                }
                return;
            }
            BufferValue bufferValue = BufferValue.deserialize(ByteBuffer.wrap(keyValue.value));
            K key = keySerde.deserializer().deserialize(topic,
                PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.extractStoreKeyBytes(keyValue.key.get()));

            Change<V> value = valueSerde.deserializeParts(
                topic,
                new Change<>(bufferValue.newValue(), bufferValue.oldValue())
            );
            while (keyValue != null && predicate.get() && wrapped().observedStreamTime - gracePeriod.toMillis() >= minTimestamp()) {
                if (bufferValue.context().timestamp() != minTimestamp) {
                    throw new IllegalStateException(
                        "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                            bufferValue.context().timestamp() + "]"
                    );
                }
                callback.accept(new Eviction<>(key, value, bufferValue.context()));
                wrapped().remove(keyValue.key);
                numRec--;
                bufferSize = bufferSize - computeRecordSize(keyValue.key, bufferValue);
                if (iterator.hasNext()) {
                    keyValue = iterator.next();
                    if (keyValue == null) {
                        minTimestamp = Long.MAX_VALUE;
                    } else {
                        bufferValue = BufferValue.deserialize(ByteBuffer.wrap(keyValue.value));
                        key = keySerde.deserializer().deserialize(topic,
                            PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.extractStoreKeyBytes(keyValue.key.get()));
                        value = valueSerde.deserializeParts(
                            topic,
                            new Change<>(bufferValue.newValue(), bufferValue.oldValue())
                        );
                        minTimestamp = bufferValue.context().timestamp();
                    }
                } else {
                    keyValue = null;
                    minTimestamp = Long.MAX_VALUE;
                }
            }
        }
    }


    @Override
    public Maybe<ValueAndTimestamp<V>> priorValueForBuffered(final K key) {
        return null;
    }

    @Override
    public void put(final long time, final Record<K, Change<V>> record, final ProcessorRecordContext recordContext) {
        requireNonNull(record.value(), "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");
        final Bytes serializedKey = Bytes.wrap(
            PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.toStoreKeyBinary(keySerde.serializer().serialize(topic, record.key()),
                record.timestamp(),
                0).get());
        final Change<byte[]> serialChange = valueSerde.serializeParts(topic, record.value());
        final BufferValue buffered = new BufferValue(serialChange.oldValue, serialChange.oldValue, serialChange.newValue, recordContext);
        if (wrapped().observedStreamTime - gracePeriod.toMillis() > record.timestamp()) {
            return;
        }
        wrapped().put(serializedKey, buffered.serialize(0).array());
        bufferSize += computeRecordSize(serializedKey, buffered);
        numRec++;
        if (minTimestamp() > record.timestamp()) {
            minTimestamp = record.timestamp();
        }
    }

    @Override
    public int numRecords() {
        return numRec;
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
        size += 8; // buffer time
        size += key.get().length;
        if (value != null) {
            size += value.residentMemorySizeEstimate();
        }
        return size;
    }

}