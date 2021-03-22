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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class MeteredTimeOrderedKeyValueBuffer<K, V> implements TimeOrderedKeyValueBuffer<K, V> {
    private final InMemoryTimeOrderedKeyValueBuffer wrapped;

    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private FullChangeSerde<V> valueChangeSerde;

    private final String metricsScope;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;

    private InternalProcessorContext context;
    private StreamsMetricsImpl streamsMetrics;
    private String threadId;
    private String taskId;

    private String changelogTopic;

    MeteredTimeOrderedKeyValueBuffer(final InMemoryTimeOrderedKeyValueBuffer inner,
                                     final String metricsScope,
                                     final Serde<K> keySerde,
                                     final Serde<V> valueSerde) {
        this.wrapped = Objects.requireNonNull(inner);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.valueChangeSerde = FullChangeSerde.wrap(valueSerde);

        this.metricsScope = metricsScope;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = ProcessorContextUtils.asInternalProcessorContext(context);
        init(root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        this.context = ProcessorContextUtils.asInternalProcessorContext(context);
        init(root);
    }

    private void init(final StateStore root) {
        streamsMetrics = context.metrics();
        threadId = Thread.currentThread().getName();
        taskId = context.taskId().toString();

        bufferSizeSensor = StateStoreMetrics.suppressionBufferSizeSensor(
            threadId,
            taskId,
            metricsScope,
            name(),
            streamsMetrics
        );
        bufferCountSensor = StateStoreMetrics.suppressionBufferCountSensor(
            threadId,
            taskId,
            metricsScope,
            name(),
            streamsMetrics
        );

        context.register(root, (RecordBatchingStateRestoreCallback) (final Collection<ConsumerRecord<byte[], byte[]>> records) -> {
            wrapped.restoreBatch(records);
            updateBufferMetrics();
        });
        changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());
        updateBufferMetrics();

        wrapped.init((StateStoreContext) context, root);
    }

    @Override
    public String name() {
        return wrapped.name();
    }

    @Override
    public boolean persistent() {
        return wrapped.persistent();
    }

    @Override
    public boolean isOpen() {
        return wrapped.isOpen();
    }

    @Override
    public void flush() {
        wrapped.flush();
    }

    @Override
    public void close() {
        wrapped.close();

        updateBufferMetrics();
        streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId, name());
    }

    @Override
    public void setSerdesIfNull(final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.keySerde = this.keySerde == null ? keySerde : this.keySerde;
        this.valueChangeSerde = this.valueChangeSerde == null ? FullChangeSerde.wrap(valueSerde) : this.valueChangeSerde;
    }

    @Override
    public void put(final long time,
                    final K key,
                    final Change<V> value,
                    final ProcessorRecordContext recordContext) {
        requireNonNull(value, "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");

        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, key));
        final Change<byte[]> serializedChange = valueChangeSerde.serializeParts(changelogTopic, value);

        wrapped.put(time, serializedKey, serializedChange, recordContext);

        updateBufferMetrics();
    }

    @Override
    public Maybe<ValueAndTimestamp<V>> priorValueForBuffered(final K key) {
        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, key));

        final Maybe<ValueAndTimestamp<byte[]>> maybe = wrapped.priorValueForBuffered(serializedKey);

        if (maybe.isDefined()) {
            final V deserializedValue = valueChangeSerde.innerSerde().deserializer().deserialize(
                changelogTopic,
                maybe.getNullableValue() == null ? null : maybe.getNullableValue().value()
            );

            // it's unfortunately not possible to know this, unless we materialize the suppressed result, since our only
            // knowledge of the prior value is what the upstream processor sends us as the "old value" when we first
            // buffer something.
            return Maybe.defined(ValueAndTimestamp.make(deserializedValue, RecordQueue.UNKNOWN));
        } else {
            return Maybe.undefined();
        }
    }

    @Override
    public int evictWhile(final Supplier<Boolean> predicate,
                          final Consumer<Eviction<K, V>> callback) {
        final int evictions = wrapped.evictWhile(predicate, (final Eviction<Bytes, byte[]> e) -> {
            final K key = keySerde.deserializer().deserialize(changelogTopic, e.key().get());
            final Change<V> value = valueChangeSerde.deserializeParts(changelogTopic, e.value());
            final Eviction<K, V> eviction = new Eviction<>(key, value, e.recordContext());
            callback.accept(eviction);
        });

        if (evictions > 0) {
            updateBufferMetrics();
        }

        return evictions;
    }

    @Override
    public int numRecords() {
        return wrapped.numRecords();
    }

    @Override
    public long bufferSize() {
        return wrapped.bufferSize();
    }

    @Override
    public long minTimestamp() {
        return wrapped.minTimestamp();
    }

    private void updateBufferMetrics() {
        bufferSizeSensor.record(bufferSize(), context.currentSystemTimeMs());
        bufferCountSensor.record(numRecords(), context.currentSystemTimeMs());
    }

    @Override
    public V get(final K key) {
        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, key));

        final byte[] serializedValue = wrapped.get(serializedKey);

        if (serializedValue != null) {
            return valueSerde.deserializer().deserialize(changelogTopic, serializedValue);
        } else {
            return null;
        }
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        return new MeteredTimeOrderedKeyValueBuffer<K, V>.MeteredKeyValueIterator(
            wrapped.range(Bytes.wrap(keySerde.serializer().serialize(changelogTopic, from)),
                Bytes.wrap(keySerde.serializer().serialize(changelogTopic, to)))
        );
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(final K from,
                                               final K to) {
        return new MeteredTimeOrderedKeyValueBuffer<K, V>.MeteredKeyValueIterator(
            wrapped.reverseRange(Bytes.wrap(keySerde.serializer().serialize(changelogTopic, from)),
                Bytes.wrap(keySerde.serializer().serialize(changelogTopic, to)))
        );
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredTimeOrderedKeyValueBuffer<K, V>.MeteredKeyValueIterator(wrapped.all());
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return new MeteredTimeOrderedKeyValueBuffer<K, V>.MeteredKeyValueIterator(wrapped.reverseAll());
    }

    private class MeteredKeyValueIterator implements KeyValueIterator<K, V> {
        private final KeyValueIterator<Bytes, byte[]> iter;

        private MeteredKeyValueIterator(final KeyValueIterator<Bytes, byte[]> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                keySerde.deserializer().deserialize(changelogTopic, keyValue.key.get()),
                valueSerde.deserializer().deserialize(changelogTopic, keyValue.value));
        }

        @Override
        public void close() {
            iter.close();
        }

        @Override
        public K peekNextKey() {
            return keySerde.deserializer().deserialize(changelogTopic, iter.peekNextKey().get());
        }
    }
}
