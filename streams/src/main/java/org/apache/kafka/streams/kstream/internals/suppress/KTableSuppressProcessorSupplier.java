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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.suppress.TimeDefinitions.TimeDefinition;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.Maybe;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;

import static java.util.Objects.requireNonNull;

public class KTableSuppressProcessorSupplier<K, V> implements
    KTableProcessorSupplier<K, V, K, V> {
    private final SuppressedInternal<K> suppress;
    private final String storeName;
    private final KTableImpl<K, ?, V> parentKTable;

    public KTableSuppressProcessorSupplier(final SuppressedInternal<K> suppress,
                                           final String storeName,
                                           final KTableImpl<K, ?, V> parentKTable) {
        this.suppress = suppress;
        this.storeName = storeName;
        this.parentKTable = parentKTable;
        // The suppress buffer requires seeing the old values, to support the prior value view.
        parentKTable.enableSendingOldValues(true);
    }

    @Override
    public Processor<K, Change<V>, K, Change<V>> get() {
        return new KTableSuppressProcessor<>(suppress, storeName);
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parentKTable.valueGetterSupplier();
        return new KTableValueGetterSupplier<K, V>() {

            @Override
            public KTableValueGetter<K, V> get() {
                final KTableValueGetter<K, V> parentGetter = parentValueGetterSupplier.get();
                return new KTableValueGetter<K, V>() {
                    private TimeOrderedKeyValueBuffer<K, V> buffer;

                    @Override
                    public void init(final ProcessorContext<?, ?> context) {
                        parentGetter.init(context);
                        // the main processor is responsible for the buffer's lifecycle
                        buffer = requireNonNull(context.getStateStore(storeName));
                    }

                    @Override
                    public ValueAndTimestamp<V> get(final K key) {
                        final Maybe<ValueAndTimestamp<V>> maybeValue = buffer.priorValueForBuffered(key);
                        if (maybeValue.isDefined()) {
                            return maybeValue.getNullableValue();
                        } else {
                            // not buffered, so the suppressed view is equal to the parent view
                            return parentGetter.get(key);
                        }
                    }

                    @Override
                    public void close() {
                        // the main processor is responsible for the buffer's lifecycle
                        parentGetter.close();
                    }
                };
            }

            @Override
            public String[] storeNames() {
                final String[] parentStores = parentValueGetterSupplier.storeNames();
                final String[] stores = new String[1 + parentStores.length];
                System.arraycopy(parentStores, 0, stores, 1, parentStores.length);
                stores[0] = storeName;
                return stores;
            }
        };
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        return parentKTable.enableSendingOldValues(forceMaterialization);
    }

    private static final class KTableSuppressProcessor<K, V> extends ContextualProcessor<K, Change<V>, K, Change<V>> {
        private final long maxRecords;
        private final long maxBytes;
        private final long suppressDurationMillis;
        private final TimeDefinition<K> bufferTimeDefinition;
        private final BufferFullStrategy bufferFullStrategy;
        private final boolean safeToDropTombstones;
        private final String storeName;

        private TimeOrderedKeyValueBuffer<K, V> buffer;
        private InternalProcessorContext<K, Change<V>> internalProcessorContext;
        private Sensor suppressionEmitSensor;
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

        private KTableSuppressProcessor(final SuppressedInternal<K> suppress, final String storeName) {
            this.storeName = storeName;
            requireNonNull(suppress);
            maxRecords = suppress.bufferConfig().maxRecords();
            maxBytes = suppress.bufferConfig().maxBytes();
            suppressDurationMillis = suppress.timeToWaitForMoreEvents().toMillis();
            bufferTimeDefinition = suppress.timeDefinition();
            bufferFullStrategy = suppress.bufferConfig().bufferFullStrategy();
            safeToDropTombstones = suppress.safeToDropTombstones();
        }

        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext<K, Change<V>>) context;
            suppressionEmitSensor = ProcessorNodeMetrics.suppressionEmitSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                internalProcessorContext.currentNode().name(),
                internalProcessorContext.metrics()
            );

            buffer = requireNonNull(context.getStateStore(storeName));
            buffer.setSerdesIfNull(new SerdeGetter(context));
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            observedStreamTime = Math.max(observedStreamTime, record.timestamp());
            buffer(record);
            enforceConstraints();
        }

        private void buffer(final Record<K, Change<V>> record) {
            final long bufferTime = bufferTimeDefinition.time(internalProcessorContext, record.key());

            buffer.put(bufferTime, record, internalProcessorContext.recordContext());
        }

        private void enforceConstraints() {
            final long streamTime = observedStreamTime;
            final long expiryTime = streamTime - suppressDurationMillis;

            buffer.evictWhile(() -> buffer.minTimestamp() <= expiryTime, this::emit);

            if (overCapacity()) {
                switch (bufferFullStrategy) {
                    case EMIT:
                        buffer.evictWhile(this::overCapacity, this::emit);
                        return;
                    case SHUT_DOWN:
                        throw new StreamsException(String.format(
                            "%s buffer exceeded its max capacity. Currently [%d/%d] records and [%d/%d] bytes.",
                            internalProcessorContext.currentNode().name(),
                            buffer.numRecords(), maxRecords,
                            buffer.bufferSize(), maxBytes
                        ));
                    default:
                        throw new UnsupportedOperationException(
                            "The bufferFullStrategy [" + bufferFullStrategy +
                                "] is not implemented. This is a bug in Kafka Streams."
                        );
                }
            }
        }

        private boolean overCapacity() {
            return buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
        }

        private void emit(final TimeOrderedKeyValueBuffer.Eviction<K, V> toEmit) {
            if (shouldForward(toEmit.value())) {
                final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
                internalProcessorContext.setRecordContext(toEmit.recordContext());
                try {
                    internalProcessorContext.forward(toEmit.record()
                        .withTimestamp(toEmit.recordContext().timestamp())
                        .withHeaders(toEmit.recordContext().headers()));
                    suppressionEmitSensor.record(1.0d, internalProcessorContext.currentSystemTimeMs());
                } finally {
                    internalProcessorContext.setRecordContext(prevRecordContext);
                }
            }
        }

        private boolean shouldForward(final Change<V> value) {
            return value.newValue != null || !safeToDropTombstones;
        }
    }
}
