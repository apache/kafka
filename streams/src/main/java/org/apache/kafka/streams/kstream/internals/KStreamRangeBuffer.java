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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

/**
 * Buffer processor for {@link org.apache.kafka.streams.kstream.RangedKStream}.
 * Writes each record to the buffer {@link WindowStore} and forwards it downstream.
 * Drops records with null keys or values, and records that violate the grace period.
 */
public class KStreamRangeBuffer<K, V> implements ProcessorSupplier<K, V, K, V> {

    private static final Logger log = LoggerFactory.getLogger(KStreamRangeBuffer.class);

    private final StoreFactory storeFactory;
    private final long gracePeriodMs;

    public KStreamRangeBuffer(final StoreFactory storeFactory, final long gracePeriodMs) {
        this.storeFactory = storeFactory;
        this.gracePeriodMs = gracePeriodMs;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(new FactoryWrappingStoreBuilder<>(storeFactory));
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new KStreamRangeBufferProcessor<>(storeFactory.storeName(), gracePeriodMs);
    }

    private static final class KStreamRangeBufferProcessor<K, V> extends ContextualProcessor<K, V, K, V> {

        private final String storeName;
        private final long gracePeriodMs;

        private WindowStore<K, V> store;
        private Sensor droppedRecordsSensor;
        private long observedStreamTime = Long.MIN_VALUE;

        KStreamRangeBufferProcessor(final String storeName, final long gracePeriodMs) {
            this.storeName = storeName;
            this.gracePeriodMs = gracePeriodMs;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<K, V> context) {
            super.init(context);
            store = context.getStateStore(storeName);
            final InternalProcessorContext<K, V> internalContext = (InternalProcessorContext<K, V>) context;
            final StreamsMetricsImpl metrics = internalContext.metrics();
            droppedRecordsSensor = droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                metrics
            );
        }

        @Override
        public void process(final Record<K, V> record) {
            if (record.key() == null) {
                logDropped("null key", record);
                droppedRecordsSensor.record();
                return;
            }
            if (record.value() == null) {
                logDropped("null value", record);
                droppedRecordsSensor.record();
                return;
            }

            final long timestamp = record.timestamp();
            if (observedStreamTime < timestamp) {
                observedStreamTime = timestamp;
            }

            if (gracePeriodMs >= 0 && timestamp < observedStreamTime - gracePeriodMs) {
                logDropped("grace period", record);
                droppedRecordsSensor.record();
                return;
            }

            store.put(record.key(), record.value(), timestamp);
            context().forward(record);
        }

        private void logDropped(final String reason, final Record<K, V> record) {
            if (context().recordMetadata().isPresent()) {
                final RecordMetadata meta = context().recordMetadata().get();
                log.warn("Skipping record due to {}. topic=[{}] partition=[{}] offset=[{}]",
                    reason, meta.topic(), meta.partition(), meta.offset());
            } else {
                log.warn("Skipping record due to {}. Topic, partition, and offset not known.", reason);
            }
        }
    }
}
