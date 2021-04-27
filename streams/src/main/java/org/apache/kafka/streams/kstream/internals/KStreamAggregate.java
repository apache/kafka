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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamAggregate<K, V, T> implements KStreamAggProcessorSupplier<K, K, V, T> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamAggregate.class);
    private final String storeName;
    private final Initializer<T> initializer;
    private final Aggregator<? super K, ? super V, T> aggregator;

    private boolean sendOldValues = false;

    KStreamAggregate(final String storeName,
                     final Initializer<T> initializer,
                     final Aggregator<? super K, ? super V, T> aggregator) {
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamAggregateProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamAggregateProcessor extends AbstractProcessor<K, V> {
        private TimestampedKeyValueStore<K, T> store;
        private Sensor droppedRecordsSensor;
        private TimestampedTupleForwarder<K, T> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics());
            store = (TimestampedKeyValueStore<K, T>) context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null) {
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            final ValueAndTimestamp<T> oldAggAndTimestamp = store.get(key);
            T oldAgg = getValueOrNull(oldAggAndTimestamp);

            final T newAgg;
            final long newTimestamp;

            if (oldAgg == null) {
                oldAgg = initializer.apply();
                newTimestamp = context().timestamp();
            } else {
                oldAgg = oldAggAndTimestamp.value();
                newTimestamp = Math.max(context().timestamp(), oldAggAndTimestamp.timestamp());
            }

            newAgg = aggregator.apply(key, value, oldAgg);

            store.put(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }

    @Override
    public KTableValueGetterSupplier<K, T> view() {
        return new KTableValueGetterSupplier<K, T>() {

            public KTableValueGetter<K, T> get() {
                return new KStreamAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }


    private class KStreamAggregateValueGetter implements KTableValueGetter<K, T> {
        private TimestampedKeyValueStore<K, T> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (TimestampedKeyValueStore<K, T>) context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<T> get(final K key) {
            return store.get(key);
        }
    }
}
