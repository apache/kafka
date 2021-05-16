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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

public class KTableSource<K, V> implements ProcessorSupplier<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableSource.class);

    private final String storeName;
    private String queryableName;
    private boolean sendOldValues;

    public KTableSource(final String storeName, final String queryableName) {
        Objects.requireNonNull(storeName, "storeName can't be null");

        this.storeName = storeName;
        this.queryableName = queryableName;
        this.sendOldValues = false;
    }

    public String queryableName() {
        return queryableName;
    }

    @Override
    public Processor<K, V> get() {
        return new KTableSourceProcessor();
    }

    // when source ktable requires sending old values, we just
    // need to set the queryable name as the store name to enforce materialization
    public void enableSendingOldValues() {
        this.sendOldValues = true;
        this.queryableName = storeName;
    }

    // when the source ktable requires materialization from downstream, we just
    // need to set the queryable name as the store name to enforce materialization
    public void materialize() {
        this.queryableName = storeName;
    }

    public boolean materialized() {
        return queryableName != null;
    }

    private class KTableSourceProcessor extends AbstractProcessor<K, V> {

        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            if (queryableName != null) {
                store = (TimestampedKeyValueStore<K, V>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, then ignore the record
            if (key == null) {
                LOG.warn(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            if (queryableName != null) {
                final ValueAndTimestamp<V> oldValueAndTimestamp = store.get(key);
                final V oldValue;
                if (oldValueAndTimestamp != null) {
                    oldValue = oldValueAndTimestamp.value();
                    if (context().timestamp() < oldValueAndTimestamp.timestamp()) {
                        LOG.warn("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            store.name(), context().offset(), context().partition());
                    }
                } else {
                    oldValue = null;
                }
                store.put(key, ValueAndTimestamp.make(value, context().timestamp()));
                tupleForwarder.maybeForward(key, value, oldValue);
            } else {
                context().forward(key, new Change<>(value, null));
            }
        }
    }
}
