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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

public class KTableSource<K, V> implements ProcessorSupplier<K, V, K, Change<V>> {
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
    public Processor<K, V, K, Change<V>> get() {
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

    private class KTableSourceProcessor extends ContextualProcessor<K, V, K, Change<V>> {

        private TimestampedKeyValueStore<K, V> store;
        private TupleChangeForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;

        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            if (queryableName != null) {
                store = context.getStateStore(queryableName);
                tupleForwarder = new TupleChangeForwarder<>(
                    store,
                    context,
                    new TupleChangeCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<K, V> record) {
            // if the key is null, then ignore the record
            if (record.key() == null) {
                LOG.warn(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    context().recordMetadata().map(RecordMetadata::topic).orElse("<>"),
                    context().recordMetadata().map(RecordMetadata::partition).orElse(-1),
                    context().recordMetadata().map(RecordMetadata::offset).orElse(-1L)
                );
                droppedRecordsSensor.record();
                return;
            }

            if (queryableName != null) {
                final ValueAndTimestamp<V> oldValueAndTimestamp = store.get(record.key());
                final V oldValue;
                if (oldValueAndTimestamp != null) {
                    oldValue = oldValueAndTimestamp.value();
                    if (record.timestamp() < oldValueAndTimestamp.timestamp()) {
                        LOG.warn("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            store.name(),
                            context().recordMetadata().get().offset(),
                            context().recordMetadata().get().partition());
                    }
                } else {
                    oldValue = null;
                }
                store.put(record.key(), ValueAndTimestamp.make(record));
                tupleForwarder.maybeForward(record, record.value(), oldValue);
            } else {
                context().forward(record.withValue(new Change<>(record.value(), null)));
            }
        }
    }
}
