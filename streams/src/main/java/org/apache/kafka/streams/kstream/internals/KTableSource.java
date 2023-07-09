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

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

import java.util.Objects;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KTableSource<KIn, VIn> implements ProcessorSupplier<KIn, VIn, KIn, Change<VIn>> {

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
    public Processor<KIn, VIn, KIn, Change<VIn>> get() {
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

    private class KTableSourceProcessor implements Processor<KIn, VIn, KIn, Change<VIn>> {

        private ProcessorContext<KIn, Change<VIn>> context;
        private KeyValueStoreWrapper<KIn, VIn> store;
        private TimestampedTupleForwarder<KIn, VIn> tupleForwarder;
        private Sensor droppedRecordsSensor;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<KIn, Change<VIn>> context) {
            this.context = context;
            final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(),
                context.taskId().toString(), metrics);
            if (queryableName != null) {
                store = new KeyValueStoreWrapper<>(context, queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store.getStore(),
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            // if the key is null, then ignore the record
            if (record.key() == null) {
                if (context.recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context.recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            if (queryableName != null) {
                final ValueAndTimestamp<VIn> oldValueAndTimestamp = store.get(record.key());
                final VIn oldValue;
                if (oldValueAndTimestamp != null) {
                    oldValue = oldValueAndTimestamp.value();
                    if (record.timestamp() < oldValueAndTimestamp.timestamp()) {
                        if (context.recordMetadata().isPresent()) {
                            final RecordMetadata recordMetadata = context.recordMetadata().get();
                            LOG.warn(
                                "Detected out-of-order KTable update for {}, "
                                    + "old timestamp=[{}] new timestamp=[{}]. "
                                    + "topic=[{}] partition=[{}] offset=[{}].",
                                store.name(),
                                oldValueAndTimestamp.timestamp(), record.timestamp(),
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset() 
                            );
                        } else {
                            LOG.warn(
                                "Detected out-of-order KTable update for {}, "
                                    + "old timestamp=[{}] new timestamp=[{}]. "
                                    + "Topic, partition and offset not known.",
                                store.name(),
                                oldValueAndTimestamp.timestamp(), record.timestamp()
                            );
                        }
                    }
                } else {
                    oldValue = null;
                }
                final long putReturnCode = store.put(record.key(), record.value(), record.timestamp());
                // if not put to store, do not forward downstream either
                if (putReturnCode != PUT_RETURN_CODE_NOT_PUT) {
                    tupleForwarder.maybeForward(record.withValue(new Change<>(record.value(), oldValue, putReturnCode == PUT_RETURN_CODE_IS_LATEST)));
                }
            } else {
                context.forward(record.withValue(new Change<>(record.value(), null, true)));
            }
        }
    }
}
