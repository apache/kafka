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
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

public class KStreamAggregate<KIn, VIn, VAgg> implements KStreamAggProcessorSupplier<KIn, VIn, KIn, VAgg> {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamAggregate.class);

    private final String storeName;
    private final StoreFactory storeFactory;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;

    private boolean sendOldValues = false;

    KStreamAggregate(final StoreFactory storeFactory,
                     final Initializer<VAgg> initializer,
                     final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this.storeFactory = storeFactory;
        this.storeName = storeFactory.storeName();
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(new FactoryWrappingStoreBuilder<>(storeFactory));
    }

    @Override
    public Processor<KIn, VIn, KIn, Change<VAgg>> get() {
        return new KStreamAggregateProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamAggregateProcessor extends ContextualProcessor<KIn, VIn, KIn, Change<VAgg>> {
        private KeyValueStoreWrapper<KIn, VAgg> store;
        private Sensor droppedRecordsSensor;
        private TimestampedTupleForwarder<KIn, VAgg> tupleForwarder;

        @Override
        public void init(final ProcessorContext<KIn, Change<VAgg>> context) {
            super.init(context);
            droppedRecordsSensor = droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics());
            store = new KeyValueStoreWrapper<>(context, storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store.store(),
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            // If the key or value is null we don't need to proceed
            if (record.key() == null || record.value() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key or value. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key or value. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            final ValueAndTimestamp<VAgg> oldAggAndTimestamp = store.get(record.key());
            VAgg oldAgg = getValueOrNull(oldAggAndTimestamp);

            final VAgg newAgg;
            final long newTimestamp;

            if (oldAgg == null) {
                oldAgg = initializer.apply();
                newTimestamp = record.timestamp();
            } else {
                oldAgg = oldAggAndTimestamp.value();
                newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
            }

            newAgg = aggregator.apply(record.key(), record.value(), oldAgg);

            final long putReturnCode = store.put(record.key(), newAgg, newTimestamp);
            // if not put to store, do not forward downstream either
            if (putReturnCode != PUT_RETURN_CODE_NOT_PUT) {
                tupleForwarder.maybeForward(
                    record.withValue(new Change<>(newAgg, sendOldValues ? oldAgg : null, putReturnCode == PUT_RETURN_CODE_IS_LATEST))
                        .withTimestamp(newTimestamp));
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<KIn, VAgg> view() {
        return new KTableValueGetterSupplier<KIn, VAgg>() {

            public KTableValueGetter<KIn, VAgg> get() {
                return new KStreamAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KStreamAggregateValueGetter implements KTableValueGetter<KIn, VAgg> {
        private KeyValueStoreWrapper<KIn, VAgg> store;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            store = new KeyValueStoreWrapper<>(context, storeName);
        }

        @Override
        public ValueAndTimestamp<VAgg> get(final KIn key) {
            return store.get(key);
        }

        @Override
        public ValueAndTimestamp<VAgg> get(final KIn key, final long asOfTimestamp) {
            return store.get(key, asOfTimestamp);
        }

        @Override
        public boolean isVersioned() {
            return store.isVersionedStore();
        }
    }
}
