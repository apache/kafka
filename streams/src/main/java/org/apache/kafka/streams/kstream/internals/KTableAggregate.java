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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

public class KTableAggregate<KIn, VIn, VAgg> implements
    KTableProcessorSupplier<KIn, VIn, KIn, VAgg> {

    private final String storeName;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> add;
    private final Aggregator<? super KIn, ? super VIn, VAgg> remove;

    private boolean sendOldValues = false;

    KTableAggregate(final String storeName,
                    final Initializer<VAgg> initializer,
                    final Aggregator<? super KIn, ? super VIn, VAgg> add,
                    final Aggregator<? super KIn, ? super VIn, VAgg> remove) {
        this.storeName = storeName;
        this.initializer = initializer;
        this.add = add;
        this.remove = remove;
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        // Aggregates are always materialized:
        sendOldValues = true;
        return true;
    }

    @Override
    public Processor<KIn, Change<VIn>, KIn, Change<VAgg>> get() {
        return new KTableAggregateProcessor();
    }

    private class KTableAggregateProcessor implements Processor<KIn, Change<VIn>, KIn, Change<VAgg>> {
        private KeyValueStoreWrapper<KIn, VAgg> store;
        private TimestampedTupleForwarder<KIn, VAgg> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<KIn, Change<VAgg>> context) {
            store = new KeyValueStoreWrapper<>(context, storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store.getStore(),
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final Record<KIn, Change<VIn>> record) {
            // the keys should never be null
            if (record.key() == null) {
                throw new StreamsException("Record key for KTable aggregate operator with state " + storeName + " should not be null.");
            }

            final ValueAndTimestamp<VAgg> oldAggAndTimestamp = store.get(record.key());
            final VAgg oldAgg = getValueOrNull(oldAggAndTimestamp);
            final VAgg intermediateAgg;
            long newTimestamp = record.timestamp();

            // first try to remove the old value
            if (record.value().oldValue != null && oldAgg != null) {
                intermediateAgg = remove.apply(record.key(), record.value().oldValue, oldAgg);
                newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
            } else {
                intermediateAgg = oldAgg;
            }

            // then try to add the new value
            final VAgg newAgg;
            if (record.value().newValue != null) {
                final VAgg initializedAgg;
                if (intermediateAgg == null) {
                    initializedAgg = initializer.apply();
                } else {
                    initializedAgg = intermediateAgg;
                }

                newAgg = add.apply(record.key(), record.value().newValue, initializedAgg);
                if (oldAggAndTimestamp != null) {
                    newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
                }
            } else {
                newAgg = intermediateAgg;
            }

            // update the store with the new value
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
        return new KTableMaterializedValueGetterSupplier<>(storeName);
    }
}
