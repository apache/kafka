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
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

public class KTableReduce<K, V> implements KTableProcessorSupplier<K, V, K, V> {

    private final String storeName;
    private final Reducer<V> addReducer;
    private final Reducer<V> removeReducer;

    private boolean sendOldValues = false;

    KTableReduce(final String storeName, final Reducer<V> addReducer, final Reducer<V> removeReducer) {
        this.storeName = storeName;
        this.addReducer = addReducer;
        this.removeReducer = removeReducer;
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        // Reduce is always materialized:
        sendOldValues = true;
        return true;
    }

    @Override
    public Processor<K, Change<V>, K, Change<V>> get() {
        return new KTableReduceProcessor();
    }

    private class KTableReduceProcessor implements Processor<K, Change<V>, K, Change<V>> {

        private KeyValueStoreWrapper<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
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
        public void process(final Record<K, Change<V>> record) {
            // the keys should never be null
            if (record.key() == null) {
                throw new StreamsException("Record key for KTable reduce operator with state " + storeName + " should not be null.");
            }

            final ValueAndTimestamp<V> oldAggAndTimestamp = store.get(record.key());
            final V oldAgg = getValueOrNull(oldAggAndTimestamp);
            final V intermediateAgg;
            long newTimestamp;

            // first try to remove the old value
            if (record.value().oldValue != null && oldAgg != null) {
                intermediateAgg = removeReducer.apply(oldAgg, record.value().oldValue);
                newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
            } else {
                intermediateAgg = oldAgg;
                newTimestamp = record.timestamp();
            }

            // then try to add the new value
            final V newAgg;
            if (record.value().newValue != null) {
                if (intermediateAgg == null) {
                    newAgg = record.value().newValue;
                } else {
                    newAgg = addReducer.apply(intermediateAgg, record.value().newValue);
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
    public KTableValueGetterSupplier<K, V> view() {
        return new KTableMaterializedValueGetterSupplier<>(storeName);
    }
}
