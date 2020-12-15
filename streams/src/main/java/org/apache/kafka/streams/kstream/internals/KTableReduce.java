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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KTableReduce<K, V> implements KTableProcessorSupplier<K, V, V> {

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
    public Processor<K, Change<V>> get() {
        return new KTableReduceProcessor();
    }

    private class KTableReduceProcessor extends AbstractProcessor<K, Change<V>> {

        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final K key, final Change<V> value) {
            // the keys should never be null
            if (key == null) {
                throw new StreamsException("Record key for KTable reduce operator with state " + storeName + " should not be null.");
            }

            final ValueAndTimestamp<V> oldAggAndTimestamp = store.get(key);
            final V oldAgg = getValueOrNull(oldAggAndTimestamp);
            final V intermediateAgg;
            long newTimestamp;

            // first try to remove the old value
            if (value.oldValue != null && oldAgg != null) {
                intermediateAgg = removeReducer.apply(oldAgg, value.oldValue);
                newTimestamp = Math.max(context().timestamp(), oldAggAndTimestamp.timestamp());
            } else {
                intermediateAgg = oldAgg;
                newTimestamp = context().timestamp();
            }

            // then try to add the new value
            final V newAgg;
            if (value.newValue != null) {
                if (intermediateAgg == null) {
                    newAgg = value.newValue;
                } else {
                    newAgg = addReducer.apply(intermediateAgg, value.newValue);
                    newTimestamp = Math.max(context().timestamp(), oldAggAndTimestamp.timestamp());
                }
            } else {
                newAgg = intermediateAgg;
            }

            // update the store with the new value
            store.put(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return new KTableMaterializedValueGetterSupplier<>(storeName);
    }
}
