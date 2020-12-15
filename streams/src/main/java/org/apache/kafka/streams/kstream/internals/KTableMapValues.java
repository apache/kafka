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

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;


class KTableMapValues<K, V, V1> implements KTableProcessorSupplier<K, V, V1> {
    private final KTableImpl<K, ?, V> parent;
    private final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapper;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableMapValues(final KTableImpl<K, ?, V> parent,
                    final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapper,
                    final String queryableName) {
        this.parent = parent;
        this.mapper = mapper;
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableMapValuesProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V1> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply map-values on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<K, V1>() {
                final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<K, V1> get() {
                    return new KTableMapValuesValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public String[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        if (queryableName != null) {
            sendOldValues = true;
            return true;
        }

        if (parent.enableSendingOldValues(forceMaterialization)) {
            sendOldValues = true;
        }

        return sendOldValues;
    }

    private V1 computeValue(final K key, final V value) {
        V1 newValue = null;

        if (value != null) {
            newValue = mapper.apply(key, value);
        }

        return newValue;
    }

    private ValueAndTimestamp<V1> computeValueAndTimestamp(final K key, final ValueAndTimestamp<V> valueAndTimestamp) {
        V1 newValue = null;
        long timestamp = 0;

        if (valueAndTimestamp != null) {
            newValue = mapper.apply(key, valueAndTimestamp.value());
            timestamp = valueAndTimestamp.timestamp();
        }

        return ValueAndTimestamp.make(newValue, timestamp);
    }


    private class KTableMapValuesProcessor extends AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                store = (TimestampedKeyValueStore<K, V1>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final K key, final Change<V> change) {
            final V1 newValue = computeValue(key, change.newValue);
            final V1 oldValue = computeOldValue(key, change);

            if (queryableName != null) {
                store.put(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            } else {
                context().forward(key, new Change<>(newValue, oldValue));
            }
        }

        private V1 computeOldValue(final K key, final Change<V> change) {
            if (!sendOldValues) {
                return null;
            }

            return queryableName != null
                ? getValueOrNull(store.get(key))
                : computeValue(key, change.oldValue);
        }
    }


    private class KTableMapValuesValueGetter implements KTableValueGetter<K, V1> {
        private final KTableValueGetter<K, V> parentGetter;

        KTableMapValuesValueGetter(final KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(final ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<V1> get(final K key) {
            return computeValueAndTimestamp(key, parentGetter.get(key));
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }
}
