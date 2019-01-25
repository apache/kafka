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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;


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
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V1 computeValue(final K key, final V value) {
        V1 newValue = null;

        if (value != null) {
            newValue = mapper.apply(key, value);
        }

        return newValue;
    }

    private class KTableMapValuesProcessor extends AbstractProcessor<K, Change<V>> {
        private KeyValueStore<K, ValueAndTimestamp<V1>> store;
        private TupleForwarder<K, V1> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                StateStore store = context.getStateStore(queryableName);
                if (store instanceof WrappedStateStore) {
                    store = ((WrappedStateStore) store).wrappedStore();
                }
                this.store = ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade) store).inner;
                tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V1>(context), sendOldValues);
            }
        }

        @Override
        public void process(final K key, final Change<V> change) {
            final V1 newValue = computeValue(key, change.newValue);
            final V1 oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (queryableName != null) {
                store.put(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            } else {
                context().forward(key, new Change<>(newValue, oldValue));
            }
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
            final ValueAndTimestamp<V> parentValueAndTimestamp = parentGetter.get(key);
            final V parentValue = parentValueAndTimestamp == null ? null : parentValueAndTimestamp.value();
            final V1 resultValue = computeValue(key, parentValue);
            return resultValue == null ? null : ValueAndTimestamp.make(resultValue, parentValueAndTimestamp.timestamp());  // this is a potential NPE -- if `parentValueAndTimestamp == null` we could also directly return `null`, but this would be a semantic change
        }


        @Override
        public void close() {
            parentGetter.close();
        }
    }
}
