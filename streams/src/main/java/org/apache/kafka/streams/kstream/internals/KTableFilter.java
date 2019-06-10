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

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.StateStoreType;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

class KTableFilter<K, V> implements KTableProcessorSupplier<K, V, V> {
    private final KTableImpl<K, ?, V> parent;
    private final Predicate<?, ? super V> predicate;
    private final boolean filterNot;
    private final String queryableName;
    private boolean sendOldValues = false;
    private final StateStoreType stateStoreType;

    KTableFilter(final KTableImpl<K, ?, V> parent,
                 final Predicate<?, ? super V> predicate,
                 final boolean filterNot,
                 final String queryableName,
                 StateStoreType stateStoreType) {
        this.parent = parent;
        this.predicate = predicate;
        this.filterNot = filterNot;
        this.queryableName = queryableName;
        this.stateStoreType = stateStoreType;
    }

    @Override
    public Processor<?, Change<V>> get() {
        if (stateStoreType == StateStoreType.KEY_VALUE_STORE) {
            return new KTableFilterProcessor();
        } else if (stateStoreType == StateStoreType.TIME_WINDOW_STORE) {
            return new TimeWindowedKTableFilterProcessor();
        } else {
            return new SessionWindowedKTableFilterProcessor();
        }
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V computeValue(final K key, final V value) {
        V newValue = null;

        if (value != null && (filterNot ^ ((Predicate<K, V>)predicate).test(key, value))) {
            newValue = value;
        }

        return newValue;
    }

    private ValueAndTimestamp<V> computeValue(final K key, final ValueAndTimestamp<V> valueAndTimestamp) {
        ValueAndTimestamp<V> newValueAndTimestamp = null;

        if (valueAndTimestamp != null) {
            final V value = valueAndTimestamp.value();
            if (filterNot ^ ((Predicate<K, V>)predicate).test(key, value)) {
                newValueAndTimestamp = valueAndTimestamp;
            }
        }

        return newValueAndTimestamp;
    }


    private V computeWindowedValue(final Windowed<K> key, final V value) {
        V newValue = null;

        if (value != null && (filterNot ^ ((Predicate<Windowed<K>, V>)predicate).test(key, value))) {
            newValue = value;
        }

        return newValue;
    }

    private ValueAndTimestamp<V> computeWindowedValue(final Windowed<K> key, final ValueAndTimestamp<V> valueAndTimestamp) {
        ValueAndTimestamp<V> newValueAndTimestamp = null;

        if (valueAndTimestamp != null) {
            final V value = valueAndTimestamp.value();
            if (filterNot ^ ((Predicate<Windowed<K>, V>)predicate).test(key, value)) {
                newValueAndTimestamp = valueAndTimestamp;
            }
        }

        return newValueAndTimestamp;
    }

    private class KTableFilterProcessor extends AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
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
        public void process(final K key, final Change<V> change) {
            final V newValue = computeValue(key, change.newValue);
            final V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null) {
                return; // unnecessary to forward here.
            }

            if (queryableName != null) {
                store.put(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            } else {
                context().forward(key, new Change<>(newValue, oldValue));
            }
        }
    }

    private class TimeWindowedKTableFilterProcessor extends AbstractProcessor<Windowed<K>, Change<V>> {
        private TimestampedWindowStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                store = (TimestampedWindowStore<K, V>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                        store,
                        context,
                        new TimestampedCacheFlushListener<>(context),
                        sendOldValues);
            }
        }

        @Override
        public void process(Windowed<K> windowedKey, final Change<V> change) {
            final K messageKey = windowedKey.key();
            final V newValue = computeWindowedValue(windowedKey, change.newValue);
            final V oldValue = sendOldValues ? computeWindowedValue(windowedKey, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null) {
                return; // unnecessary to forward here.
            }

            if (queryableName != null) {
                store.put(messageKey,
                          ValueAndTimestamp.make(newValue, context().timestamp()),
                          windowedKey.window().start());
                tupleForwarder.maybeForward(messageKey, newValue, oldValue);
            } else {
                context().forward(windowedKey, new Change<>(newValue, oldValue));
            }
        }
    }

    private class SessionWindowedKTableFilterProcessor extends AbstractProcessor<Windowed<K>, Change<V>> {
        private SessionStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                store = (SessionStore<K, V>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                        store,
                        context,
                        new TimestampedCacheFlushListener<>(context),
                        sendOldValues);
            }
        }

        @Override
        public void process(Windowed<K> windowedKey, final Change<V> change) {
            final K messageKey = windowedKey.key();
            final V newValue = computeWindowedValue(windowedKey, change.newValue);
            final V oldValue = sendOldValues ? computeWindowedValue(windowedKey, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null) {
                return; // unnecessary to forward here.
            }

            if (queryableName != null) {
                store.put(windowedKey, newValue);
                tupleForwarder.maybeForward(messageKey, newValue, oldValue);
            } else {
                context().forward(windowedKey, new Change<>(newValue, oldValue));
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply filter on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<K, V>() {
                final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<K, V> get() {
                    return new KTableFilterValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public String[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }


    private class KTableFilterValueGetter implements KTableValueGetter<K, V> {
        private final KTableValueGetter<K, V> parentGetter;

        KTableFilterValueGetter(final KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            return computeValue(key, parentGetter.get(key));
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }

}
