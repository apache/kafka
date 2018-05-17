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

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

class KTableTransformValues<K, V, V1> implements KTableProcessorSupplier<K, V, V1> {

    private final KTableImpl<K, ?, V> parent;
    private final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends V1> transformerSupplier;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableTransformValues(final KTableImpl<K, ?, V> parent,
                          final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends V1> transformerSupplier,
                          final String queryableName) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.transformerSupplier = Objects.requireNonNull(transformerSupplier, "transformerSupplier");
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer = transformerSupplier.get();

        final OldValueGetter<K, V, V1> oldValueGetter;
        if (!sendOldValues) {
            oldValueGetter = new NullOldValueGetter<>();
        } else if (queryableName != null) {
            oldValueGetter = new MaterializedOldValueGetter<>(getMaterializedValueGetterSupplier());
        } else {
            oldValueGetter = new TransformingOldValueGetter<>(valueTransformer);
        }

        return new KTableTransformValuesProcessor(valueTransformer, oldValueGetter);
    }

    @Override
    public KTableValueGetterSupplier<K, V1> view() {
        if (queryableName != null) {
            return getMaterializedValueGetterSupplier();
        }

        return new KTableValueGetterSupplier<K, V1>() {
            final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            public KTableValueGetter<K, V1> get() {
                return new KTableTransformValuesGetter(
                        parentValueGetterSupplier.get(),
                        transformerSupplier.get());
            }

            @Override
            public String[] storeNames() {
                return parentValueGetterSupplier.storeNames();
            }
        };
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private KTableValueGetterSupplier<K, V1> getMaterializedValueGetterSupplier() {
        return new KTableMaterializedValueGetterSupplier<>(queryableName);
    }

    private static <K, V, V1> V1 computeValue(final K key,
                                              final V value,
                                              final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer) {
        if (value == null) {
            return null;
        }

        return valueTransformer.transform(key, value);
    }

    private class KTableTransformValuesProcessor extends AbstractProcessor<K, Change<V>> {
        private final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer;
        private final OldValueGetter<K, V, V1> oldValueGetter;
        private KeyValueStore<K, V1> store;
        private TupleForwarder<K, V1> tupleForwarder;

        private KTableTransformValuesProcessor(final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer,
                                               final OldValueGetter<K, V, V1> oldValueGetter) {
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
            this.oldValueGetter = Objects.requireNonNull(oldValueGetter, "oldValueGetter");
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);

            oldValueGetter.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));

            if (queryableName != null) {
                final ForwardingCacheFlushListener<K, V1> flushListener = new ForwardingCacheFlushListener<>(context, sendOldValues);
                store = (KeyValueStore<K, V1>) context.getStateStore(queryableName);
                tupleForwarder = new TupleForwarder<>(store, context, flushListener, sendOldValues);
            }
        }

        @Override
        public void process(final K key, final Change<V> change) {
            final V1 newValue = computeValue(key, change.newValue, valueTransformer);
            final V1 oldValue = oldValueGetter.get(key, change);

            if (queryableName == null) {
                context().forward(key, new Change<>(newValue, oldValue));
            } else {
                store.put(key, newValue);
                tupleForwarder.maybeForward(key, newValue, oldValue);
            }
        }

        @Override
        public void close() {
            valueTransformer.close();
            oldValueGetter.close();
        }
    }

    private class KTableTransformValuesGetter implements KTableValueGetter<K, V1> {

        private final KTableValueGetter<K, V> parentGetter;
        private final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer;

        KTableTransformValuesGetter(final KTableValueGetter<K, V> parentGetter,
                                    final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer) {
            this.parentGetter = Objects.requireNonNull(parentGetter, "parentGetter");
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @Override
        public void init(final ProcessorContext context) {
            parentGetter.init(context);

            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
        }

        @Override
        public V1 get(final K key) {
            return computeValue(key, parentGetter.get(key), valueTransformer);
        }

        @Override
        public void close() {
            parentGetter.close();
            valueTransformer.close();
        }
    }

    private interface OldValueGetter<K, V, V1> {
        void init(ProcessorContext context);

        V1 get(K key, Change<V> change);

        void close();
    }

    private static class NullOldValueGetter<K, V, V1> implements OldValueGetter<K, V, V1> {
        @Override
        public void init(final ProcessorContext context) {
        }

        @Override
        public V1 get(final K key, final Change<V> change) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    /**
     * Obtains the old value by querying the state store.
     *
     * This works correctly with stateful and stateless transformer implementations.
     */
    private static class MaterializedOldValueGetter<K, V, V1> implements OldValueGetter<K, V, V1> {
        private final KTableValueGetter<K, V1> valueGetter;

        private MaterializedOldValueGetter(final KTableValueGetterSupplier<K, V1> getterSupplier) {
            this.valueGetter = getterSupplier.get();
        }

        @Override
        public void init(final ProcessorContext context) {
            valueGetter.init(context);
        }

        @Override
        public V1 get(final K key, final Change<V> change) {
            return valueGetter.get(key);
        }

        @Override
        public void close() {
            valueGetter.close();
        }
    }

    /**
     * Obtains the old value by applying the transformer to the parent old value.
     *
     * This can result in incorrect results for stateful transformer implementations.
     * The table should be materialized where stateful transformers are used.
     */
    private static class TransformingOldValueGetter<K, V, V1> implements OldValueGetter<K, V, V1> {
        private final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer;

        private TransformingOldValueGetter(final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext context) {
        }

        @Override
        public V1 get(final K key, final Change<V> change) {
            return computeValue(key, change.oldValue, valueTransformer);
        }

        @Override
        public void close() {
        }
    }
}
