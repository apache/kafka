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

// Todo(ac): Document how many times transform instances might be created.
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
        return new KTableTransformValuesProcessor(transformerSupplier.get());
    }

    @Override
    public KTableValueGetterSupplier<K, V1> view() {
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
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
        private KeyValueStore<K, V1> store;
        private TupleForwarder<K, V1> tupleForwarder;

        private KTableTransformValuesProcessor(final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer) {
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);

            valueTransformer.init(new ForwardingDisabledProcessorContext(context));

            if (queryableName != null) {
                final ForwardingCacheFlushListener<K, V1> flushListener = new ForwardingCacheFlushListener<>(context, sendOldValues);
                store = (KeyValueStore<K, V1>) context.getStateStore(queryableName);
                tupleForwarder = new TupleForwarder<>(store, context, flushListener, sendOldValues);
            }
        }

        @Override
        public void process(final K key, final Change<V> change) {
            // Todo(ac): Use getter to get old value.
            final V1 newValue = computeValue(key, change.newValue, valueTransformer);
            final V1 oldValue = sendOldValues ? computeValue(key, change.oldValue, valueTransformer) : null;

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
}
