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

import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.ValueTransformer;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ValueTransformerWithKeyAdapter;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableTransformValues<K, V, V1> implements KTableChangeProcessorSupplier<K, V, V1, K, V1> {
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
    public Processor<K, Change<V>, K, Change<V1>> get() {
        return new KTableTransformValuesProcessor(
            ValueTransformerWithKeyAdapter.adapt(transformerSupplier.get()));
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
                    ValueTransformerWithKeyAdapter.adapt(transformerSupplier.get()));
            }

            @Override
            public String[] storeNames() {
                return parentValueGetterSupplier.storeNames();
            }
        };
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

    private class KTableTransformValuesProcessor extends
        ContextualProcessor<K, Change<V>, K, Change<V1>> {

        private final ValueTransformer<? super K, ? super V, ? extends V1> valueTransformer;
        private TimestampedKeyValueStore<K, V1> store;
        private TupleChangeForwarder<K, V1> tupleForwarder;

        private KTableTransformValuesProcessor(
            final ValueTransformer<? super K, ? super V, ? extends V1> valueTransformer) {
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<K, Change<V1>> context) {
            super.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext<>((InternalProcessorContext) context));
            if (queryableName != null) {
                store = context.getStateStore(queryableName);
                tupleForwarder = new TupleChangeForwarder<>(
                    store,
                    context,
                    new TupleChangeCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            final V1 newValue = valueTransformer.transform(record.withValue(record.value().newValue));

            if (queryableName == null) {
                final V1 v1 = valueTransformer.transform(record.withValue(record.value().oldValue));
                final V1 oldValue = sendOldValues ? v1 : null;
                context().forward(record.withValue(new Change<>(newValue, oldValue)));
            } else {
                final V1 oldValue = sendOldValues ? getValueOrNull(store.get(record.key())) : null;
                store.put(record.key(), ValueAndTimestamp.make(newValue, record.timestamp()));
                tupleForwarder.maybeForward(record, newValue, oldValue);
            }
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }


    private class KTableTransformValuesGetter implements KTableValueGetter<K, V1> {

        private final KTableValueGetter<K, V> parentGetter;
        private final ValueTransformer<? super K, ? super V, ? extends V1> valueTransformer;

        KTableTransformValuesGetter(final KTableValueGetter<K, V> parentGetter,
                                    final ValueTransformer<? super K, ? super V, ? extends V1> valueTransformer) {
            this.parentGetter = Objects.requireNonNull(parentGetter, "parentGetter");
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @SuppressWarnings("unchecked")
        @Override
        public <KParent, VParent> void init(final ProcessorContext<KParent, VParent> context) {
            parentGetter.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext<>((InternalProcessorContext) context));
        }

        @Override
        public ValueAndTimestamp<V1> get(final K key) {
            final ValueAndTimestamp<V> valueAndTimestamp = parentGetter.get(key);
            final long timestamp =
                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp();
            final Record<K, V> record = new Record<>(
                key,
                getValueOrNull(valueAndTimestamp),
                timestamp);
            return ValueAndTimestamp.make(valueTransformer.transform(record), timestamp);
        }

        @Override
        public void close() {
            parentGetter.close();
            valueTransformer.close();
        }
    }
}
