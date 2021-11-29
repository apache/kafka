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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableTransformValues<K, V, VOut> implements KTableProcessorSupplier<K, V, K, VOut> {
    private final KTableImpl<K, ?, V> parent;
    private final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableTransformValues(final KTableImpl<K, ?, V> parent,
                          final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier,
                          final String queryableName) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.transformerSupplier = Objects.requireNonNull(transformerSupplier, "transformerSupplier");
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>, K, Change<VOut>> get() {
        return new KTableTransformValuesProcessor(transformerSupplier.get());
    }

    @Override
    public KTableValueGetterSupplier<K, VOut> view() {
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        }

        return new KTableValueGetterSupplier<K, VOut>() {
            final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            public KTableValueGetter<K, VOut> get() {
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

    private class KTableTransformValuesProcessor extends ContextualProcessor<K, Change<V>, K, Change<VOut>> {
        private final ValueTransformerWithKey<? super K, ? super V, ? extends VOut> valueTransformer;
        private TimestampedKeyValueStore<K, VOut> store;
        private TimestampedTupleForwarder<K, VOut> tupleForwarder;

        private KTableTransformValuesProcessor(final ValueTransformerWithKey<? super K, ? super V, ? extends VOut> valueTransformer) {
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @Override
        public void init(final ProcessorContext<K, Change<VOut>> context) {
            super.init(context);
            final InternalProcessorContext<K, Change<VOut>> internalProcessorContext = (InternalProcessorContext<K, Change<VOut>>) context;
            valueTransformer.init(new ForwardingDisabledProcessorContext(internalProcessorContext));
            if (queryableName != null) {
                store = context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            final VOut newValue = valueTransformer.transform(record.key(), record.value().newValue);

            if (queryableName == null) {
                final VOut oldValue = sendOldValues ? valueTransformer.transform(record.key(), record.value().oldValue) : null;
                context().forward(record.withValue(new Change<>(newValue, oldValue)));
            } else {
                final VOut oldValue = sendOldValues ? getValueOrNull(store.get(record.key())) : null;
                store.put(record.key(), ValueAndTimestamp.make(newValue, record.timestamp()));
                tupleForwarder.maybeForward(record.withValue(new Change<>(newValue, oldValue)));
            }
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }


    private class KTableTransformValuesGetter implements KTableValueGetter<K, VOut> {
        private final KTableValueGetter<K, V> parentGetter;
        private InternalProcessorContext internalProcessorContext;
        private final ValueTransformerWithKey<? super K, ? super V, ? extends VOut> valueTransformer;

        KTableTransformValuesGetter(final KTableValueGetter<K, V> parentGetter,
                                    final ValueTransformerWithKey<? super K, ? super V, ? extends VOut> valueTransformer) {
            this.parentGetter = Objects.requireNonNull(parentGetter, "parentGetter");
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            internalProcessorContext = (InternalProcessorContext) context;
            parentGetter.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(internalProcessorContext));
        }

        @Override
        public ValueAndTimestamp<VOut> get(final K key) {
            final ValueAndTimestamp<V> valueAndTimestamp = parentGetter.get(key);

            final ProcessorRecordContext currentContext = internalProcessorContext.recordContext();

            internalProcessorContext.setRecordContext(new ProcessorRecordContext(
                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp(),
                -1L, // we don't know the original offset
                // technically, we know the partition, but in the new `api.Processor` class,
                // we move to `RecordMetadata` than would be `null` for this case and thus
                // we won't have the partition information, so it's better to not provide it
                // here either, to not introduce a regression later on
                -1,
                null, // we don't know the upstream input topic
                new RecordHeaders()
            ));

            final ValueAndTimestamp<VOut> result = ValueAndTimestamp.make(
                valueTransformer.transform(key, getValueOrNull(valueAndTimestamp)),
                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp());

            internalProcessorContext.setRecordContext(currentContext);

            return result;
        }

        @Override
        public void close() {
            parentGetter.close();
            valueTransformer.close();
        }
    }
}
