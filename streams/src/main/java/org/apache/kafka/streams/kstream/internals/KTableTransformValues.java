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
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

@SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
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
    public org.apache.kafka.streams.processor.Processor<K, Change<V>> get() {
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

    private class KTableTransformValuesProcessor extends org.apache.kafka.streams.processor.AbstractProcessor<K, Change<V>> {
        private final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer;
        private TimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;

        private KTableTransformValuesProcessor(final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer) {
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @Override
        public void init(final org.apache.kafka.streams.processor.ProcessorContext context) {
            super.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
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
        public void process(final K key, final Change<V> change) {
            final V1 newValue = valueTransformer.transform(key, change.newValue);

            if (queryableName == null) {
                final V1 oldValue = sendOldValues ? valueTransformer.transform(key, change.oldValue) : null;
                context().forward(key, new Change<>(newValue, oldValue));
            } else {
                final V1 oldValue = sendOldValues ? getValueOrNull(store.get(key)) : null;
                store.put(key, ValueAndTimestamp.make(newValue, context().timestamp()));
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
        private InternalProcessorContext internalProcessorContext;
        private final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer;

        KTableTransformValuesGetter(final KTableValueGetter<K, V> parentGetter,
                                    final ValueTransformerWithKey<? super K, ? super V, ? extends V1> valueTransformer) {
            this.parentGetter = Objects.requireNonNull(parentGetter, "parentGetter");
            this.valueTransformer = Objects.requireNonNull(valueTransformer, "valueTransformer");
        }

        @Override
        public void init(final org.apache.kafka.streams.processor.ProcessorContext context) {
            internalProcessorContext = (InternalProcessorContext) context;
            parentGetter.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
        }

        @Override
        public ValueAndTimestamp<V1> get(final K key) {
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

            final ValueAndTimestamp<V1> result = ValueAndTimestamp.make(
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
