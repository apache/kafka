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
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableProcessValues<K, V, VOut> implements KTableProcessorSupplier<K, V, K, VOut> {
    private final KTableImpl<K, ?, V> parent;
    private final FixedKeyProcessorSupplier<? super K, ? super V, ? extends VOut> fixedKeyProcessorSupplier;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableProcessValues(final KTableImpl<K, ?, V> parent,
                        final FixedKeyProcessorSupplier<? super K, ? super V, ? extends VOut> fixedKeyProcessorSupplier,
                        final String queryableName) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.fixedKeyProcessorSupplier = Objects.requireNonNull(fixedKeyProcessorSupplier, "fixedKeyProcessorSupplier");
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>, K, Change<VOut>> get() {
        return new KTableFixedKeyProcessor(fixedKeyProcessorSupplier.get());
    }

    @Override
    public KTableValueGetterSupplier<K, VOut> view() {
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        }

        return new KTableValueGetterSupplier<>() {
            final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            public KTableValueGetter<K, VOut> get() {
                return new KTableTransformValuesGetter(
                    parentValueGetterSupplier.get(),
                    fixedKeyProcessorSupplier.get());
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

    private class KTableFixedKeyProcessor extends ContextualProcessor<K, Change<V>, K, Change<VOut>> {
        private final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor;
        private ForwardCaptureProcessorContext<K, V, VOut> captureContext;


        private KTableFixedKeyProcessor(final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor) {
            this.fixedKeyProcessor = Objects.requireNonNull(fixedKeyProcessor, "fixedKeyProcessor");
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public void init(final ProcessorContext<K, Change<VOut>> context) {
            super.init(context);
            captureContext = new ForwardCaptureProcessorContext<>(context, queryableName, sendOldValues, fixedKeyProcessor);
            fixedKeyProcessor.init((FixedKeyProcessorContext) captureContext);
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            captureContext.setInputRecord(record);

            fixedKeyProcessor.process(InternalFixedKeyRecordFactory.create(
                new Record<>(record.key(), record.value().newValue, record.timestamp(), record.headers())
            ));
        }

        @Override
        public void close() {
            fixedKeyProcessor.close();
        }
    }


    private class KTableTransformValuesGetter implements KTableValueGetter<K, VOut> {
        private final KTableValueGetter<K, V> parentGetter;
        private InternalProcessorContext<?, ?> internalProcessorContext;
        private final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor;
        private ForwardCaptureProcessorContext<K, V, VOut> captureContext;

        KTableTransformValuesGetter(final KTableValueGetter<K, V> parentGetter,
                                    final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor) {
            this.parentGetter = Objects.requireNonNull(parentGetter, "parentGetter");
            this.fixedKeyProcessor = Objects.requireNonNull(fixedKeyProcessor, "fixedKeyProcessor");
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public void init(final ProcessorContext<?, ?> context) {
            internalProcessorContext = (InternalProcessorContext<?, ?>) context;
            parentGetter.init(context);

            captureContext = new ForwardCaptureProcessorContext<>((ProcessorContext<K, Change<VOut>>) context, queryableName, sendOldValues, fixedKeyProcessor);
            fixedKeyProcessor.init((FixedKeyProcessorContext) captureContext);
        }

        @Override
        public ValueAndTimestamp<VOut> get(final K key) {
            return processValue(key, parentGetter.get(key));
        }

        @Override
        public ValueAndTimestamp<VOut> get(final K key, final long asOfTimestamp) {
            return processValue(key, parentGetter.get(key, asOfTimestamp));
        }

        @Override
        public boolean isVersioned() {
            return parentGetter.isVersioned();
        }

        @Override
        public void close() {
            parentGetter.close();
            fixedKeyProcessor.close();
        }

        private ValueAndTimestamp<VOut> processValue(final K key, final ValueAndTimestamp<V> valueAndTimestamp) {
            final ProcessorRecordContext currentContext = internalProcessorContext.recordContext();

            final long timestamp = valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp();

            internalProcessorContext.setRecordContext(new ProcessorRecordContext(
                timestamp,
                -1L, // we don't know the original offset
                // technically, we know the partition, but in the new `api.Processor` class,
                // we move to `RecordMetadata` than would be `null` for this case and thus
                // we won't have the partition information, so it's better to not provide it
                // here either, to not introduce a regression later on
                -1,
                null, // we don't know the upstream input topic
                new RecordHeaders()
            ));

            captureContext.forward = false;
            fixedKeyProcessor.process(InternalFixedKeyRecordFactory.create(
                new Record<>(key, getValueOrNull(valueAndTimestamp), timestamp) // TODO: we might pass in -1L here, which would lead to an exception
            ));
            captureContext.forward = true;
            final ValueAndTimestamp<VOut> result = ValueAndTimestamp.make(
                captureContext.capturedValue,
                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp());

            internalProcessorContext.setRecordContext(currentContext);

            return result;
        }
    }
}
