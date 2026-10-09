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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueTimestampHeaders.getValueOrNull;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

class KTableProcessFixedKey<K, V, VOut> implements KTableProcessorSupplier<K, V, K, VOut> {
    private final KTableImpl<K, ?, V> parent;
    private final FixedKeyProcessorSupplier<? super K, ? super V, ? extends VOut> fixedKeyProcessorSupplier;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableProcessFixedKey(final KTableImpl<K, ?, V> parent,
                          final FixedKeyProcessorSupplier<? super K, ? super V, ? extends VOut> fixedKeyProcessorSupplier,
                          final String queryableName) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.fixedKeyProcessorSupplier = Objects.requireNonNull(fixedKeyProcessorSupplier, "fixedKeyProcessorSupplier");
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>, K, Change<VOut>> get() {
        return new KTableProcessFixedKeyProcessor(fixedKeyProcessorSupplier.get());
    }

    @Override
    public KTableValueGetterSupplier<K, VOut> view() {
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        }

        return new KTableValueGetterSupplier<>() {
            final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            public KTableValueGetter<K, VOut> get() {
                return new KTableProcessFixedKeyGetter(
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

    private class KTableProcessFixedKeyProcessor extends ContextualProcessor<K, Change<V>, K, Change<VOut>> {
        private final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor;
        private KeyValueStoreWrapper<K, VOut> store;
        private TimestampedTupleForwarder<K, VOut> tupleForwarder;
        private CapturingFixedKeyProcessorContext capturingContext;

        private KTableProcessFixedKeyProcessor(final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor) {
            this.fixedKeyProcessor = Objects.requireNonNull(fixedKeyProcessor, "fixedKeyProcessor");
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void init(final ProcessorContext<K, Change<VOut>> context) {
            super.init(context);
            capturingContext = new CapturingFixedKeyProcessorContext((InternalProcessorContext<?, ?>) context);
            ((FixedKeyProcessor) fixedKeyProcessor).init(capturingContext);
            if (queryableName != null) {
                store = new KeyValueStoreWrapper<>(context, queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store.store(),
                    context,
                    store.isHeadersStore()
                        ? new TimestampedCacheFlushListenerWithHeaders<>(context)
                        : new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        private VOut applyProcessor(final K key, final V value, final long timestamp, final Headers headers) {
            capturingContext.reset();
            fixedKeyProcessor.process(InternalFixedKeyRecordFactory.create(new Record<>(key, value, timestamp, headers)));
            return capturingContext.captured();
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            final VOut newValue = applyProcessor(record.key(), record.value().newValue, record.timestamp(), record.headers());

            if (queryableName != null) {
                final VOut oldValue = sendOldValues ? getValueOrNull(store.get(record.key())) : null;
                final long putReturnCode = store.put(record.key(), newValue, record.timestamp(), new RecordHeaders());
                if (putReturnCode != PUT_RETURN_CODE_NOT_PUT) {
                    tupleForwarder.maybeForward(record.withValue(new Change<>(newValue, oldValue, putReturnCode == PUT_RETURN_CODE_IS_LATEST)));
                }
            } else {
                final VOut oldValue = sendOldValues
                    ? applyProcessor(record.key(), record.value().oldValue, record.timestamp(), record.headers())
                    : null;
                context().forward(record.withValue(new Change<>(newValue, oldValue, record.value().isLatest)));
            }
        }

        @Override
        public void close() {
            fixedKeyProcessor.close();
        }
    }

    private class KTableProcessFixedKeyGetter implements KTableValueGetter<K, VOut> {
        private final KTableValueGetter<K, V> parentGetter;
        private InternalProcessorContext<?, ?> internalProcessorContext;
        private final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor;
        private CapturingFixedKeyProcessorContext capturingContext;

        KTableProcessFixedKeyGetter(final KTableValueGetter<K, V> parentGetter,
                                    final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor) {
            this.parentGetter = Objects.requireNonNull(parentGetter, "parentGetter");
            this.fixedKeyProcessor = Objects.requireNonNull(fixedKeyProcessor, "fixedKeyProcessor");
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void init(final ProcessorContext<?, ?> context) {
            internalProcessorContext = (InternalProcessorContext<?, ?>) context;
            parentGetter.init(context);
            capturingContext = new CapturingFixedKeyProcessorContext(internalProcessorContext);
            ((FixedKeyProcessor) fixedKeyProcessor).init(capturingContext);
        }

        @Override
        public ValueTimestampHeaders<VOut> get(final K key) {
            return transformValue(key, parentGetter.get(key));
        }

        @Override
        public ValueTimestampHeaders<VOut> get(final K key, final long asOfTimestamp) {
            return transformValue(key, parentGetter.get(key, asOfTimestamp));
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

        private ValueTimestampHeaders<VOut> transformValue(final K key, final ValueTimestampHeaders<V> valueTimestampHeaders) {
            final ProcessorRecordContext savedContext = internalProcessorContext.recordContext();

            internalProcessorContext.setRecordContext(new ProcessorRecordContext(
                valueTimestampHeaders == null ? UNKNOWN : valueTimestampHeaders.timestamp(),
                -1L,
                -1,
                null,
                valueTimestampHeaders == null ? new RecordHeaders() : valueTimestampHeaders.headers()
            ));

            // FixedKeyRecord rejects negative timestamps; clamp to 0 when timestamp is UNKNOWN
            final long timestamp = valueTimestampHeaders == null ? 0L : Math.max(0L, valueTimestampHeaders.timestamp());
            final Headers headers = valueTimestampHeaders == null ? new RecordHeaders() : valueTimestampHeaders.headers();

            capturingContext.reset();
            fixedKeyProcessor.process(InternalFixedKeyRecordFactory.create(
                new Record<>(key, getValueOrNull(valueTimestampHeaders), timestamp, headers)
            ));
            final VOut transformedValue = capturingContext.captured();

            internalProcessorContext.setRecordContext(savedContext);

            return ValueTimestampHeaders.make(
                transformedValue,
                valueTimestampHeaders == null ? UNKNOWN : valueTimestampHeaders.timestamp(),
                valueTimestampHeaders == null ? savedContext.headers() : valueTimestampHeaders.headers()
            );
        }
    }

    // Intercepts forward() calls to capture the processor's output value.
    // All other ProcessingContext methods delegate to the underlying InternalProcessorContext.
    private class CapturingFixedKeyProcessorContext implements FixedKeyProcessorContext<K, VOut> {
        private final InternalProcessorContext<?, ?> delegate;
        private VOut capturedValue;

        CapturingFixedKeyProcessorContext(final InternalProcessorContext<?, ?> delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        void reset() {
            capturedValue = null;
        }

        VOut captured() {
            return capturedValue;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K1 extends K, V1 extends VOut> void forward(final FixedKeyRecord<K1, V1> record) {
            capturedValue = (VOut) record.value();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K1 extends K, V1 extends VOut> void forward(final FixedKeyRecord<K1, V1> record, final String childName) {
            capturedValue = (VOut) record.value();
        }

        @Override
        public String applicationId() {
            return delegate.applicationId();
        }

        @Override
        public TaskId taskId() {
            return delegate.taskId();
        }

        @Override
        public Optional<RecordMetadata> recordMetadata() {
            return delegate.recordMetadata();
        }

        @Override
        public Serde<?> keySerde() {
            return delegate.keySerde();
        }

        @Override
        public Serde<?> valueSerde() {
            return delegate.valueSerde();
        }

        @Override
        public File stateDir() {
            return delegate.stateDir();
        }

        @Override
        public StreamsMetrics metrics() {
            return delegate.metrics();
        }

        @Override
        public <S extends StateStore> S getStateStore(final String name) {
            return delegate.getStateStore(name);
        }

        @Override
        public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
            return delegate.schedule(interval, type, callback);
        }

        @Override
        public Cancellable schedule(final Instant startTime, final Duration interval, final PunctuationType type, final Punctuator callback) {
            return delegate.schedule(startTime, interval, type, callback);
        }

        @Override
        public void commit() {
            delegate.commit();
        }

        @Override
        public Map<String, Object> appConfigs() {
            return delegate.appConfigs();
        }

        @Override
        public Map<String, Object> appConfigsWithPrefix(final String prefix) {
            return delegate.appConfigsWithPrefix(prefix);
        }

        @Override
        public long currentSystemTimeMs() {
            return delegate.currentSystemTimeMs();
        }

        @Override
        public long currentStreamTimeMs() {
            return delegate.currentStreamTimeMs();
        }
    }
}
