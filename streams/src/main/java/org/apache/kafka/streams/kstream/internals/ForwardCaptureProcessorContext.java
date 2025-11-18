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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

public class ForwardCaptureProcessorContext<K, V, VOut> implements FixedKeyProcessorContext<K, VOut> {
    private final ProcessorContext<K, Change<VOut>> context;
    private final String queryableName;
    private final boolean sendOldValues;
    private final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor;

    private KeyValueStoreWrapper<K, VOut> store;
    private TimestampedTupleForwarder<K, VOut> tupleForwarder;

    private Record<K, Change<V>> inputRecord;
    boolean forward = true;

    K capturedKey;
    VOut capturedValue;
    long capturedTimestamp;

    public ForwardCaptureProcessorContext(final ProcessorContext<K, Change<VOut>> context,
                                          final String queryableName,
                                          final boolean sendOldValues,
                                          final FixedKeyProcessor<? super K, ? super V, ? extends VOut> fixedKeyProcessor) {
        this.context = context;
        this.queryableName = queryableName;
        this.sendOldValues = sendOldValues;
        this.fixedKeyProcessor = fixedKeyProcessor;

        if (queryableName != null) {
            store = new KeyValueStoreWrapper<>(context, queryableName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store.store(),
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }
    }

    @Override
    public <KForward extends K, VForward extends VOut> void forward(final FixedKeyRecord<KForward, VForward> record) {
        forward(record, null);
    }

    @Override
    public <KForward extends K, VForward extends VOut> void forward(final FixedKeyRecord<KForward, VForward> record, final String childName) {
        capturedKey = record.key();
        capturedValue = record.value();
        capturedTimestamp = record.timestamp();

        if (forward) {
            if (queryableName != null) {
                final VOut oldValue = sendOldValues ? getValueOrNull(store.get(capturedKey)) : null;
                final long putReturnCode = store.put(capturedKey, capturedValue, capturedTimestamp);
                // if not put to store, do not forward downstream either
                if (putReturnCode != PUT_RETURN_CODE_NOT_PUT) {
                    tupleForwarder.maybeForward(new Record<>(capturedKey, new Change<>(capturedValue, oldValue, putReturnCode == PUT_RETURN_CODE_IS_LATEST), capturedTimestamp, record.headers()));
                }
            } else {
                final VOut newValue = capturedValue;
                final VOut oldValue;
                final long timestamp = capturedTimestamp;

                if (sendOldValues) {
                    forward = false;
                    fixedKeyProcessor.process(InternalFixedKeyRecordFactory.create(
                        new Record<>(inputRecord.key(), inputRecord.value().oldValue, inputRecord.timestamp())
                    ));
                    forward = true;

                    oldValue = capturedValue;
                } else {
                    oldValue = null;
                }

                context.forward(
                    new Record<>(capturedKey, new Change<>(newValue, oldValue, inputRecord.value().isLatest), timestamp, record.headers()),
                    childName
                );
            }
        }
    }

    void setInputRecord(final Record<K, Change<V>> inputRecord) {
        this.inputRecord = inputRecord;
    }

    @Override
    public String applicationId() {
        return context.applicationId();
    }

    @Override
    public TaskId taskId() {
        return context.taskId();
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        return context.recordMetadata();
    }

    @Override
    public Serde<?> keySerde() {
        return context.keySerde();
    }

    @Override
    public Serde<?> valueSerde() {
        return context.valueSerde();
    }

    @Override
    public File stateDir() {
        return context.stateDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return context.metrics();
    }

    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        return context.getStateStore(name);
    }

    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        return context.schedule(interval, type, callback);
    }

    @Override
    public Cancellable schedule(final Instant startTime, final Duration interval, final PunctuationType type, final Punctuator callback) {
        return context.schedule(startTime, interval, type, callback);
    }

    @Override
    public void commit() {
        context.commit();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return context.appConfigs();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return context.appConfigsWithPrefix(prefix);
    }

    @Override
    public long currentSystemTimeMs() {
        return context.currentSystemTimeMs();
    }

    @Override
    public long currentStreamTimeMs() {
        return context.currentStreamTimeMs();
    }

}
