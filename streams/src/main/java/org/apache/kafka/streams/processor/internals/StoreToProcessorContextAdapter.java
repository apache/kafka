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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;

import java.io.File;
import java.time.Duration;
import java.util.Map;

public final class StoreToProcessorContextAdapter implements ProcessorContext {
    private final StateStoreContext delegate;

    public static ProcessorContext adapt(final StateStoreContext delegate) {
        if (delegate instanceof ProcessorContext) {
            return (ProcessorContext) delegate;
        } else {
            return new StoreToProcessorContextAdapter(delegate);
        }
    }

    private StoreToProcessorContextAdapter(final StateStoreContext delegate) {
        this.delegate = delegate;
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
    public void register(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
        delegate.register(store, stateRestoreCallback);
    }

    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        throw new UnsupportedOperationException("StateStores can't access getStateStore.");
    }

    @Deprecated
    @Override
    public Cancellable schedule(final long intervalMs, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("StateStores can't access schedule.");
    }

    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("StateStores can't access schedule.");
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        throw new UnsupportedOperationException("StateStores can't access forward.");
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        throw new UnsupportedOperationException("StateStores can't access forward.");
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("StateStores can't access commit.");
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException("StateStores can't access topic.");
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException("StateStores can't access partition.");
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException("StateStores can't access offset.");
    }

    @Override
    public Headers headers() {
        throw new UnsupportedOperationException("StateStores can't access headers.");
    }

    @Override
    public long timestamp() {
        throw new UnsupportedOperationException("StateStores can't access timestamp.");
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
        throw new UnsupportedOperationException("StateStores can't access system time.");
    }

    @Override
    public long currentStreamTimeMs() {
        throw new UnsupportedOperationException("StateStores can't access stream time.");
    }
}
