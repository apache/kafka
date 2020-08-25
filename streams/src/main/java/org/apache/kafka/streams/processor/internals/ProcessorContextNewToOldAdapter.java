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
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;

import java.io.File;
import java.time.Duration;
import java.util.Map;

public final class ProcessorContextNewToOldAdapter implements org.apache.kafka.streams.processor.ProcessorContext {
    private final ProcessorContext<Object, Object> delegate;

    @SuppressWarnings("unchecked")
    public static org.apache.kafka.streams.processor.ProcessorContext adapt(final ProcessorContext<?, ?> delegate) {
        if (delegate instanceof InternalProcessorContextOldToNewAdapter) {
            return ((InternalProcessorContextOldToNewAdapter<?, ?>) delegate).delegate();
        } else if (delegate instanceof InternalApiProcessorContext) {
            return InternalProcessorContextNewToOldAdapter.adapt((InternalApiProcessorContext<Object, Object>) delegate);
        } else {
            return new ProcessorContextNewToOldAdapter(delegate);
        }
    }

    @SuppressWarnings("unchecked")
    private ProcessorContextNewToOldAdapter(final ProcessorContext<?, ?> delegate) {
        this.delegate = (ProcessorContext<Object, Object>) delegate;
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
        throw new UnsupportedOperationException("The supplied ProcsesorContext doesn't support registering stateRestoreCallbacks");
    }

    @Override
    public StateStore getStateStore(final String name) {
        return delegate.getStateStore(name);
    }

    @Deprecated
    @Override
    public Cancellable schedule(final long intervalMs, final PunctuationType type, final Punctuator callback) {
        return delegate.schedule(Duration.ofMillis(intervalMs), type, callback);
    }

    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        return delegate.schedule(interval, type, callback);
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        delegate.forward(key, value);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        delegate.forward(key, value, to);
    }

    @Deprecated
    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        // Note, we'll only fall through to here if the delegate does _not_ implement InternalApiProcessorContext,
        // so only for mocks and such.
        throw new UnsupportedOperationException(
            "forward(key, value, index) has been deprecated since 2.0.0, and is not supported by this ProcessorContext."
        );
    }

    @Deprecated
    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        delegate.forward(key, value, To.child(childName));
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public String topic() {
        return delegate.topic();
    }

    @Override
    public int partition() {
        return delegate.partition();
    }

    @Override
    public long offset() {
        return delegate.offset();
    }

    @Override
    public Headers headers() {
        return delegate.headers();
    }

    @Override
    public long timestamp() {
        return delegate.timestamp();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return delegate.appConfigs();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return delegate.appConfigsWithPrefix(prefix);
    }
}
