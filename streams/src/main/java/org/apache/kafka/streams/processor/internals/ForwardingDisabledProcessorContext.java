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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * {@code ProcessorContext} implementation that will throw on any forward call.
 */
public final class ForwardingDisabledProcessorContext implements ProcessorContext {
    private final ProcessorContext delegate;

    private static final String EXPLANATION = "ProcessorContext#forward() is not supported from this context, "
        + "as the framework must ensure the key is not changed (#forward allows changing the key on "
        + "messages which are sent). Try another function, which doesn't allow the key to be changed "
        + "(for example - #tranformValues).";

    public ForwardingDisabledProcessorContext(final ProcessorContext delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
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
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
        delegate.register(store, stateRestoreCallback);
    }

    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        return delegate.getStateStore(name);
    }

    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        return delegate.schedule(interval, type, callback);
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        throw new StreamsException(EXPLANATION);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        throw new StreamsException(EXPLANATION);
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

    @Override
    public long currentSystemTimeMs() {
        return delegate.currentSystemTimeMs();
    }

    @Override
    public long currentStreamTimeMs() {
        return delegate.currentStreamTimeMs();
    }
}
