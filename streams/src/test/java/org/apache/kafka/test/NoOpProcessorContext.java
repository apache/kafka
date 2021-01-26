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
package org.apache.kafka.test;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.StateManager;
import org.apache.kafka.streams.processor.internals.StateManagerStub;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

public class NoOpProcessorContext extends AbstractProcessorContext {
    public boolean initialized;
    @SuppressWarnings("WeakerAccess")
    public Map<Object, Object> forwardedValues = new HashMap<>();

    public NoOpProcessorContext() {
        super(new TaskId(1, 1), streamsConfig(), new MockStreamsMetrics(new Metrics()), null);
    }

    private static StreamsConfig streamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "boot");
        return new StreamsConfig(props);
    }

    @Override
    protected StateManager stateManager() {
        return new StateManagerStub();
    }

    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        return null;
    }

    @Override
    @Deprecated
    public Cancellable schedule(final long interval,
                                final PunctuationType type,
                                final Punctuator callback) {
        return null;
    }

    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        return null;
    }

    @Override
    public <K, V> void forward(final Record<K, V> record) {
        forward(record.key(), record.value());
    }

    @Override
    public <K, V> void forward(final Record<K, V> record, final String childName) {
        forward(record.key(), record.value());
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        forwardedValues.put(key, value);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        forward(key, value);
    }

    @Override
    @Deprecated
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        forward(key, value);
    }

    @Override
    @Deprecated
    public <K, V> void forward(final K key, final V value, final String childName) {
        forward(key, value);
    }

    @Override
    public void commit() {}

    @Override
    public void initialize() {
        initialized = true;
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
    }

    @Override
    public TaskType taskType() {
        return TaskType.ACTIVE;
    }

    @Override
    public void logChange(final String storeName,
                          final Bytes key,
                          final byte[] value,
                          final long timestamp) {
    }

    @Override
    public void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache) {
    }

    @Override
    public void transitionToStandby(final ThreadCache newCache) {
    }

    @Override
    public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
        cache.addDirtyEntryFlushListener(namespace, listener);
    }

    @Override
    public String changelogFor(final String storeName) {
        return ProcessorStateManager.storeChangelogTopic(applicationId(), storeName);
    }
}
