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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.CompositeRestoreListener;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;

public class MockInternalProcessorContext extends MockProcessorContext implements InternalProcessorContext {

    private final Map<String, StateRestoreCallback> restoreCallbacks = new LinkedHashMap<>();
    private final Map<String, StateRestoreCallback> restoreFuncs = new HashMap<>();
    private final ThreadCache threadCache;
    private final StreamsMetricsImpl metrics;
    private ProcessorNode currentNode;
    private RecordCollector recordCollector;

    public MockInternalProcessorContext() {
        super();
        threadCache = null;
        metrics = (StreamsMetricsImpl) super.metrics();
    }

    public MockInternalProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        super(config, taskId, stateDir);
        threadCache = null;
        metrics = (StreamsMetricsImpl) super.metrics();
    }

    public MockInternalProcessorContext(final Properties config, final TaskId taskId, final LogContext logContext, final long maxCacheSizeBytes) {
        super(config, taskId, createStateDir(config));
        metrics = (StreamsMetricsImpl) super.metrics();
        metrics.setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        threadCache = new ThreadCache(logContext, maxCacheSizeBytes, metrics);
    }

    public MockInternalProcessorContext(final Properties config, final TaskId taskId, final Metrics metrics) {
        super(config, taskId, createStateDir(config));
        this.metrics = new StreamsMetricsImpl(metrics, "client-id", config.getProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG));
        setCurrentNode(new ProcessorNode<>("TESTING_NODE"));
        threadCache = null;
    }

    private static File createStateDir(Properties config) {
        return new File(new QuietStreamsConfig(config).getString(StreamsConfig.STATE_DIR_CONFIG));
    }

    @Override
    public StreamsMetricsImpl metrics() {
        return metrics;
    }

    @Override
    public ProcessorRecordContext recordContext() {
        return new ProcessorRecordContext(timestamp(), offset(), partition(), topic(), headers());
    }

    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        setRecordMetadata(
            recordContext.topic(),
            recordContext.partition(),
            recordContext.offset(),
            recordContext.headers(),
            recordContext.timestamp()
        );
    }

    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode currentNode() {
        return currentNode;
    }

    @Override
    public ThreadCache getCache() {
        return threadCache;
    }

    @Override
    public void initialize() {}

    @Override
    public void uninitialize() {}

    @Override
    public RecordCollector recordCollector() {
        return recordCollector;
    }

    public void setRecordCollector(final RecordCollector recordCollector) {
        this.recordCollector = recordCollector;
    }

    @Override
    public void register(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
        restoreCallbacks.put(store.name(), stateRestoreCallback);
        restoreFuncs.put(store.name(), stateRestoreCallback);
        super.register(store, stateRestoreCallback);
    }

    public StateRestoreCallback stateRestoreCallback(final String storeName) {
        return restoreCallbacks.get(storeName);
    }

    public StateRestoreListener getRestoreListener(final String storeName) {
        return getStateRestoreListener(restoreFuncs.get(storeName));
    }

    public void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog) {
        final RecordBatchingStateRestoreCallback restoreCallback = adapt(restoreFuncs.get(storeName));
        final StateRestoreListener restoreListener = getRestoreListener(storeName);

        restoreListener.onRestoreStart(null, storeName, 0L, 0L);

        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (final KeyValue<byte[], byte[]> keyValue : changeLog) {
            records.add(new ConsumerRecord<>("", 0, 0L, keyValue.key, keyValue.value));
        }

        restoreCallback.restoreBatch(records);

        restoreListener.onRestoreEnd(null, storeName, 0L);
    }

    private StateRestoreListener getStateRestoreListener(final StateRestoreCallback restoreCallback) {
        if (restoreCallback instanceof StateRestoreListener) {
            return (StateRestoreListener) restoreCallback;
        }

        return CompositeRestoreListener.NO_OP_STATE_RESTORE_LISTENER;
    }
}