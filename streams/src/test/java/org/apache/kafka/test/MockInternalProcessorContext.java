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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
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
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;

@SuppressWarnings("rawtypes")
public class MockInternalProcessorContext extends MockProcessorContext implements InternalProcessorContext<Object, Object> {

    public static final TaskId DEFAULT_TASK_ID = new TaskId(0, 0);
    public static final RecordHeaders DEFAULT_HEADERS = new RecordHeaders();
    public static final String DEFAULT_TOPIC = "";
    public static final int DEFAULT_PARTITION = 0;
    public static final long DEFAULT_OFFSET = 0L;
    public static final long DEFAULT_TIMESTAMP = 0L;
    public static final String DEFAULT_CLIENT_ID = "client-id";
    public static final String DEFAULT_THREAD_CACHE_PREFIX = "testCache ";
    public static final String DEFAULT_PROCESSOR_NODE_NAME = "TESTING_NODE";
    public static final int DEFAULT_MAX_CACHE_SIZE_BYTES = 0;
    public static final String DEFAULT_METRICS_VERSION = StreamsConfig.METRICS_LATEST;

    private final Map<String, StateRestoreCallback> restoreCallbacks = new LinkedHashMap<>();
    private ThreadCache threadCache;
    private ProcessorNode currentNode;
    private StreamsMetricsImpl metrics;
    private RecordCollector recordCollector;

    public MockInternalProcessorContext() {
        super(StreamsTestUtils.getStreamsConfig(), DEFAULT_TASK_ID, TestUtils.tempDirectory());
        final StreamsMetricsImpl metrics = (StreamsMetricsImpl) super.metrics();
        init(metrics, new ThreadCache(new LogContext(DEFAULT_THREAD_CACHE_PREFIX), DEFAULT_MAX_CACHE_SIZE_BYTES, metrics));
    }

    public MockInternalProcessorContext(final LogContext logContext, final long maxCacheSizeBytes) {
        super(StreamsTestUtils.getStreamsConfig(), DEFAULT_TASK_ID, TestUtils.tempDirectory());
        final StreamsMetricsImpl streamsMetrics = (StreamsMetricsImpl) super.metrics();
        final ThreadCache threadCache = new ThreadCache(logContext, maxCacheSizeBytes, streamsMetrics);
        init(streamsMetrics, threadCache);
    }

    public MockInternalProcessorContext(final Properties config, final Metrics metrics) {
        super(config, DEFAULT_TASK_ID, createStateDir(config));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, DEFAULT_CLIENT_ID, getMetricsVersion(config));
        final ThreadCache threadCache = new ThreadCache(new LogContext(DEFAULT_THREAD_CACHE_PREFIX), DEFAULT_MAX_CACHE_SIZE_BYTES, streamsMetrics);
        init(streamsMetrics, threadCache);
    }

    public MockInternalProcessorContext(final Properties config, final File stateDir, final ThreadCache cache) {
        super(config, DEFAULT_TASK_ID, stateDir);
        init((StreamsMetricsImpl) super.metrics(), cache);
    }

    private void init(final StreamsMetricsImpl metrics, final ThreadCache threadCache) {
        this.metrics = metrics;
        metrics.setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        setCurrentNode(new ProcessorNode<>(DEFAULT_PROCESSOR_NODE_NAME));
        this.threadCache = threadCache;
        setRecordCollector(new MockRecordCollector());
        setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, DEFAULT_OFFSET, DEFAULT_PARTITION, DEFAULT_TOPIC, DEFAULT_HEADERS));
        TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), DEFAULT_TASK_ID.toString(), metrics);
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
    public void setCurrentNode(final ProcessorNode<?, ?> currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode<?, ?> currentNode() {
        return currentNode;
    }

    @Override
    public ThreadCache getCache() {
        return threadCache;
    }

    @Override
    public void initialize() {
    }

    @Override
    public void uninitialize() {
    }

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
        super.register(store, stateRestoreCallback);
    }

    public StateRestoreCallback stateRestoreCallback(final String storeName) {
        return restoreCallbacks.get(storeName);
    }

    public StateRestoreListener getRestoreListener(final String storeName) {
        final StateRestoreCallback restoreCallback = restoreCallbacks.get(storeName);
        if (restoreCallback instanceof StateRestoreListener) {
            return (StateRestoreListener) restoreCallback;
        }
        return CompositeRestoreListener.NO_OP_STATE_RESTORE_LISTENER;
    }

    public void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog) {
        final RecordBatchingStateRestoreCallback restoreCallback = adapt(restoreCallbacks.get(storeName));
        final StateRestoreListener restoreListener = getRestoreListener(storeName);

        restoreListener.onRestoreStart(null, storeName, DEFAULT_OFFSET, DEFAULT_OFFSET);

        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (final KeyValue<byte[], byte[]> keyValue : changeLog) {
            records.add(new ConsumerRecord<>(DEFAULT_TOPIC, DEFAULT_PARTITION, DEFAULT_OFFSET, keyValue.key, keyValue.value));
        }

        restoreCallback.restoreBatch(records);

        restoreListener.onRestoreEnd(null, storeName, 0L);
    }

    private static File createStateDir(final Properties config) {
        if (config.containsKey(StreamsConfig.STATE_DIR_CONFIG)) {
            return new File(config.getProperty(StreamsConfig.STATE_DIR_CONFIG));
        }
        return TestUtils.tempDirectory();
    }

    private static String getMetricsVersion(final Properties config) {
        return config.getProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, DEFAULT_METRICS_VERSION);
    }
}