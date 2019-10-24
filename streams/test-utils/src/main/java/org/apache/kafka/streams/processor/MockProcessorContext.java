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
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.CompositeRestoreListener;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.ToInternal;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;

/**
 * {@link MockProcessorContext} is a mock of {@link ProcessorContext} for users to test their {@link Processor},
 * {@link Transformer}, and {@link ValueTransformer} implementations.
 * <p>
 * The tests for this class (org.apache.kafka.streams.MockProcessorContextTest) include several behavioral
 * tests that serve as example usage.
 * <p>
 * Note that this class does not take any automated actions (such as firing scheduled punctuators).
 * It simply captures any data it witnesses.
 * If you require more automated tests, we recommend wrapping your {@link Processor} in a minimal source-processor-sink
 * {@link Topology} and using the {@link TopologyTestDriver}.
 */
public class MockProcessorContext extends AbstractProcessorContext implements ProcessorContext,
        RecordCollector.Supplier {
    // Immutable fields ================================================
    private final File stateDir;
    private static final int DEFAULT_PARTITION = -1;
    private static final long DEFAULT_OFFSET = -1L;
    private static final long DEFAULT_TIMESTAMP = -1L;

    // mocks ================================================
    private final Map<String, StateStore> stateStores = new HashMap<>();
    private final List<CapturedPunctuator> punctuators = new LinkedList<>();
    private final Map<String, StateRestoreCallback> restoreFuncs = new HashMap<>();
    private final List<CapturedForward> capturedForwards = new LinkedList<>();
    private boolean committed = false;
    private final ToInternal toInternal = new ToInternal();

    /**
     * {@link CapturedPunctuator} holds captured punctuators, along with their scheduling information.
     */
    public static class CapturedPunctuator {
        private final long intervalMs;
        private final PunctuationType type;
        private final Punctuator punctuator;
        private boolean cancelled = false;

        private CapturedPunctuator(final long intervalMs, final PunctuationType type, final Punctuator punctuator) {
            this.intervalMs = intervalMs;
            this.type = type;
            this.punctuator = punctuator;
        }

        public long getIntervalMs() {
            return intervalMs;
        }

        public PunctuationType getType() {
            return type;
        }

        public Punctuator getPunctuator() {
            return punctuator;
        }

        @SuppressWarnings({"WeakerAccess", "unused"})
        public void cancel() {
            cancelled = true;
        }

        public boolean cancelled() {
            return cancelled;
        }
    }


    public static class CapturedForward {
        private final String childName;
        private final long timestamp;
        private final KeyValue keyValue;

        private CapturedForward(final To to, final KeyValue keyValue) {
            if (keyValue == null) {
                throw new IllegalArgumentException();
            }

            this.childName = to.childName;
            this.timestamp = to.timestamp;
            this.keyValue = keyValue;
        }

        /**
         * The child this data was forwarded to.
         *
         * @return The child name, or {@code null} if it was broadcast.
         */
        public String childName() {
            return childName;
        }

        /**
         * The timestamp attached to the forwarded record.
         *
         * @return A timestamp, or {@code -1} if none was forwarded.
         */
        public long timestamp() {
            return timestamp;
        }

        /**
         * The data forwarded.
         *
         * @return A key/value pair. Not null.
         */
        public KeyValue keyValue() {
            return keyValue;
        }

        @Override
        public String toString() {
            return "CapturedForward{" +
                "childName='" + childName + '\'' +
                ", timestamp=" + timestamp +
                ", keyValue=" + keyValue +
                '}';
        }
    }

    // constructors ================================================

    /**
     * Create a {@link MockProcessorContext} with dummy {@code config} and {@code taskId} and {@code null} {@code stateDir}.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link InMemoryKeyValueStore}, so the stateDir won't matter.
     */
    public MockProcessorContext() {
        //noinspection DoubleBraceInitialization
        this(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, "");
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                }
            });
    }

    /**
     * Create a {@link MockProcessorContext} with dummy {@code taskId} and {@code null} {@code stateDir}.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link InMemoryKeyValueStore}, so the stateDir won't matter.
     *
     * @param config a Properties object, used to configure the context and the processor.
     */
    public MockProcessorContext(final Properties config) {
        this(config, createTaskId(), null);
    }

    /**
     * Create a {@link MockProcessorContext} with a specified taskId and null stateDir.
     *
     * @param config   a {@link Properties} object, used to configure the context and the processor.
     * @param taskId   a {@link TaskId}, which the context makes available via {@link MockProcessorContext#taskId()}.
     * @param stateDir a {@link File}, which the context makes available via {@link MockProcessorContext#stateDir()}.
     */
    public MockProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        this(
                taskId,
                stateDir,
                createStreamsMetrics(createStreamsConfig(config)),
                createStreamsConfig(config),
                null
        );
    }

    public MockProcessorContext(final TaskId taskId,
                                final File stateDir,
                                final StreamsMetricsImpl metrics,
                                final StreamsConfig config,
                                final ThreadCache cache) {
        super(taskId, config, metrics, null, cache);
        this.stateDir = stateDir;
        setRecordContext(createDefaultProcessorRecordedContext());
        ThreadMetrics.skipRecordSensor(Thread.currentThread().getName(), metrics());
    }

    protected static TaskId createTaskId() {
        return new TaskId(0, 0);
    }

    private static StreamsMetricsImpl createStreamsMetrics(final StreamsConfig streamsConfig) {
        final MetricConfig metricConfig = new MetricConfig();
        metricConfig.recordLevel(Sensor.RecordingLevel.DEBUG);
        final String threadId = Thread.currentThread().getName();
        return new StreamsMetricsImpl(new Metrics(metricConfig), threadId,
                streamsConfig.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG));
    }

    private static StreamsConfig createStreamsConfig(final Properties config) {
        return new QuietStreamsConfig(config);
    }

    private static ProcessorRecordContext createDefaultProcessorRecordedContext() {
        return new ProcessorRecordContext(DEFAULT_TIMESTAMP, DEFAULT_OFFSET,
                DEFAULT_PARTITION, null, new RecordHeaders());
    }

    @Override
    public File stateDir() {
        if (stateDir == null) {
            throw new UnsupportedOperationException("State directory not specified");
        }
        return stateDir;
    }

    // settable record metadata ================================================

    /**
     * The context exposes these metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set them directly.
     *
     * @param topic     A topic name
     * @param partition A partition number
     * @param offset    A record offset
     * @param timestamp A record timestamp
     */
    public void setRecordMetadata(final String topic,
                                  final int partition,
                                  final long offset,
                                  final Headers headers,
                                  final long timestamp) {
        setRecordContext(new ProcessorRecordContext(timestamp, offset, partition, topic, headers));
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param topic A topic name
     */
    public void setTopic(final String topic) {
        setRecordMetadata(
                topic,
                this.recordContext.partition(),
                this.recordContext.offset(),
                this.recordContext.headers(),
                this.recordContext.timestamp()
        );
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param partition A partition number
     */
    public void setPartition(final int partition) {
        setRecordMetadata(
                this.recordContext.topic(),
                partition,
                this.recordContext.offset(),
                this.recordContext.headers(),
                this.recordContext.timestamp()
        );
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param offset A record offset
     */
    public void setOffset(final long offset) {
        setRecordMetadata(
                this.recordContext.topic(),
                this.recordContext.partition(),
                offset,
                this.recordContext.headers(),
                this.recordContext.timestamp()
        );
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param headers Record headers
     */
    public void setHeaders(final Headers headers) {
        setRecordMetadata(
                this.recordContext.topic(),
                this.recordContext.partition(),
                this.recordContext.offset(),
                headers,
                this.recordContext.timestamp()
        );
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param timestamp A record timestamp
     */
    public void setTimestamp(final long timestamp) {
        setRecordMetadata(
                this.recordContext.topic(),
                this.recordContext.partition(),
                this.recordContext.offset(),
                this.recordContext.headers(),
                timestamp
        );
    }

    @Override
    public String topic() {
        if (recordContext().topic() == null) {
            throw new IllegalStateException("Topic must be set before use via setRecordMetadata() or setTopic().");
        }
        return recordContext.topic();
    }

    @Override
    public int partition() {
        if (recordContext().partition() == DEFAULT_PARTITION) {
            throw new IllegalStateException("Partition must be set before use via setRecordMetadata() or setPartition().");
        }
        return recordContext().partition();
    }

    @Override
    public long offset() {
        if (recordContext().offset() == DEFAULT_OFFSET) {
            throw new IllegalStateException("Offset must be set before use via setRecordMetadata() or setOffset().");
        }
        return recordContext().offset();
    }

    @Override
    public Headers headers() {
        return recordContext().headers();
    }

    @Override
    public long timestamp() {
        if (recordContext().timestamp() == DEFAULT_TIMESTAMP) {
            throw new IllegalStateException("Timestamp must be set before use via setRecordMetadata() or setTimestamp().");
        }
        return recordContext().timestamp();
    }

    // mocks ================================================

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallbackIsIgnoredInMock) {
        stateStores.put(store.name(), store);
        restoreFuncs.put(store.name(), stateRestoreCallbackIsIgnoredInMock);
    }

    @Override
    public StateStore getStateStore(final String name) {
        return stateStores.get(name);
    }

    @Override
    @Deprecated
    public Cancellable schedule(final long intervalMs,
                                final PunctuationType type,
                                final Punctuator callback) {
        final CapturedPunctuator capturedPunctuator = new CapturedPunctuator(intervalMs, type, callback);

        punctuators.add(capturedPunctuator);

        return capturedPunctuator::cancel;
    }

    @SuppressWarnings("deprecation") // removing #schedule(final long intervalMs,...) will fix this
    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        return schedule(ApiUtils.validateMillisecondDuration(interval, "interval"), type, callback);
    }

    /**
     * Get the punctuators scheduled so far. The returned list is not affected by subsequent calls to {@code schedule(...)}.
     *
     * @return A list of captured punctuators.
     */
    public List<CapturedPunctuator> scheduledPunctuators() {
        return new LinkedList<>(punctuators);
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        forward(key, value, To.all());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        toInternal.update(to);
        if (toInternal.hasTimestamp()) {
            setTimestamp(toInternal.timestamp());
        }
        final ProcessorNode thisNode = currentNode;
        try {
            for (final ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
                if (toInternal.child() == null || toInternal.child().equals(childNode.name())) {
                    currentNode = childNode;
                    childNode.process(key, value);
                    toInternal.update(to); // need to reset because MockProcessorContext is shared over multiple Processors and toInternal might have been modified
                }
            }
        } finally {
            currentNode = thisNode;
        }

        capturedForwards.add(
            new CapturedForward(
                to.timestamp == -1 ?
                        to.withTimestamp(recordContext().timestamp()) : to,
                new KeyValue(key, value)
            )
        );
    }

    @Override
    @Deprecated
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        throw new UnsupportedOperationException(
            "Forwarding to a child by index is deprecated. " +
                "Please transition processors to forward using a 'To' object instead."
        );
    }

    @Override
    @Deprecated
    public <K, V> void forward(final K key, final V value, final String childName) {
        throw new UnsupportedOperationException(
            "Forwarding to a child by name is deprecated. " +
                "Please transition processors to forward using 'To.child(childName)' instead."
        );
    }

    /**
     * Get all the forwarded data this context has observed. The returned list will not be
     * affected by subsequent interactions with the context. The data in the list is in the same order as the calls to
     * {@code forward(...)}.
     *
     * @return A list of key/value pairs that were previously passed to the context.
     */
    public List<CapturedForward> forwarded() {
        return new LinkedList<>(capturedForwards);
    }

    /**
     * Get all the forwarded data this context has observed for a specific child by name.
     * The returned list will not be affected by subsequent interactions with the context.
     * The data in the list is in the same order as the calls to {@code forward(...)}.
     *
     * @param childName The child name to retrieve forwards for
     * @return A list of key/value pairs that were previously passed to the context.
     */
    public List<CapturedForward> forwarded(final String childName) {
        final LinkedList<CapturedForward> result = new LinkedList<>();
        for (final CapturedForward capture : capturedForwards) {
            if (capture.childName() == null || capture.childName().equals(childName)) {
                result.add(capture);
            }
        }
        return result;
    }

    /**
     * Clear the captured forwarded data.
     */
    public void resetForwards() {
        capturedForwards.clear();
    }

    @Override
    public void commit() {
        committed = true;
    }

    /**
     * Whether {@link ProcessorContext#commit()} has been called in this context.
     *
     * @return {@code true} iff {@link ProcessorContext#commit()} has been called in this context since construction or reset.
     */
    public boolean committed() {
        return committed;
    }

    /**
     * Reset the commit capture to {@code false} (whether or not it was previously {@code true}).
     */
    public void resetCommit() {
        committed = false;
    }

    @Override
    public RecordCollector recordCollector() {
        // This interface is assumed by state stores that add change-logging.
        // Rather than risk a mysterious ClassCastException during unit tests, throw an explanatory exception.

        throw new UnsupportedOperationException(
            "MockProcessorContext does not provide record collection. " +
                "For processor unit tests, use an in-memory state store with change-logging disabled. " +
                "Alternatively, use the TopologyTestDriver for testing processor/store/topology integration."
        );
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
