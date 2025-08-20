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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.processor.internals.ClientUtils;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * {@link MockProcessorContext} is a mock of {@link ProcessorContext} for users to test their
 * {@link org.apache.kafka.streams.kstream.ValueTransformer} implementations.
 * <p>
 * The tests for this class (org.apache.kafka.streams.MockProcessorContextTest) include several behavioral
 * tests that serve as example usage.
 * <p>
 * Note that this class does not take any automated actions (such as firing scheduled punctuators).
 * It simply captures any data it witnesses.
 *
 * @deprecated Since 4.0. Use {@link org.apache.kafka.streams.processor.api.MockProcessorContext} instead.
 */
@Deprecated
public class MockProcessorContext implements ProcessorContext, RecordCollector.Supplier {
    // Immutable fields ================================================
    private final StreamsMetricsImpl metrics;
    private final TaskId taskId;
    private final StreamsConfig config;
    private final File stateDir;

    // settable record metadata ================================================
    private String topic;
    private Integer partition;
    private Long offset;
    private Headers headers;
    private Long recordTimestamp;
    private Long currentSystemTimeMs;
    private Long currentStreamTimeMs;

    // mocks ================================================
    private final Map<String, StateStore> stateStores = new HashMap<>();
    private final List<CapturedPunctuator> punctuators = new LinkedList<>();
    private final List<CapturedForward> capturedForwards = new LinkedList<>();
    private boolean committed = false;


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

        @SuppressWarnings({"WeakerAccess", "unused"})
        public long getIntervalMs() {
            return intervalMs;
        }

        @SuppressWarnings({"WeakerAccess", "unused"})
        public PunctuationType getType() {
            return type;
        }

        @SuppressWarnings({"WeakerAccess", "unused"})
        public Punctuator getPunctuator() {
            return punctuator;
        }

        @SuppressWarnings({"WeakerAccess", "unused"})
        public void cancel() {
            cancelled = true;
        }

        @SuppressWarnings({"WeakerAccess", "unused"})
        public boolean cancelled() {
            return cancelled;
        }
    }


    public static class CapturedForward {
        private final String childName;
        private final long timestamp;
        private final Headers headers;
        private final KeyValue<?, ?> keyValue;

        private CapturedForward(final KeyValue<?, ?> keyValue, final To to, final Headers headers) {
            if (keyValue == null) {
                throw new IllegalArgumentException();
            }

            this.childName = to.childName;
            this.timestamp = to.timestamp;
            this.keyValue = keyValue;
            this.headers = headers;
        }

        /**
         * The child this data was forwarded to.
         *
         * @return The child name, or {@code null} if it was broadcast.
         */
        @SuppressWarnings({"WeakerAccess", "unused"})
        public String childName() {
            return childName;
        }

        /**
         * The timestamp attached to the forwarded record.
         *
         * @return A timestamp, or {@code -1} if none was forwarded.
         */
        @SuppressWarnings({"WeakerAccess", "unused"})
        public long timestamp() {
            return timestamp;
        }

        /**
         * The data forwarded.
         *
         * @return A key/value pair. Not null.
         */
        @SuppressWarnings({"WeakerAccess", "unused", "rawtypes"})
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

        public Headers headers() {
            return this.headers;
        }
    }

    // constructors ================================================

    /**
     * Create a {@link MockProcessorContext} with dummy {@code config} and {@code taskId} and {@code null} {@code stateDir}.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link InMemoryKeyValueStore}, so the stateDir won't matter.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public MockProcessorContext() {
        //noinspection DoubleBraceInitialization
        this(
            new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, "");
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                }
            },
            new TaskId(0, 0),
            null);
    }

    /**
     * Create a {@link MockProcessorContext} with dummy {@code taskId} and {@code null} {@code stateDir}.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link InMemoryKeyValueStore}, so the stateDir won't matter.
     *
     * @param config a Properties object, used to configure the context and the processor.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public MockProcessorContext(final Properties config) {
        this(config, new TaskId(0, 0), null);
    }

    /**
     * Create a {@link MockProcessorContext} with a specified taskId and null stateDir.
     *
     * @param config   a {@link Properties} object, used to configure the context and the processor.
     * @param taskId   a {@link TaskId}, which the context makes available via {@link MockProcessorContext#taskId()}.
     * @param stateDir a {@link File}, which the context makes available viw {@link MockProcessorContext#stateDir()}.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public MockProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        final Properties configCopy = new Properties();
        configCopy.putAll(config);
        configCopy.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-bootstrap-host:0");
        configCopy.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-mock-app-id");
        final StreamsConfig streamsConfig = new ClientUtils.QuietStreamsConfig(configCopy);
        this.taskId = taskId;
        this.config = streamsConfig;
        this.stateDir = stateDir;
        final MetricConfig metricConfig = new MetricConfig();
        metricConfig.recordLevel(Sensor.RecordingLevel.DEBUG);
        final String threadId = Thread.currentThread().getName();
        this.metrics = new StreamsMetricsImpl(
                new Metrics(metricConfig),
                threadId,
                "processId",
                Time.SYSTEM
        );
        TaskMetrics.droppedRecordsSensor(threadId, taskId.toString(), metrics);
    }

    @Override
    public String applicationId() {
        return config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
    }

    @Override
    public TaskId taskId() {
        return taskId;
    }

    @Override
    public Map<String, Object> appConfigs() {
        final Map<String, Object> combined = new HashMap<>();
        combined.putAll(config.originals());
        combined.putAll(config.values());
        return combined;
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return config.originalsWithPrefix(prefix);
    }

    @Override
    public long currentSystemTimeMs() {
        if (currentSystemTimeMs == null) {
            throw new IllegalStateException("System time must be set before use via setCurrentSystemTimeMs().");
        }
        return currentSystemTimeMs;
    }

    @Override
    public long currentStreamTimeMs() {
        if (currentStreamTimeMs == null) {
            throw new IllegalStateException("Stream time must be set before use via setCurrentStreamTimeMs().");
        }
        return currentStreamTimeMs;
    }

    @Override
    public Serde<?> keySerde() {
        return config.defaultKeySerde();
    }

    @Override
    public Serde<?> valueSerde() {
        return config.defaultValueSerde();
    }

    @Override
    public File stateDir() {
        return stateDir;
    }

    @Override
    public StreamsMetrics metrics() {
        return metrics;
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
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void setRecordMetadata(final String topic,
                                  final int partition,
                                  final long offset,
                                  final Headers headers,
                                  final long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.headers = headers;
        this.recordTimestamp = timestamp;
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param topic A topic name
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void setTopic(final String topic) {
        this.topic = topic;
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param partition A partition number
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void setPartition(final int partition) {
        this.partition = partition;
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param offset A record offset
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void setOffset(final long offset) {
        this.offset = offset;
    }

    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param headers Record headers
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void setHeaders(final Headers headers) {
        this.headers = headers;
    }


    /**
     * The context exposes this metadata for use in the processor. Normally, they are set by the Kafka Streams framework,
     * but for the purpose of driving unit tests, you can set it directly. Setting this attribute doesn't affect the others.
     *
     * @param recordTimestamp A record timestamp
     */
    @SuppressWarnings({"WeakerAccess"})
    public void setRecordTimestamp(final long recordTimestamp) {
        this.recordTimestamp = recordTimestamp;
    }

    public void setCurrentSystemTimeMs(final long currentSystemTimeMs) {
        this.currentSystemTimeMs = currentSystemTimeMs;
    }

    public void setCurrentStreamTimeMs(final long currentStreamTimeMs) {
        this.currentStreamTimeMs = currentStreamTimeMs;
    }

    @Override
    public String topic() {
        if (topic == null) {
            throw new IllegalStateException("Topic must be set before use via setRecordMetadata() or setTopic().");
        }
        return topic;
    }

    @Override
    public int partition() {
        if (partition == null) {
            throw new IllegalStateException("Partition must be set before use via setRecordMetadata() or setPartition().");
        }
        return partition;
    }

    @Override
    public long offset() {
        if (offset == null) {
            throw new IllegalStateException("Offset must be set before use via setRecordMetadata() or setOffset().");
        }
        return offset;
    }

    /**
     * Returns the headers of the current input record; could be {@code null} if it is not
     * available.
     *
     * <p> Note, that headers should never be {@code null} in the actual Kafka Streams runtime,
     * even if they could be empty. However, this mock does not guarantee non-{@code null} headers.
     * Thus, you either need to add a {@code null} check to your production code to use this mock
     * for testing, or you always need to set headers manually via {@link #setHeaders(Headers)} to
     * avoid a {@link NullPointerException} from your {@link org.apache.kafka.streams.kstream.ValueTransformer}implementation.
     *
     * @return the headers
     */
    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public long timestamp() {
        if (recordTimestamp == null) {
            throw new IllegalStateException("Timestamp must be set before use via setRecordMetadata() or setRecordTimestamp().");
        }
        return recordTimestamp;
    }

    // mocks ================================================

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallbackIsIgnoredInMock) {
        stateStores.put(store.name(), store);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        return (S) stateStores.get(name);
    }

    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        final long intervalMs = ApiUtils.validateMillisecondDuration(interval, "interval");
        if (intervalMs < 1) {
            throw new IllegalArgumentException("The minimum supported scheduling interval is 1 millisecond.");
        }
        final CapturedPunctuator capturedPunctuator = new CapturedPunctuator(intervalMs, type, callback);

        punctuators.add(capturedPunctuator);

        return capturedPunctuator::cancel;
    }

    /**
     * Get the punctuators scheduled so far. The returned list is not affected by subsequent calls to {@code schedule(...)}.
     *
     * @return A list of captured punctuators.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<CapturedPunctuator> scheduledPunctuators() {
        return new LinkedList<>(punctuators);
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        forward(key, value, To.all());
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        capturedForwards.add(
            new CapturedForward(
                new KeyValue<>(key, value),
                to.timestamp == -1 ? to.withTimestamp(recordTimestamp == null ? -1 : recordTimestamp) : to,
                headers
            )
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
    @SuppressWarnings({"WeakerAccess", "unused"})
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
    @SuppressWarnings({"WeakerAccess", "unused"})
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
    @SuppressWarnings("WeakerAccess")
    public boolean committed() {
        return committed;
    }

    /**
     * Reset the commit capture to {@code false} (whether or not it was previously {@code true}).
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
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
}
