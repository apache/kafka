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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;

/**
 * {@link MockFixedKeyProcessorContext} is a mock {@link FixedKeyProcessorContext} for users to test their {@link FixedKeyProcessor},
 * {@link ValueTransformer}, and {@link org.apache.kafka.streams.kstream.ValueTransformerWithKey} implementations, this class is an
 * extension of the {@link MockProcessorContext}.
 * <p>
 * The tests for this class (org.apache.kafka.streams.FixedKeyMockProcessorContextTest) include several behavioral
 * tests that serve as example usage.
 * <p>
 * Note that this class does not take any automated actions (such as firing scheduled punctuators).
 * It simply captures any data it witnesses.
 * If you require more automated tests, we recommend wrapping your {@link FixedKeyProcessor} in a minimal source-processor-sink
 * {@link Topology} and using the {@link TopologyTestDriver}.
 */
public class MockFixedKeyProcessorContext<KForward, VForward>  implements
        FixedKeyProcessorContext<KForward, VForward>, RecordCollector.Supplier {
    // Immutable fields ================================================
    private final StreamsMetricsImpl metrics;
    private final TaskId taskId;
    private final StreamsConfig config;
    private final File stateDir;

    // settable record metadata ================================================
    private MockRecordMetadata recordMetadata;
    private Long currentSystemTimeMs;
    private Long currentStreamTimeMs;

    // mocks ================================================
    private final Map<String, StateStore> stateStores = new HashMap<>();
    private final List<CapturedPunctuator> punctuators = new LinkedList<>();
    private final List<CapturedForward<? extends KForward, ? extends VForward>> capturedForwards = new LinkedList<>();
    private boolean committed = false;

    private static final class MockRecordMetadata implements RecordMetadata {
        private final String topic;
        private final int partition;
        private final long offset;

        private MockRecordMetadata(final String topic, final int partition, final long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public String topic() {
            return topic;
        }

        @Override
        public int partition() {
            return partition;
        }

        @Override
        public long offset() {
            return offset;
        }
    }

/**
 * {@link MockFixedKeyProcessorContext.CapturedPunctuator} holds captured punctuators, along with their scheduling information.
 */
    public static final class CapturedPunctuator {
        private final Duration interval;
        private final PunctuationType type;
        private final Punctuator punctuator;
        private boolean cancelled = false;

        private CapturedPunctuator(final Duration interval, final PunctuationType type, final Punctuator punctuator) {
            this.interval = interval;
            this.type = type;
            this.punctuator = punctuator;
        }

        public Duration getInterval() {
            return interval;
        }

        public PunctuationType getType() {
            return type;
        }

        public Punctuator getPunctuator() {
            return punctuator;
        }

        public void cancel() {
            cancelled = true;
        }

        public boolean cancelled() {
            return cancelled;
        }
    }

    public static final class CapturedForward<K, V> {

        private final FixedKeyRecord<K, V> fixedKeyRecord;
        private final Optional<String> childName;

        public CapturedForward(final FixedKeyRecord<K, V> fixedKeyRecord) {
            this(fixedKeyRecord, Optional.empty());
        }

        public CapturedForward(final FixedKeyRecord<K, V> fixedKeyRecord, final Optional<String> childName) {
            this.fixedKeyRecord = Objects.requireNonNull(fixedKeyRecord);
            this.childName = Objects.requireNonNull(childName);
        }

        /**
         * The child this data was forwarded to.
         *
         * @return If present, the child name the record was forwarded to.
         *         If empty, the forward was a broadcast.
         */
        public Optional<String> childName() {
            return childName;
        }

        /**
         * The fixed key record that was forwarded.
         *
         * @return The forwarded fixed key record. Not null.
         */
        public FixedKeyRecord<K, V> fixedKeyRecord() {
            return fixedKeyRecord;
        }

        @Override
        public String toString() {
            return "CapturedForward{" +
                    "fixed key record=" + fixedKeyRecord +
                    ", childName=" + childName +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final CapturedForward<?, ?> that = (CapturedForward<?, ?>) o;
            return Objects.equals(fixedKeyRecord, that.fixedKeyRecord) &&
                    Objects.equals(childName, that.childName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fixedKeyRecord, childName);
        }
    }

    // constructors ================================================

    /**
     * Create a {@link MockProcessorContext} with dummy {@code config} and {@code taskId} and {@code null} {@code stateDir}.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link InMemoryKeyValueStore}, so the stateDir won't matter.
     */
    public MockFixedKeyProcessorContext() {
        this(
                mkProperties(mkMap(
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, ""),
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
                )),
                new TaskId(0, 0),
                null
        );
    }

    /**
     * Create a {@link MockFixedKeyProcessorContext} with dummy {@code taskId} and {@code null} {@code stateDir}.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link InMemoryKeyValueStore}, so the stateDir won't matter.
     *
     * @param config a Properties object, used to configure the context and the processor.
     */
    public MockFixedKeyProcessorContext(final Properties config) {
        this(config, new TaskId(0, 0), null);
    }

    /**
     * Create a {@link MockFixedKeyProcessorContext} with a specified taskId and null stateDir.
     *
     * @param config   a {@link Properties} object, used to configure the context and the processor.
     * @param taskId   a {@link TaskId}, which the context makes available via {@link MockFixedKeyProcessorContext#taskId()}.
     * @param stateDir a {@link File}, which the context makes available viw {@link MockFixedKeyProcessorContext#stateDir()}.
     */
    public MockFixedKeyProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
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
        metrics = new StreamsMetricsImpl(
                new Metrics(metricConfig),
                threadId,
                streamsConfig.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
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
        final Map<String, Object> combined = new java.util.HashMap<>();
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
        return Objects.requireNonNull(
                stateDir,
                "The stateDir constructor argument was needed (probably for a state store) but not supplied. " +
                        "You can either reconfigure your test so that it doesn't need access to the disk " +
                        "(such as using an in-memory store), or use the full MockProcessorContext constructor to supply " +
                        "a non-null stateDir argument."
        );
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
     */
    public void setRecordMetadata(final String topic,
                                  final int partition,
                                  final long offset) {
        recordMetadata = new MockRecordMetadata(topic, partition, offset);
    }

    public void setCurrentSystemTimeMs(final long currentSystemTimeMs) {
        this.currentSystemTimeMs = currentSystemTimeMs;
    }

    public void setCurrentStreamTimeMs(final long currentStreamTimeMs) {
        this.currentStreamTimeMs = currentStreamTimeMs;
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        return Optional.ofNullable(recordMetadata);
    }

    // mocks ================================================

    @SuppressWarnings("unchecked")
    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        return (S) stateStores.get(name);
    }

    public <S extends StateStore> void addStateStore(final S stateStore) {
        stateStores.put(stateStore.name(), stateStore);
    }

    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        final CapturedPunctuator capturedPunctuator = new CapturedPunctuator(interval, type, callback);

        punctuators.add(capturedPunctuator);

        return capturedPunctuator::cancel;
    }

    /**
     * Get the punctuators scheduled so far. The returned list is not affected by subsequent calls to {@code schedule(...)}.
     *
     * @return A list of captured punctuators.
     */
    public List<CapturedPunctuator> scheduledPunctuators() {
        return new LinkedList<>(punctuators);
    }

    public <K extends KForward, V extends VForward> void forward(final FixedKeyRecord<K, V> record) {
        forward(record, null);
    }

    public <K extends KForward, V extends VForward> void forward(final FixedKeyRecord<K, V> record, final String childName) {
        capturedForwards.add(new CapturedForward<>(record, Optional.ofNullable(childName)));
    }

    /**
     * Get all the forwarded data this context has observed. The returned list will not be
     * affected by subsequent interactions with the context. The data in the list is in the same order as the calls to
     * {@code forward(...)}.
     *
     * @return A list of records that were previously passed to the context.
     */
    public List<CapturedForward<? extends KForward, ? extends VForward>> forwarded() {
        return new LinkedList<>(capturedForwards);
    }

    /**
     * Get all the forwarded data this context has observed for a specific child by name.
     * The returned list will not be affected by subsequent interactions with the context.
     * The data in the list is in the same order as the calls to {@code forward(...)}.
     *
     * @param childName The child name to retrieve forwards for
     * @return A list of records that were previously passed to the context.
     */
    public List<CapturedForward<? extends KForward, ? extends VForward>> forwarded(final String childName) {
        final LinkedList<CapturedForward<? extends KForward, ? extends VForward>> result = new LinkedList<>();
        for (final CapturedForward<? extends KForward, ? extends VForward> capture : capturedForwards) {
            if (!capture.childName().isPresent() || capture.childName().equals(Optional.of(childName))) {
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
     * Reset the commit capture to {@code false} (whether it was previously {@code true}).
     */
    public void resetCommit() {
        committed = false;
    }

    @Override
    public RecordCollector recordCollector() {
        // This interface is assumed by state stores that add change-logging.
        // Rather than risk a mysterious ClassCastException during unit tests, throw an explanatory exception.

        throw new UnsupportedOperationException(
                "MockFixedKeyProcessorContext does not provide record collection. " +
                        "For processor unit tests, use an in-memory state store with change-logging disabled. " +
                        "Alternatively, use the TopologyTestDriver for testing processor/store/topology integration."
        );
    }

    /**
     * Used to get a {@link StateStoreContext} for use with
     * {@link StateStore#init(StateStoreContext, StateStore)}
     * if you need to initialize a store for your tests.
     * @return a {@link StateStoreContext} that delegates to this ProcessorContext.
     */
    public StateStoreContext getStateStoreContext() {
        return new StateStoreContext() {
            @Override
            public String applicationId() {
                return MockFixedKeyProcessorContext.this.applicationId();
            }

            @Override
            public TaskId taskId() {
                return MockFixedKeyProcessorContext.this.taskId();
            }

            @Override
            public java.util.Optional<RecordMetadata> recordMetadata() {
                return MockFixedKeyProcessorContext.this.recordMetadata();
            }

            @Override
            public Serde<?> keySerde() {
                return MockFixedKeyProcessorContext.this.keySerde();
            }

            @Override
            public Serde<?> valueSerde() {
                return MockFixedKeyProcessorContext.this.valueSerde();
            }

            @Override
            public File stateDir() {
                return MockFixedKeyProcessorContext.this.stateDir();
            }

            @Override
            public StreamsMetrics metrics() {
                return MockFixedKeyProcessorContext.this.metrics();
            }

            @Override
            public void register(final StateStore store,
                                 final StateRestoreCallback stateRestoreCallback) {
                register(store, stateRestoreCallback, () -> { });
            }

            @Override
            public void register(final StateStore store,
                                 final StateRestoreCallback stateRestoreCallback,
                                 final CommitCallback checkpoint) {
                stateStores.put(store.name(), store);
            }

            @Override
            public Map<String, Object> appConfigs() {
                return MockFixedKeyProcessorContext.this.appConfigs();
            }

            @Override
            public Map<String, Object> appConfigsWithPrefix(final String prefix) {
                return MockFixedKeyProcessorContext.this.appConfigsWithPrefix(prefix);
            }
        };
    }
}
