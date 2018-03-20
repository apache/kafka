package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.internals.StreamsMetricsImpl;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This is a mock of {@link ProcessorContext} provided for authors of {@link Processor},
 * {@link org.apache.kafka.streams.kstream.Transformer}, and {@link org.apache.kafka.streams.kstream.ValueTransformer}.
 * <p>
 * The tests for this class ({@link org.apache.kafka.streams.MockProcessorContextTest}) include several behavioral
 * tests that serve as example usage.
 * <p>
 * Note that this class does not take any automated actions (such as firing scheduled punctuators).
 * It simply captures any data it witnessess.
 * If you require more automated tests, we recommend wrapping your {@link Processor} in a dummy
 * {@link org.apache.kafka.streams.Topology} and using the {@link org.apache.kafka.streams.TopologyTestDriver}.
 * <p>
 */
@SuppressWarnings("JavadocReference")
@InterfaceStability.Evolving
public class MockProcessorContext implements ProcessorContext {
    // Immutable fields ================================================
    private final StreamsMetricsImpl metrics;
    private final TaskId taskId;
    private final StreamsConfig config;
    private final File stateDir; // default: null

    /**
     * Create a {@link MockProcessorContext} with dummy taskId and null stateDir.
     * Most unit tests using this mock won't need to know the taskId,
     * and most unit tests should be able to get by with the
     * {@link org.apache.kafka.streams.state.internals.InMemoryKeyValueStore}, so the stateDir won't matter.
     *
     * @param config a StreamsConfig Properties object.
     *               {@link StreamsConfig.BOOTSTRAP_SERVERS_CONFIG} [required] is ignored.
     *               {@link StreamsConfig.APPLICATION_ID_CONFIG} [required] will set the {@link ProcessorContext#applicationId()} value.
     *               {@link StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG} will set the {@link ProcessorContext#keySerde()} value.
     *               {@link StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG} will set the {@link ProcessorContext#valueSerde()} value.
     *               All properties will be available via {@link ProcessorContext#appConfigs()} and {@link ProcessorContext#appConfigsWithPrefix(String)}.
     */
    public MockProcessorContext(final Properties config) {
        this(config, new TaskId(0, 0), null);
    }

    public MockProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        this.taskId = taskId;
        this.config = streamsConfig;
        this.stateDir = stateDir;
        this.metrics = new StreamsMetricsImpl(new Metrics(), "mock-processor-context", new HashMap<String, String>());
    }

    @Override public String applicationId() { return config.getString(StreamsConfig.APPLICATION_ID_CONFIG);}

    @Override public TaskId taskId() { return taskId;}

    @Override public Map<String, Object> appConfigs() {
        final Map<String, Object> combined = new HashMap<>();
        combined.putAll(config.originals());
        combined.putAll(config.values());
        return combined;
    }

    @Override public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return config.originalsWithPrefix(prefix);
    }

    @Override public Serde<?> keySerde() { return config.defaultKeySerde();}

    @Override public Serde<?> valueSerde() { return config.defaultValueSerde();}

    @Override public File stateDir() { return stateDir;}

    @Override public StreamsMetrics metrics() { return metrics;}

    // settable record metadata ================================================

    private String topic;
    private Integer partition;
    private Long offset;
    private Long timestamp;

    public void setRecordMetadata(final String topic, final int partition, final long offset, final long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }
    public void setTopic(final String topic) { this.topic = topic;}
    public void setPartition(final int partition) { this.partition = partition;}
    public void setOffset(final long offset) { this.offset = offset;}
    public void setTimestamp(final long timestamp) { this.timestamp = timestamp;}

    @Override public String topic() { return topic;}
    @Override public int partition() { return partition;}
    @Override public long offset() { return offset;}
    @Override public long timestamp() { return timestamp;}

    // mocks ================================================

    // state stores
    private final Map<String, StateStore> stateStores = new HashMap<>();
    @Override
    public void register(final StateStore store, final boolean loggingEnabledIsDeprecatedAndIgnored, final StateRestoreCallback stateRestoreCallbackIsIgnoredInMock) {
        stateStores.put(store.name(), store);
    }

    @Override public StateStore getStateStore(final String name) { return stateStores.get(name);}

    // punctuators
    public static class CapturedPunctuator {
        private final long intervalMs;
        private final PunctuationType type;
        private final Punctuator punctuator;

        private CapturedPunctuator(final long intervalMs, final PunctuationType type, final Punctuator punctuator) {
            this.intervalMs = intervalMs;
            this.type = type;
            this.punctuator = punctuator;
        }

        public long getIntervalMs() { return intervalMs;}
        public PunctuationType getType() { return type;}
        public Punctuator getPunctuator() { return punctuator;}
    }

    private final List<CapturedPunctuator> punctuators = new LinkedList<>();

    @Override
    public Cancellable schedule(final long intervalMs, final PunctuationType type, final Punctuator callback) {
        punctuators.add(new CapturedPunctuator(intervalMs, type, callback));
        return new Cancellable() {
            @Override public void cancel() {}
        };
    }
    @Override
    public void schedule(final long interval) {
        throw new UnsupportedOperationException("schedule() is deprecated and not supported in Mock. Use schedule(final long intervalMs, final PunctuationType type, final Punctuator callback) instead.");
    }

    public List<CapturedPunctuator> scheduledPunctuators() {
        final LinkedList<CapturedPunctuator> capturedPunctuators = new LinkedList<>();
        capturedPunctuators.addAll(punctuators);
        return capturedPunctuators;
    }

    // forwards
    private class Capture {
        /*Nullable*/ private final Integer childIndex;
        /*Nullable*/ private final String childName;
        private final KeyValue kv;

        private Capture(final KeyValue kv) {
            this.childIndex = null;
            this.childName = null;
            this.kv = kv;
        }

        private Capture(final Integer childIndex, final KeyValue kv) {
            this.childIndex = childIndex;
            this.childName = null;
            this.kv = kv;
        }

        private Capture(final String childName, final KeyValue kv) {
            this.childIndex = null;
            this.childName = childName;
            this.kv = kv;
        }
    }

    private List<Capture> captured = new LinkedList<>();

    @Override public <FK, FV> void forward(final FK key, final FV value) {
        captured.add(new Capture(castKV(key, value)));
    }

    @Override public <FK, FV> void forward(final FK key, final FV value, final To to) {
        captured.add(new Capture(to.childName, castKV(key, value)));
    }

    @Override public <FK, FV> void forward(final FK key, final FV value, final int childIndex) {
        captured.add(new Capture(childIndex, castKV(key, value)));
    }

    @Override
    public <FK, FV> void forward(final FK key, final FV value, final String childName) {
        captured.add(new Capture(childName, castKV(key, value)));
    }

    @SuppressWarnings("unchecked")
    private <FK, FV> KeyValue castKV(final FK key, final FV value) { return new KeyValue(key, value);}

    public List<KeyValue> forwarded() {
        final LinkedList<KeyValue> result = new LinkedList<>();
        for (final Capture capture : captured) {
            result.add(capture.kv);
        }
        return result;
    }

    public List<KeyValue> forwarded(final int childIndex) {
        final LinkedList<KeyValue> result = new LinkedList<>();
        for (final Capture capture : captured) {
            if (capture.childIndex != null && capture.childIndex == childIndex) {
                result.add(capture.kv);
            }
        }
        return result;
    }

    public List<KeyValue> forwarded(final String childName) {
        final LinkedList<KeyValue> result = new LinkedList<>();
        for (final Capture capture : captured) {
            if (capture.childName != null && capture.childName.equals(childName)) {
                result.add(capture.kv);
            }
        }
        return result;
    }

    public void resetForwards() { captured = new LinkedList<>();}

    // commits

    private boolean committed = false;

    @Override public void commit() { committed = true; }

    public boolean committed() { return committed;}

    public void resetCommit() {committed = false;}
}
