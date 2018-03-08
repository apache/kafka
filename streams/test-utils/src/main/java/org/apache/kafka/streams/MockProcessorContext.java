package org.apache.kafka.streams;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamsMetricsImpl;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MockProcessorContext implements ProcessorContext {
    private final StreamsMetrics metrics;
    private final String applicationId;
    private final TaskId taskId;
    private final StreamsConfig config;
    private final File stateDir;

    private String topic = null;
    private Integer partition = null;
    private Long offset = null;
    private Long timestamp = null;

    public MockProcessorContext(final String applicationId, final TaskId taskId, final Properties config) {
        this(applicationId, taskId, config, null);
    }

    public MockProcessorContext(final String applicationId, final TaskId taskId, final Properties config, final File stateDir) {
        this.applicationId = applicationId;
        this.taskId = taskId;
        this.config = new StreamsConfig(config);
        this.stateDir = stateDir;
        this.metrics = new StreamsMetricsImpl(new Metrics(), "mock-processor-context-metrics", Collections.<String, String>emptyMap());
    }

    @Override
    public StreamsMetrics metrics() {
        return metrics;
    }

    // high level metadata ====================================================
    @Override
    public String applicationId() {
        return applicationId;
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
    public Serde<?> keySerde() { return config.defaultKeySerde(); }

    @Override
    public Serde<?> valueSerde() { return config.defaultValueSerde();}

    @Override
    public File stateDir() { return stateDir;}

    // record metadata ========================================================
    public void setRecordMetadata(final String topic, final int partition, final long offset, final long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public void setRecordMetadataTopic(final String topic) { this.topic = topic;}
    public void setRecordMetadataPartition(final int partition) { this.partition = partition;}
    public void setRecordMetadataOffset(final long offset) { this.offset = offset;}
    public void setRecordMetadataTimestamp(final long timestamp) { this.timestamp = timestamp;}

    @Override
    public String topic() {
        if (topic == null) throw new IllegalStateException("topic was not set.");
        return topic;
    }
    @Override
    public int partition() {
        if (partition == null) throw new IllegalStateException("partition was not set.");
        return partition;
    }
    @Override
    public long offset() {
        if (offset == null) throw new IllegalStateException("offset was not set.");
        return offset;
    }
    @Override
    public long timestamp() {
        if (timestamp == null) throw new IllegalStateException("timestamp was not set.");
        return timestamp;
    }

    // mocked methods =========================================================
    @Override
    public void register(final StateStore store, final boolean loggingEnabledIsDeprecatedAndIgnored, final StateRestoreCallback stateRestoreCallback) {
        throw new RuntimeException("TODO");
    }

    @Override
    public StateStore getStateStore(final String name) {
        throw new RuntimeException("TODO");
    }

    @Override
    public Cancellable schedule(final long intervalMs, final PunctuationType type, final Punctuator callback) {
        throw new RuntimeException("TODO");
    }

    public List<CapturedPunctuator> scheduledPunctuators() {
        throw new RuntimeException("TODO");
    }

    @Override
    public void schedule(final long interval) {
        throw new UnsupportedOperationException("This method is deprecated, and this mock cannot support it, since it doesn't have a handle on your punctuate method.");
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        throw new RuntimeException("TODO");
    }

    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        throw new RuntimeException("TODO");
    }

    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        throw new RuntimeException("TODO");
    }

    public <K, V> List<KeyValue<K, V>> forwarded() { throw new RuntimeException("TODO"); }
    public <K, V> List<KeyValue<K, V>> forwarded(final int childIndex) { throw new RuntimeException("TODO"); }
    public <K, V> List<KeyValue<K, V>> forwarded(final String childName) { throw new RuntimeException("TODO"); }

    public void resetForwards() {throw new RuntimeException("TODO");}

    @Override
    public void commit() {throw new RuntimeException("TODO");}

    public boolean committed() {throw new RuntimeException("TODO");}

    public void resetCommits() {throw new RuntimeException("TODO");}


    public static class CapturedPunctuator {
        private final long intervalMs;
        private final PunctuationType punctuationType;
        private final Punctuator punctuator;

        private CapturedPunctuator(final long intervalMs, final PunctuationType punctuationType, final Punctuator punctuator) {
            this.intervalMs = intervalMs;
            this.punctuationType = punctuationType;
            this.punctuator = punctuator;
        }

        public long getIntervalMs() { return intervalMs;}

        public PunctuationType getPunctuationType() { return punctuationType;}

        public Punctuator getPunctuator() { return punctuator;}
    }
}
