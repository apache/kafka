package org.apache.kafka.streams.processor.internals;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

public class ValueProcessorContext<KForward, VForward> implements ProcessorContext<KForward, VForward> {

    final ProcessorContext<KForward, VForward> delegate;

    private KForward key;

    public ValueProcessorContext(ProcessorContext<KForward, VForward> delegate) {
        this.delegate = delegate;
    }

    @Override
    public String applicationId() {
        return null;
    }

    @Override
    public TaskId taskId() {
        return null;
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        return Optional.empty();
    }

    @Override
    public Serde<?> keySerde() {
        return null;
    }

    @Override
    public Serde<?> valueSerde() {
        return null;
    }

    @Override
    public File stateDir() {
        return null;
    }

    @Override
    public StreamsMetrics metrics() {
        return null;
    }

    @Override
    public <S extends StateStore> S getStateStore(String name) {
        return null;
    }

    @Override
    public Cancellable schedule(Duration interval, PunctuationType type,
        Punctuator callback) {
        return null;
    }

    public void setInitialKey(KForward initialKey) {
        this.key = initialKey;
    }

    public void unsetInitialKey() {
        this.key = null;
    }

    @Override
    public <K extends KForward, V extends VForward> void forward(Record<K, V> record) {
        if (key != null) {
            if (!record.key().equals(key)) {
                throw new IllegalArgumentException("Key has changed, and repartition is required.");
            }
        }
        delegate.forward(record);
    }

    @Override
    public <K extends KForward, V extends VForward> void forward(Record<K, V> record,
        String childName) {

    }

    @Override
    public void commit() {

    }

    @Override
    public Map<String, Object> appConfigs() {
        return null;
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String prefix) {
        return null;
    }
}
