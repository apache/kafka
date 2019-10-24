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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.unmodifiableList;

public class MockInternalProcessorContext extends MockProcessorContext implements InternalProcessorContext {

    public static final class MockRecordCollector implements RecordCollector {


        private final List<ProducerRecord<byte[], byte[]>> collected = new LinkedList<>();

        @Override
        public <K, V> void send(final String topic,
                                final K key,
                                final V value,
                                final Headers headers,
                                final Integer partition,
                                final Long timestamp,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valueSerializer) {
            collected.add(new ProducerRecord<>(topic,
                                               partition,
                                               timestamp,
                                               keySerializer.serialize(topic, key),
                                               valueSerializer.serialize(topic, value),
                                               headers));
        }

        @Override
        public <K, V> void send(final String topic,
                                final K key,
                                final V value,
                                final Headers headers,
                                final Long timestamp,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valueSerializer,
                                final StreamPartitioner<? super K, ? super V> partitioner) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void init(final Producer<byte[], byte[]> producer) {

        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }

        @Override
        public Map<TopicPartition, Long> offsets() {
            return null;
        }

        public List<ProducerRecord<byte[], byte[]>> collected() {
            return unmodifiableList(collected);
        }

    }

    private final Map<String, StateRestoreCallback> restoreCallbacks = new LinkedHashMap<>();
    private RecordCollector recordCollector;
    private Serde<?> keySerde;
    private Serde<?> valueSerde;

    public MockInternalProcessorContext() {
        this(null, null, null, createStreamsConfig());
    }

    public MockInternalProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        super(config, taskId, stateDir);
    }

    public MockInternalProcessorContext(final File stateDir,
                                        final StreamsConfig config) {
        this(
                stateDir,
                null,
                null,
                config
        );
    }

    public MockInternalProcessorContext(final File stateDir,
                                        final Serde<?> keySerde,
                                        final Serde<?> valueSerde,
                                        final StreamsConfig config) {
        this(
                stateDir,
                keySerde,
                valueSerde,
                createStreamsMetrics(),
                config,
                null,
                null
        );
    }

    public MockInternalProcessorContext(final StateSerdes<?, ?> serdes,
                                        final RecordCollector collector) {
        this(serdes, collector, createMetrics());
    }

    public MockInternalProcessorContext(final StateSerdes<?, ?> serdes,
                                        final RecordCollector collector,
                                        final Metrics metrics) {
        this(
                null,
                serdes.keySerde(),
                serdes.valueSerde(),
                createStreamsMetrics(metrics),
                createStreamsConfig(),
                () -> collector,
                null
        );
    }

    public MockInternalProcessorContext(final File stateDir,
                                        final Serde<?> keySerde,
                                        final Serde<?> valueSerde,
                                        final RecordCollector collector,
                                        final ThreadCache cache) {
        this(
                stateDir,
                keySerde,
                valueSerde,
                createStreamsMetrics(),
                createStreamsConfig(),
                () -> collector,
                cache
        );
    }

    public MockInternalProcessorContext(final File stateDir,
                                        final Serde<?> keySerde,
                                        final Serde<?> valueSerde,
                                        final StreamsMetricsImpl metrics,
                                        final StreamsConfig config,
                                        final RecordCollector.Supplier collectorSupplier,
                                        final ThreadCache cache) {
        super(createTaskId(), stateDir, metrics, config, cache);
        setKeySerde(keySerde);
        setValueSerde(valueSerde);
        if(collectorSupplier != null) {
            setRecordCollector(collectorSupplier.recordCollector());
        }
        this.metrics().setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
    }


    private static StreamsMetricsImpl createStreamsMetrics() {
        return createStreamsMetrics(createMetrics());
    }

    private static StreamsMetricsImpl createStreamsMetrics(final Metrics metrics) {
        return new StreamsMetricsImpl(metrics, "mock", StreamsConfig.METRICS_LATEST);
    }

    private static Metrics createMetrics() {
        return new Metrics();
    }

    private static StreamsConfig createStreamsConfig() {
        return new StreamsConfig(StreamsTestUtils.getStreamsConfig());
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
        super.register(store, stateRestoreCallback);
        restoreCallbacks.put(store.name(), stateRestoreCallback);
    }

    public StateRestoreCallback stateRestoreCallback(final String storeName) {
        return restoreCallbacks.get(storeName);
    }

    public void setKeySerde(final Serde<?> keySerde) {
        this.keySerde = keySerde;
    }

    public void setValueSerde(final Serde<?> valueSerde) {
        this.valueSerde = valueSerde;
    }

    @Override
    public Serde<?> keySerde() {
        return keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        return valueSerde;
    }
}
