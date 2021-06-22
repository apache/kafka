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
package org.apache.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TopologyTestDriverTest {

    TopologyTestDriverTest(final Map<String, String> overrides) {
        config = mkProperties(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test-TopologyTestDriver"),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath())
        ));
        config.putAll(overrides);
    }

    private final static String SOURCE_TOPIC_1 = "source-topic-1";
    private final static String SOURCE_TOPIC_2 = "source-topic-2";
    private final static String SINK_TOPIC_1 = "sink-topic-1";
    private final static String SINK_TOPIC_2 = "sink-topic-2";

    private final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

    private final byte[] key1 = new byte[0];
    private final byte[] value1 = new byte[0];
    private final long timestamp1 = 42L;
    private final TestRecord<byte[], byte[]> testRecord1 = new TestRecord<>(key1, value1, headers, timestamp1);

    private final byte[] key2 = new byte[0];
    private final byte[] value2 = new byte[0];
    private final long timestamp2 = 43L;

    private TopologyTestDriver testDriver;
    private final Properties config;
    private KeyValueStore<String, Long> store;

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();

    private final static class TTDTestRecord {
        private final Object key;
        private final Object value;
        private final long timestamp;
        private final long offset;
        private final String topic;
        private final Headers headers;

        TTDTestRecord(final String newTopic,
                      final TestRecord<byte[], byte[]> consumerRecord,
                      final long newOffset) {
            key = consumerRecord.key();
            value = consumerRecord.value();
            timestamp = consumerRecord.timestamp();
            offset = newOffset;
            topic = newTopic;
            headers = consumerRecord.headers();
        }

        TTDTestRecord(final Object key,
                      final Object value,
                      final Headers headers,
                      final long timestamp,
                      final long offset,
                      final String topic) {
            this.key = key;
            this.value = value;
            this.headers = headers;
            this.timestamp = timestamp;
            this.offset = offset;
            this.topic = topic;
        }

        @Override
        public String toString() {
            return "key: " + key +
                   ", value: " + value +
                   ", timestamp: " + timestamp +
                   ", offset: " + offset +
                   ", topic: " + topic +
                   ", num.headers: " + (headers == null ? "null" : headers.toArray().length);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TTDTestRecord record = (TTDTestRecord) o;
            return timestamp == record.timestamp &&
                offset == record.offset &&
                Objects.equals(key, record.key) &&
                Objects.equals(value, record.value) &&
                Objects.equals(topic, record.topic) &&
                Objects.equals(headers, record.headers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, headers, timestamp, offset, topic);
        }
    }

    private final static class Punctuation {
        private final long intervalMs;
        private final PunctuationType punctuationType;
        private final Punctuator callback;

        Punctuation(final long intervalMs,
                    final PunctuationType punctuationType,
                    final Punctuator callback) {
            this.intervalMs = intervalMs;
            this.punctuationType = punctuationType;
            this.callback = callback;
        }
    }

    private final static class MockPunctuator implements Punctuator {
        private final List<Long> punctuatedAt = new LinkedList<>();

        @Override
        public void punctuate(final long timestamp) {
            punctuatedAt.add(timestamp);
        }
    }

    private final static class MockProcessor implements Processor<Object, Object, Object, Object> {
        private final Collection<Punctuation> punctuations;
        private ProcessorContext<Object, Object> context;

        private boolean initialized = false;
        private boolean closed = false;
        private final List<TTDTestRecord> processedRecords = new ArrayList<>();

        MockProcessor(final Collection<Punctuation> punctuations) {
            this.punctuations = punctuations;
        }

        @Override
        public void init(final ProcessorContext<Object, Object> context) {
            initialized = true;
            this.context = context;
            for (final Punctuation punctuation : punctuations) {
                this.context.schedule(Duration.ofMillis(punctuation.intervalMs), punctuation.punctuationType, punctuation.callback);
            }
        }

        @Override
        public void process(final Record<Object, Object> record) {
            processedRecords.add(new TTDTestRecord(
                record.key(),
                record.value(),
                record.headers(),
                record.timestamp(),
                context.recordMetadata().map(RecordMetadata::offset).orElse(-1L),
                context.recordMetadata().map(RecordMetadata::topic).orElse(null)
            ));
            context.forward(record);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final List<MockProcessor> mockProcessors = new ArrayList<>();

    private final class MockProcessorSupplier implements ProcessorSupplier<Object, Object, Object, Object> {
        private final Collection<Punctuation> punctuations;

        private MockProcessorSupplier() {
            this(Collections.emptySet());
        }

        private MockProcessorSupplier(final Collection<Punctuation> punctuations) {
            this.punctuations = punctuations;
        }

        @Override
        public Processor<Object, Object, Object, Object> get() {
            final MockProcessor mockProcessor = new MockProcessor(punctuations);

            // to keep tests simple, ignore calls from ApiUtils.checkSupplier
            if (!isCheckSupplierCall()) {
                mockProcessors.add(mockProcessor);
            }

            return mockProcessor;
        }

        /**
         * Used to keep tests simple, and ignore calls from {@link org.apache.kafka.streams.internals.ApiUtils#checkSupplier(Supplier)} )}.
         * @return true if the stack context is within a {@link org.apache.kafka.streams.internals.ApiUtils#checkSupplier(Supplier)} )} call
         */
        public boolean isCheckSupplierCall() {
            return Arrays.stream(Thread.currentThread().getStackTrace())
                    .anyMatch(caller -> "org.apache.kafka.streams.internals.ApiUtils".equals(caller.getClassName()) && "checkSupplier".equals(caller.getMethodName()));
        }
    }

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    private Topology setupSourceSinkTopology() {
        final Topology topology = new Topology();

        final String sourceName = "source";

        topology.addSource(sourceName, SOURCE_TOPIC_1);
        topology.addSink("sink", SINK_TOPIC_1, sourceName);

        return topology;
    }

    private Topology setupTopologyWithTwoSubtopologies() {
        final Topology topology = new Topology();

        final String sourceName1 = "source-1";
        final String sourceName2 = "source-2";

        topology.addSource(sourceName1, SOURCE_TOPIC_1);
        topology.addSink("sink-1", SINK_TOPIC_1, sourceName1);
        topology.addSource(sourceName2, SINK_TOPIC_1);
        topology.addSink("sink-2", SINK_TOPIC_2, sourceName2);

        return topology;
    }


    private Topology setupSingleProcessorTopology() {
        return setupSingleProcessorTopology(-1, null, null);
    }

    private Topology setupSingleProcessorTopology(final long punctuationIntervalMs,
                                                  final PunctuationType punctuationType,
                                                  final Punctuator callback) {
        final Collection<Punctuation> punctuations;
        if (punctuationIntervalMs > 0 && punctuationType != null && callback != null) {
            punctuations = Collections.singleton(new Punctuation(punctuationIntervalMs, punctuationType, callback));
        } else {
            punctuations = Collections.emptySet();
        }

        final Topology topology = new Topology();

        final String sourceName = "source";

        topology.addSource(sourceName, SOURCE_TOPIC_1);
        topology.addProcessor("processor", new MockProcessorSupplier(punctuations), sourceName);

        return topology;
    }

    private Topology setupMultipleSourceTopology(final String... sourceTopicNames) {
        final Topology topology = new Topology();

        final String[] processorNames = new String[sourceTopicNames.length];
        int i = 0;
        for (final String sourceTopicName : sourceTopicNames) {
            final String sourceName = sourceTopicName + "-source";
            final String processorName = sourceTopicName + "-processor";
            topology.addSource(sourceName, sourceTopicName);
            processorNames[i++] = processorName;
            topology.addProcessor(processorName, new MockProcessorSupplier(), sourceName);
        }
        topology.addSink("sink-topic", SINK_TOPIC_1, processorNames);

        return topology;
    }

    private Topology setupGlobalStoreTopology(final String... sourceTopicNames) {
        if (sourceTopicNames.length == 0) {
            throw new IllegalArgumentException("sourceTopicNames cannot be empty");
        }
        final Topology topology = new Topology();

        for (final String sourceTopicName : sourceTopicNames) {
            topology.addGlobalStore(
                Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(
                        sourceTopicName + "-globalStore"),
                    null,
                    null)
                    .withLoggingDisabled(),
                sourceTopicName,
                null,
                null,
                sourceTopicName,
                sourceTopicName + "-processor",
                () -> new Processor<Object, Object, Void, Void>() {
                    KeyValueStore<Object, Object> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext<Void, Void> context) {
                        store = context.getStateStore(sourceTopicName + "-globalStore");
                    }

                    @Override
                    public void process(final Record<Object, Object> record) {
                        store.put(record.key(), record.value());
                    }
                }
            );
        }

        return topology;
    }

    private Topology setupTopologyWithInternalTopic(final String firstTableName,
                                                    final String secondTableName,
                                                    final String joinName) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Object, Long> t1 = builder.stream(SOURCE_TOPIC_1)
            .selectKey((k, v) -> v)
            .groupByKey()
            .count(Materialized.as(firstTableName));

        builder.table(SOURCE_TOPIC_2, Materialized.as(secondTableName))
            .join(t1, v -> v, (v1, v2) -> v2, Named.as(joinName));

        return builder.build(config);
    }

    @Test
    public void shouldNotRequireParameters() {
        new TopologyTestDriver(setupSingleProcessorTopology(), new Properties());
    }

    @Test
    public void shouldInitProcessor() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology(), config);
        assertTrue(mockProcessors.get(0).initialized);
    }

    @Test
    public void shouldCloseProcessor() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology(), config);
        testDriver.close();
        assertTrue(mockProcessors.get(0).closed);
        // As testDriver is already closed, bypassing @AfterEach tearDown testDriver.close().
        testDriver = null;
    }

    @Test
    public void shouldThrowForUnknownTopic() {
        testDriver = new TopologyTestDriver(new Topology());
        assertThrows(
            IllegalArgumentException.class,
            () -> testDriver.pipeRecord(
                "unknownTopic",
                new TestRecord<>((byte[]) null),
                new ByteArraySerializer(),
                new ByteArraySerializer(),
                Instant.now())
        );
    }

    @Test
    public void shouldThrowForMissingTime() {
        testDriver = new TopologyTestDriver(new Topology());
        assertThrows(
            IllegalStateException.class,
            () -> testDriver.pipeRecord(
                SINK_TOPIC_1,
                new TestRecord<>("value"),
                new StringSerializer(),
                new StringSerializer(),
                null));
    }

    @Test
    public void shouldThrowNoSuchElementExceptionForUnusedOutputTopicWithDynamicRouting() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology());
        final TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(
            testDriver,
            "unused-topic",
            new StringDeserializer(),
            new StringDeserializer()
        );

        assertTrue(outputTopic.isEmpty());
        assertThrows(NoSuchElementException.class, outputTopic::readRecord);
    }

    @Test
    public void shouldCaptureSinkTopicNamesIfWrittenInto() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology());

        assertThat(testDriver.producedTopicNames(), is(Collections.emptySet()));

        pipeRecord(SOURCE_TOPIC_1, testRecord1);
        assertThat(testDriver.producedTopicNames(), hasItem(SINK_TOPIC_1));
    }

    @Test
    public void shouldCaptureInternalTopicNamesIfWrittenInto() {
        testDriver = new TopologyTestDriver(
            setupTopologyWithInternalTopic("table1", "table2", "join"),
            config
        );

        assertThat(testDriver.producedTopicNames(), is(Collections.emptySet()));

        pipeRecord(SOURCE_TOPIC_1, testRecord1);
        assertThat(
            testDriver.producedTopicNames(),
            equalTo(mkSet(
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-table1-repartition",
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-table1-changelog"
            ))
        );

        pipeRecord(SOURCE_TOPIC_2, testRecord1);
        assertThat(
            testDriver.producedTopicNames(),
            equalTo(mkSet(
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-table1-repartition",
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-table1-changelog",
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-table2-changelog",
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-join-subscription-registration-topic",
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-join-subscription-store-changelog",
                config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-join-subscription-response-topic"
            ))
        );
    }

    @Test
    public void shouldCaptureGlobalTopicNameIfWrittenInto() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable(SOURCE_TOPIC_1, Materialized.as("globalTable"));
        builder.stream(SOURCE_TOPIC_2).to(SOURCE_TOPIC_1);

        testDriver = new TopologyTestDriver(builder.build());

        assertThat(testDriver.producedTopicNames(), is(Collections.emptySet()));

        pipeRecord(SOURCE_TOPIC_2, testRecord1);
        assertThat(
            testDriver.producedTopicNames(),
            equalTo(Collections.singleton(SOURCE_TOPIC_1))
        );
    }

    @Test
    public void shouldProcessRecordForTopic() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology());

        pipeRecord(SOURCE_TOPIC_1, testRecord1);
        final ProducerRecord<byte[], byte[]> outputRecord = testDriver.readRecord(SINK_TOPIC_1);

        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_1, outputRecord.topic());
    }

    @Test
    public void shouldSetRecordMetadata() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology());

        pipeRecord(SOURCE_TOPIC_1, testRecord1);

        final List<TTDTestRecord> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final TTDTestRecord record = processedRecords.get(0);
        final TTDTestRecord expectedResult = new TTDTestRecord(SOURCE_TOPIC_1, testRecord1, 0L);

        assertThat(record, equalTo(expectedResult));
    }

    private void pipeRecord(final String topic, final TestRecord<byte[], byte[]> record) {
        testDriver.pipeRecord(topic, record, new ByteArraySerializer(), new ByteArraySerializer(), null);
    }


    @Test
    public void shouldSendRecordViaCorrectSourceTopic() {
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(SOURCE_TOPIC_1, SOURCE_TOPIC_2));

        final List<TTDTestRecord> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<TTDTestRecord> processedRecords2 = mockProcessors.get(1).processedRecords;

        final TestInputTopic<byte[], byte[]> inputTopic1 = testDriver.createInputTopic(SOURCE_TOPIC_1,
                new ByteArraySerializer(), new ByteArraySerializer());
        final TestInputTopic<byte[], byte[]> inputTopic2 = testDriver.createInputTopic(SOURCE_TOPIC_2,
                new ByteArraySerializer(), new ByteArraySerializer());

        inputTopic1.pipeInput(new TestRecord<>(key1, value1, headers, timestamp1));

        assertEquals(1, processedRecords1.size());
        assertEquals(0, processedRecords2.size());

        TTDTestRecord record = processedRecords1.get(0);
        TTDTestRecord expectedResult = new TTDTestRecord(key1, value1, headers, timestamp1, 0L, SOURCE_TOPIC_1);
        assertThat(record, equalTo(expectedResult));

        inputTopic2.pipeInput(new TestRecord<>(key2, value2, Instant.ofEpochMilli(timestamp2)));

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        record = processedRecords2.get(0);
        expectedResult = new TTDTestRecord(key2, value2, new RecordHeaders((Iterable<Header>) null), timestamp2, 0L, SOURCE_TOPIC_2);
        assertThat(record, equalTo(expectedResult));
    }

    @Test
    public void shouldUseSourceSpecificDeserializers() {
        final Topology topology = new Topology();

        final String sourceName1 = "source-1";
        final String sourceName2 = "source-2";
        final String processor = "processor";

        topology.addSource(sourceName1, Serdes.Long().deserializer(), Serdes.String().deserializer(), SOURCE_TOPIC_1);
        topology.addSource(sourceName2, Serdes.Integer().deserializer(), Serdes.Double().deserializer(), SOURCE_TOPIC_2);
        topology.addProcessor(processor, new MockProcessorSupplier(), sourceName1, sourceName2);
        topology.addSink(
            "sink",
            SINK_TOPIC_1,
            (topic, data) -> {
                if (data instanceof Long) {
                    return Serdes.Long().serializer().serialize(topic, (Long) data);
                }
                return Serdes.Integer().serializer().serialize(topic, (Integer) data);
            },
            (topic, data) -> {
                if (data instanceof String) {
                    return Serdes.String().serializer().serialize(topic, (String) data);
                }
                return Serdes.Double().serializer().serialize(topic, (Double) data);
            },
            processor);

        testDriver = new TopologyTestDriver(topology);

        final Long source1Key = 42L;
        final String source1Value = "anyString";
        final Integer source2Key = 73;
        final Double source2Value = 3.14;

        final TestRecord<Long, String> consumerRecord1 = new TestRecord<>(source1Key, source1Value);
        final TestRecord<Integer, Double> consumerRecord2 = new TestRecord<>(source2Key, source2Value);

        testDriver.pipeRecord(SOURCE_TOPIC_1,
                consumerRecord1,
                Serdes.Long().serializer(),
                Serdes.String().serializer(),
                Instant.now());
        final TestRecord<Long, String> result1 =
            testDriver.readRecord(SINK_TOPIC_1, Serdes.Long().deserializer(), Serdes.String().deserializer());
        assertThat(result1.getKey(), equalTo(source1Key));
        assertThat(result1.getValue(), equalTo(source1Value));

        testDriver.pipeRecord(SOURCE_TOPIC_2,
                consumerRecord2,
                Serdes.Integer().serializer(),
                Serdes.Double().serializer(),
                Instant.now());
        final TestRecord<Integer, Double> result2 =
            testDriver.readRecord(SINK_TOPIC_1, Serdes.Integer().deserializer(), Serdes.Double().deserializer());
        assertThat(result2.getKey(), equalTo(source2Key));
        assertThat(result2.getValue(), equalTo(source2Value));
    }

    @Test
    public void shouldPassRecordHeadersIntoSerializersAndDeserializers() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology());

        final AtomicBoolean passedHeadersToKeySerializer = new AtomicBoolean(false);
        final AtomicBoolean passedHeadersToValueSerializer = new AtomicBoolean(false);
        final AtomicBoolean passedHeadersToKeyDeserializer = new AtomicBoolean(false);
        final AtomicBoolean passedHeadersToValueDeserializer = new AtomicBoolean(false);

        final Serializer<byte[]> keySerializer = new ByteArraySerializer() {
            @Override
            public byte[] serialize(final String topic, final Headers headers, final byte[] data) {
                passedHeadersToKeySerializer.set(true);
                return serialize(topic, data);
            }
        };
        final Serializer<byte[]> valueSerializer = new ByteArraySerializer() {
            @Override
            public byte[] serialize(final String topic, final Headers headers, final byte[] data) {
                passedHeadersToValueSerializer.set(true);
                return serialize(topic, data);
            }
        };

        final Deserializer<byte[]> keyDeserializer = new ByteArrayDeserializer() {
            @Override
            public byte[] deserialize(final String topic, final Headers headers, final byte[] data) {
                passedHeadersToKeyDeserializer.set(true);
                return deserialize(topic, data);
            }
        };
        final Deserializer<byte[]> valueDeserializer = new ByteArrayDeserializer() {
            @Override
            public byte[] deserialize(final String topic, final Headers headers, final byte[] data) {
                passedHeadersToValueDeserializer.set(true);
                return deserialize(topic, data);
            }
        };

        final TestInputTopic<byte[], byte[]> inputTopic = testDriver.createInputTopic(SOURCE_TOPIC_1, keySerializer, valueSerializer);
        final TestOutputTopic<byte[], byte[]> outputTopic = testDriver.createOutputTopic(SINK_TOPIC_1, keyDeserializer, valueDeserializer);
        inputTopic.pipeInput(testRecord1);
        outputTopic.readRecord();

        assertThat(passedHeadersToKeySerializer.get(), equalTo(true));
        assertThat(passedHeadersToValueSerializer.get(), equalTo(true));
        assertThat(passedHeadersToKeyDeserializer.get(), equalTo(true));
        assertThat(passedHeadersToValueDeserializer.get(), equalTo(true));
    }

    @Test
    public void shouldUseSinkSpecificSerializers() {
        final Topology topology = new Topology();

        final String sourceName1 = "source-1";
        final String sourceName2 = "source-2";

        topology.addSource(sourceName1, Serdes.Long().deserializer(), Serdes.String().deserializer(), SOURCE_TOPIC_1);
        topology.addSource(sourceName2, Serdes.Integer().deserializer(), Serdes.Double().deserializer(), SOURCE_TOPIC_2);
        topology.addSink("sink-1", SINK_TOPIC_1, Serdes.Long().serializer(), Serdes.String().serializer(), sourceName1);
        topology.addSink("sink-2", SINK_TOPIC_2, Serdes.Integer().serializer(), Serdes.Double().serializer(), sourceName2);

        testDriver = new TopologyTestDriver(topology);

        final Long source1Key = 42L;
        final String source1Value = "anyString";
        final Integer source2Key = 73;
        final Double source2Value = 3.14;

        final TestRecord<Long, String> consumerRecord1 = new TestRecord<>(source1Key, source1Value);
        final TestRecord<Integer, Double> consumerRecord2 = new TestRecord<>(source2Key, source2Value);

        testDriver.pipeRecord(SOURCE_TOPIC_1,
                consumerRecord1,
                Serdes.Long().serializer(),
                Serdes.String().serializer(),
                Instant.now());
        final TestRecord<Long, String> result1 =
                testDriver.readRecord(SINK_TOPIC_1, Serdes.Long().deserializer(), Serdes.String().deserializer());
        assertThat(result1.getKey(), equalTo(source1Key));
        assertThat(result1.getValue(), equalTo(source1Value));

        testDriver.pipeRecord(SOURCE_TOPIC_2,
                consumerRecord2,
                Serdes.Integer().serializer(),
                Serdes.Double().serializer(),
                Instant.now());
        final TestRecord<Integer, Double> result2 =
                testDriver.readRecord(SINK_TOPIC_2, Serdes.Integer().deserializer(), Serdes.Double().deserializer());
        assertThat(result2.getKey(), equalTo(source2Key));
        assertThat(result2.getValue(), equalTo(source2Value));
    }

    @Test
    public void shouldForwardRecordsFromSubtopologyToSubtopology() {
        testDriver = new TopologyTestDriver(setupTopologyWithTwoSubtopologies());

        pipeRecord(SOURCE_TOPIC_1, testRecord1);

        ProducerRecord<byte[], byte[]> outputRecord = testDriver.readRecord(SINK_TOPIC_1);
        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_1, outputRecord.topic());

        outputRecord = testDriver.readRecord(SINK_TOPIC_2);
        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_2, outputRecord.topic());
    }

    @Test
    public void shouldPopulateGlobalStore() {
        testDriver = new TopologyTestDriver(setupGlobalStoreTopology(SOURCE_TOPIC_1));

        final KeyValueStore<byte[], byte[]> globalStore = testDriver.getKeyValueStore(SOURCE_TOPIC_1 + "-globalStore");
        assertNotNull(globalStore);
        assertNotNull(testDriver.getAllStateStores().get(SOURCE_TOPIC_1 + "-globalStore"));

        pipeRecord(SOURCE_TOPIC_1, testRecord1);

        assertThat(globalStore.get(testRecord1.key()), is(testRecord1.value()));
    }

    @Test
    public void shouldPunctuateOnStreamsTime() {
        final MockPunctuator mockPunctuator = new MockPunctuator();
        testDriver = new TopologyTestDriver(
            setupSingleProcessorTopology(10L, PunctuationType.STREAM_TIME, mockPunctuator)
        );

        final List<Long> expectedPunctuations = new LinkedList<>();

        expectedPunctuations.add(42L);
        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 42L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 42L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(51L);
        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 51L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 52L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(61L);
        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 61L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 65L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(71L);
        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 71L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 72L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(95L);
        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 95L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(101L);
        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 101L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        pipeRecord(SOURCE_TOPIC_1, new TestRecord<>(key1, value1, null, 102L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));
    }

    @Test
    public void shouldPunctuateOnWallClockTime() {
        final MockPunctuator mockPunctuator = new MockPunctuator();
        testDriver = new TopologyTestDriver(
            setupSingleProcessorTopology(10L, PunctuationType.WALL_CLOCK_TIME, mockPunctuator),
            config, Instant.ofEpochMilli(0L));

        final List<Long> expectedPunctuations = new LinkedList<>();

        testDriver.advanceWallClockTime(Duration.ofMillis(5L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(14L);
        testDriver.advanceWallClockTime(Duration.ofMillis(9L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.advanceWallClockTime(Duration.ofMillis(1L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(35L);
        testDriver.advanceWallClockTime(Duration.ofMillis(20L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(40L);
        testDriver.advanceWallClockTime(Duration.ofMillis(5L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));
    }

    @Test
    public void shouldReturnAllStores() {
        final Topology topology = setupSourceSinkTopology();
        topology.addProcessor("processor", new MockProcessorSupplier(), "source");
        topology.addStateStore(
            new KeyValueStoreBuilder<>(
                Stores.inMemoryKeyValueStore("store"),
                Serdes.ByteArray(),
                Serdes.ByteArray(),
                new SystemTime()),
            "processor");
        topology.addGlobalStore(
            new KeyValueStoreBuilder<>(
                Stores.inMemoryKeyValueStore("globalStore"),
                Serdes.ByteArray(),
                Serdes.ByteArray(),
                new SystemTime()).withLoggingDisabled(),
            "sourceProcessorName",
            Serdes.ByteArray().deserializer(),
            Serdes.ByteArray().deserializer(),
            "globalTopicName",
            "globalProcessorName",
            voidProcessorSupplier);

        testDriver = new TopologyTestDriver(topology);

        final Set<String> expectedStoreNames = new HashSet<>();
        expectedStoreNames.add("store");
        expectedStoreNames.add("globalStore");
        final Map<String, StateStore> allStores = testDriver.getAllStateStores();
        assertThat(allStores.keySet(), equalTo(expectedStoreNames));
        for (final StateStore store : allStores.values()) {
            assertNotNull(store);
        }
    }

    @Test
    public void shouldReturnCorrectPersistentStoreTypeOnly() {
        shouldReturnCorrectStoreTypeOnly(true);
    }

    @Test
    public void shouldReturnCorrectInMemoryStoreTypeOnly() {
        shouldReturnCorrectStoreTypeOnly(false);
    }

    private void shouldReturnCorrectStoreTypeOnly(final boolean persistent) {
        final String keyValueStoreName = "keyValueStore";
        final String timestampedKeyValueStoreName = "keyValueTimestampStore";
        final String windowStoreName = "windowStore";
        final String timestampedWindowStoreName = "windowTimestampStore";
        final String sessionStoreName = "sessionStore";
        final String globalKeyValueStoreName = "globalKeyValueStore";
        final String globalTimestampedKeyValueStoreName = "globalKeyValueTimestampStore";

        final Topology topology = setupSingleProcessorTopology();
        addStoresToTopology(
            topology,
            persistent,
            keyValueStoreName,
            timestampedKeyValueStoreName,
            windowStoreName,
            timestampedWindowStoreName,
            sessionStoreName,
            globalKeyValueStoreName,
            globalTimestampedKeyValueStoreName);


        testDriver = new TopologyTestDriver(topology);

        // verify state stores
        assertNotNull(testDriver.getKeyValueStore(keyValueStoreName));
        assertNull(testDriver.getTimestampedKeyValueStore(keyValueStoreName));
        assertNull(testDriver.getWindowStore(keyValueStoreName));
        assertNull(testDriver.getTimestampedWindowStore(keyValueStoreName));
        assertNull(testDriver.getSessionStore(keyValueStoreName));

        assertNotNull(testDriver.getKeyValueStore(timestampedKeyValueStoreName));
        assertNotNull(testDriver.getTimestampedKeyValueStore(timestampedKeyValueStoreName));
        assertNull(testDriver.getWindowStore(timestampedKeyValueStoreName));
        assertNull(testDriver.getTimestampedWindowStore(timestampedKeyValueStoreName));
        assertNull(testDriver.getSessionStore(timestampedKeyValueStoreName));

        assertNull(testDriver.getKeyValueStore(windowStoreName));
        assertNull(testDriver.getTimestampedKeyValueStore(windowStoreName));
        assertNotNull(testDriver.getWindowStore(windowStoreName));
        assertNull(testDriver.getTimestampedWindowStore(windowStoreName));
        assertNull(testDriver.getSessionStore(windowStoreName));

        assertNull(testDriver.getKeyValueStore(timestampedWindowStoreName));
        assertNull(testDriver.getTimestampedKeyValueStore(timestampedWindowStoreName));
        assertNotNull(testDriver.getWindowStore(timestampedWindowStoreName));
        assertNotNull(testDriver.getTimestampedWindowStore(timestampedWindowStoreName));
        assertNull(testDriver.getSessionStore(timestampedWindowStoreName));

        assertNull(testDriver.getKeyValueStore(sessionStoreName));
        assertNull(testDriver.getTimestampedKeyValueStore(sessionStoreName));
        assertNull(testDriver.getWindowStore(sessionStoreName));
        assertNull(testDriver.getTimestampedWindowStore(sessionStoreName));
        assertNotNull(testDriver.getSessionStore(sessionStoreName));

        // verify global stores
        assertNotNull(testDriver.getKeyValueStore(globalKeyValueStoreName));
        assertNull(testDriver.getTimestampedKeyValueStore(globalKeyValueStoreName));
        assertNull(testDriver.getWindowStore(globalKeyValueStoreName));
        assertNull(testDriver.getTimestampedWindowStore(globalKeyValueStoreName));
        assertNull(testDriver.getSessionStore(globalKeyValueStoreName));

        assertNotNull(testDriver.getKeyValueStore(globalTimestampedKeyValueStoreName));
        assertNotNull(testDriver.getTimestampedKeyValueStore(globalTimestampedKeyValueStoreName));
        assertNull(testDriver.getWindowStore(globalTimestampedKeyValueStoreName));
        assertNull(testDriver.getTimestampedWindowStore(globalTimestampedKeyValueStoreName));
        assertNull(testDriver.getSessionStore(globalTimestampedKeyValueStoreName));
    }

    @Test
    public void shouldThrowIfInMemoryBuiltInStoreIsAccessedWithUntypedMethod() {
        shouldThrowIfBuiltInStoreIsAccessedWithUntypedMethod(false);
    }

    @Test
    public void shouldThrowIfPersistentBuiltInStoreIsAccessedWithUntypedMethod() {
        shouldThrowIfBuiltInStoreIsAccessedWithUntypedMethod(true);
    }

    private void shouldThrowIfBuiltInStoreIsAccessedWithUntypedMethod(final boolean persistent) {
        final String keyValueStoreName = "keyValueStore";
        final String timestampedKeyValueStoreName = "keyValueTimestampStore";
        final String windowStoreName = "windowStore";
        final String timestampedWindowStoreName = "windowTimestampStore";
        final String sessionStoreName = "sessionStore";
        final String globalKeyValueStoreName = "globalKeyValueStore";
        final String globalTimestampedKeyValueStoreName = "globalKeyValueTimestampStore";

        final Topology topology = setupSingleProcessorTopology();
        addStoresToTopology(
            topology,
            persistent,
            keyValueStoreName,
            timestampedKeyValueStoreName,
            windowStoreName,
            timestampedWindowStoreName,
            sessionStoreName,
            globalKeyValueStoreName,
            globalTimestampedKeyValueStoreName);


        testDriver = new TopologyTestDriver(topology);

        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(keyValueStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + keyValueStoreName
                    + " is a key-value store and should be accessed via `getKeyValueStore()`"));
        }
        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(timestampedKeyValueStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + timestampedKeyValueStoreName
                    + " is a timestamped key-value store and should be accessed via `getTimestampedKeyValueStore()`"));
        }
        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(windowStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + windowStoreName
                    + " is a window store and should be accessed via `getWindowStore()`"));
        }
        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(timestampedWindowStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + timestampedWindowStoreName
                    + " is a timestamped window store and should be accessed via `getTimestampedWindowStore()`"));
        }
        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(sessionStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + sessionStoreName
                    + " is a session store and should be accessed via `getSessionStore()`"));
        }
        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(globalKeyValueStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + globalKeyValueStoreName
                    + " is a key-value store and should be accessed via `getKeyValueStore()`"));
        }
        {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> testDriver.getStateStore(globalTimestampedKeyValueStoreName));
            assertThat(
                e.getMessage(),
                equalTo("Store " + globalTimestampedKeyValueStoreName
                    + " is a timestamped key-value store and should be accessed via `getTimestampedKeyValueStore()`"));
        }
    }

    final ProcessorSupplier<byte[], byte[], Void, Void> voidProcessorSupplier = () -> new Processor<byte[], byte[], Void, Void>() {
        @Override
        public void process(final Record<byte[], byte[]> record) {
        }
    };

    private void addStoresToTopology(final Topology topology,
                                     final boolean persistent,
                                     final String keyValueStoreName,
                                     final String timestampedKeyValueStoreName,
                                     final String windowStoreName,
                                     final String timestampedWindowStoreName,
                                     final String sessionStoreName,
                                     final String globalKeyValueStoreName,
                                     final String globalTimestampedKeyValueStoreName) {

        // add state stores
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                persistent ?
                    Stores.persistentKeyValueStore(keyValueStoreName) :
                    Stores.inMemoryKeyValueStore(keyValueStoreName),
                Serdes.ByteArray(),
                Serdes.ByteArray()
            ),
            "processor");
        topology.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                persistent ?
                    Stores.persistentTimestampedKeyValueStore(timestampedKeyValueStoreName) :
                    Stores.inMemoryKeyValueStore(timestampedKeyValueStoreName),
                Serdes.ByteArray(),
                Serdes.ByteArray()
            ),
            "processor");
        topology.addStateStore(
            Stores.windowStoreBuilder(
                persistent ?
                    Stores.persistentWindowStore(windowStoreName, Duration.ofMillis(1000L), Duration.ofMillis(100L), false) :
                    Stores.inMemoryWindowStore(windowStoreName, Duration.ofMillis(1000L), Duration.ofMillis(100L), false),
                Serdes.ByteArray(),
                Serdes.ByteArray()
            ),
            "processor");
        topology.addStateStore(
            Stores.timestampedWindowStoreBuilder(
                persistent ?
                    Stores.persistentTimestampedWindowStore(timestampedWindowStoreName, Duration.ofMillis(1000L), Duration.ofMillis(100L), false) :
                    Stores.inMemoryWindowStore(timestampedWindowStoreName, Duration.ofMillis(1000L), Duration.ofMillis(100L), false),
                Serdes.ByteArray(),
                Serdes.ByteArray()
            ),
            "processor");
        topology.addStateStore(
            persistent ?
                Stores.sessionStoreBuilder(
                    Stores.persistentSessionStore(sessionStoreName, Duration.ofMillis(1000L)),
                    Serdes.ByteArray(),
                    Serdes.ByteArray()) :
                Stores.sessionStoreBuilder(
                    Stores.inMemorySessionStore(sessionStoreName, Duration.ofMillis(1000L)),
                    Serdes.ByteArray(),
                    Serdes.ByteArray()),
            "processor");
        // add global stores
        topology.addGlobalStore(
            persistent ?
                Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(globalKeyValueStoreName),
                    Serdes.ByteArray(),
                    Serdes.ByteArray()
                ).withLoggingDisabled() :
                Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(globalKeyValueStoreName),
                    Serdes.ByteArray(),
                    Serdes.ByteArray()
                ).withLoggingDisabled(),
            "sourceDummy1",
            Serdes.ByteArray().deserializer(),
            Serdes.ByteArray().deserializer(),
            "topicDummy1",
            "processorDummy1",
            voidProcessorSupplier);
        topology.addGlobalStore(
            persistent ?
                Stores.timestampedKeyValueStoreBuilder(
                    Stores.persistentTimestampedKeyValueStore(globalTimestampedKeyValueStoreName),
                    Serdes.ByteArray(),
                    Serdes.ByteArray()
                ).withLoggingDisabled() :
                Stores.timestampedKeyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(globalTimestampedKeyValueStoreName),
                    Serdes.ByteArray(),
                    Serdes.ByteArray()
                ).withLoggingDisabled(),
            "sourceDummy2",
            Serdes.ByteArray().deserializer(),
            Serdes.ByteArray().deserializer(),
            "topicDummy2",
            "processorDummy2",
            voidProcessorSupplier);
    }

    @Test
    public void shouldReturnAllStoresNames() {
        final Topology topology = setupSourceSinkTopology();
        topology.addStateStore(
            new KeyValueStoreBuilder<>(
                Stores.inMemoryKeyValueStore("store"),
                Serdes.ByteArray(),
                Serdes.ByteArray(),
                new SystemTime()));
        topology.addGlobalStore(
            new KeyValueStoreBuilder<>(
                Stores.inMemoryKeyValueStore("globalStore"),
                Serdes.ByteArray(),
                Serdes.ByteArray(),
                new SystemTime()).withLoggingDisabled(),
            "sourceProcessorName",
            Serdes.ByteArray().deserializer(),
            Serdes.ByteArray().deserializer(),
            "globalTopicName",
            "globalProcessorName",
            voidProcessorSupplier);

        testDriver = new TopologyTestDriver(topology);

        final Set<String> expectedStoreNames = new HashSet<>();
        expectedStoreNames.add("store");
        expectedStoreNames.add("globalStore");
        assertThat(testDriver.getAllStateStores().keySet(), equalTo(expectedStoreNames));
    }

    private void setup() {
        setup(Stores.inMemoryKeyValueStore("aggStore"));
    }

    private void setup(final KeyValueBytesStoreSupplier storeSupplier) {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(Stores.keyValueStoreBuilder(
                storeSupplier,
                Serdes.String(),
                Serdes.Long()),
                "aggregator");
        topology.addSink("sinkProcessor", "result-topic", "aggregator");

        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, config);

        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    private void pipeInput(final String topic, final String key, final Long value, final Long time) {
        testDriver.pipeRecord(topic, new TestRecord<>(key, value, null, time),
                new StringSerializer(), new LongSerializer(), null);
    }

    private void compareKeyValue(final TestRecord<String, Long> record, final String key, final Long value) {
        assertThat(record.getKey(), equalTo(key));
        assertThat(record.getValue(), equalTo(value));
    }

    @Test
    public void shouldFlushStoreForFirstInput() {
        setup();
        pipeInput("input-topic", "a", 1L, 9999L);
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        assertTrue(testDriver.isEmpty("result-topic"));
    }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        setup();
        pipeInput("input-topic", "a", 1L, 9999L);
        assertThat(store.get("a"), equalTo(21L));
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        assertTrue(testDriver.isEmpty("result-topic"));
    }

    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        setup();
        pipeInput("input-topic", "a", 42L, 9999L);
        assertThat(store.get("a"), equalTo(42L));
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 42L);
        assertTrue(testDriver.isEmpty("result-topic"));
    }

    @Test
    public void shouldUpdateStoreForNewKey() {
        setup();
        pipeInput("input-topic", "b", 21L, 9999L);
        assertThat(store.get("b"), equalTo(21L));
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "b", 21L);
        assertTrue(testDriver.isEmpty("result-topic"));
    }

    @Test
    public void shouldPunctuateIfEvenTimeAdvances() {
        setup();
        pipeInput("input-topic", "a", 1L, 9999L);
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 21L);

        pipeInput("input-topic", "a", 1L, 9999L);
        assertTrue(testDriver.isEmpty("result-topic"));

        pipeInput("input-topic", "a", 1L, 10000L);
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        assertTrue(testDriver.isEmpty("result-topic"));
    }

    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        setup();
        testDriver.advanceWallClockTime(Duration.ofMillis(60000));
        compareKeyValue(testDriver.readRecord("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        assertTrue(testDriver.isEmpty("result-topic"));
    }

    private static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long, String, Long> {
        @Override
        public Processor<String, Long, String, Long> get() {
            return new CustomMaxAggregator();
        }
    }

    private static class CustomMaxAggregator implements Processor<String, Long, String, Long> {
        ProcessorContext<String, Long> context;
        private KeyValueStore<String, Long> store;

        @Override
        public void init(final ProcessorContext<String, Long> context) {
            this.context = context;
            context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::flushStore);
            context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, this::flushStore);
            store = context.getStateStore("aggStore");
        }

        @Override
        public void process(final Record<String, Long> record) {
            final Long oldValue = store.get(record.key());
            if (oldValue == null || record.value() > oldValue) {
                store.put(record.key(), record.value());
            }
        }

        private void flushStore(final long timestamp) {
            try (final KeyValueIterator<String, Long> it = store.all()) {
                while (it.hasNext()) {
                    final KeyValue<String, Long> next = it.next();
                    context.forward(new Record<>(next.key, next.value, timestamp));
                }
            }
        }
    }

    @Test
    public void shouldAllowPrePopulatingStatesStoresWithCachingEnabled() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("aggStore"),
            Serdes.String(),
            Serdes.Long()).withCachingEnabled(), // intentionally turn on caching to achieve better test coverage
            "aggregator");

        testDriver = new TopologyTestDriver(topology);

        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    @Test
    public void shouldCleanUpPersistentStateStoresOnClose() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor(
            "storeProcessor",
            new ProcessorSupplier<String, Long, Void, Void>() {
                @Override
                public Processor<String, Long, Void, Void> get() {
                    return new Processor<String, Long, Void, Void>() {
                        private KeyValueStore<String, Long> store;

                        @Override
                        public void init(final ProcessorContext<Void, Void> context) {
                            this.store = context.getStateStore("storeProcessorStore");
                        }

                        @Override
                        public void process(final Record<String, Long> record) {
                            store.put(record.key(), record.value());
                        }
                    };
                }
            },
            "sourceProcessor"
        );
        topology.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("storeProcessorStore"), Serdes.String(), Serdes.Long()),
            "storeProcessor");

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-TopologyTestDriver-cleanup");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config)) {
            assertNull(testDriver.getKeyValueStore("storeProcessorStore").get("a"));
            testDriver.pipeRecord("input-topic", new TestRecord<>("a", 1L),
                    new StringSerializer(), new LongSerializer(), Instant.now());
            assertEquals(1L, testDriver.getKeyValueStore("storeProcessorStore").get("a"));
        }


        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config)) {
            assertNull(testDriver.getKeyValueStore("storeProcessorStore").get("a"),
                    "Closing the prior test driver should have cleaned up this store and value.");
        }

    }

    @Test
    public void shouldFeedStoreFromGlobalKTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("topic",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as("globalStore"));
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), config)) {
            final KeyValueStore<String, String> globalStore = testDriver.getKeyValueStore("globalStore");
            assertNotNull(globalStore);
            assertNotNull(testDriver.getAllStateStores().get("globalStore"));
            testDriver.pipeRecord(
                "topic",
                new TestRecord<>("k1", "value1"),
                new StringSerializer(),
                new StringSerializer(),
                Instant.now());
            // we expect to have both in the global store, the one from pipeInput and the one from the producer
            assertEquals("value1", globalStore.get("k1"));
        }
    }

    private Topology setupMultipleSourcesPatternTopology(final Pattern... sourceTopicPatternNames) {
        final Topology topology = new Topology();

        final String[] processorNames = new String[sourceTopicPatternNames.length];
        int i = 0;
        for (final Pattern sourceTopicPatternName : sourceTopicPatternNames) {
            final String sourceName = sourceTopicPatternName + "-source";
            final String processorName = sourceTopicPatternName + "-processor";
            topology.addSource(sourceName, sourceTopicPatternName);
            processorNames[i++] = processorName;
            topology.addProcessor(processorName, new MockProcessorSupplier(), sourceName);
        }
        topology.addSink("sink-topic", SINK_TOPIC_1, processorNames);
        return topology;
    }

    @Test
    public void shouldProcessFromSourcesThatMatchMultiplePattern() {

        final  Pattern pattern2Source1 = Pattern.compile("source-topic-\\d");
        final  Pattern pattern2Source2 = Pattern.compile("source-topic-[A-Z]");
        final  String consumerTopic2 = "source-topic-Z";

        final TestRecord<byte[], byte[]> consumerRecord2 = new TestRecord<>(key2, value2, null, timestamp2);

        testDriver = new TopologyTestDriver(setupMultipleSourcesPatternTopology(pattern2Source1, pattern2Source2), config);

        final List<TTDTestRecord> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<TTDTestRecord> processedRecords2 = mockProcessors.get(1).processedRecords;

        pipeRecord(SOURCE_TOPIC_1, testRecord1);

        assertEquals(1, processedRecords1.size());
        assertEquals(0, processedRecords2.size());

        final TTDTestRecord record1 = processedRecords1.get(0);
        final TTDTestRecord expectedResult1 = new TTDTestRecord(SOURCE_TOPIC_1, testRecord1, 0L);
        assertThat(record1, equalTo(expectedResult1));

        pipeRecord(consumerTopic2, consumerRecord2);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        final TTDTestRecord record2 = processedRecords2.get(0);
        final TTDTestRecord expectedResult2 = new TTDTestRecord(consumerTopic2, consumerRecord2, 0L);
        assertThat(record2, equalTo(expectedResult2));
    }

    @Test
    public void shouldProcessFromSourceThatMatchPattern() {
        final String sourceName = "source";
        final Pattern pattern2Source1 = Pattern.compile("source-topic-\\d");

        final Topology topology = new Topology();

        topology.addSource(sourceName, pattern2Source1);
        topology.addSink("sink", SINK_TOPIC_1, sourceName);

        testDriver = new TopologyTestDriver(topology);
        pipeRecord(SOURCE_TOPIC_1, testRecord1);

        final ProducerRecord<byte[], byte[]> outputRecord = testDriver.readRecord(SINK_TOPIC_1);
        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_1, outputRecord.topic());
    }

    @Test
    public void shouldThrowPatternNotValidForTopicNameException() {
        final String sourceName = "source";
        final String pattern2Source1 = "source-topic-\\d";

        final Topology topology = new Topology();

        topology.addSource(sourceName, pattern2Source1);
        topology.addSink("sink", SINK_TOPIC_1, sourceName);

        testDriver = new TopologyTestDriver(topology);
        try {
            pipeRecord(SOURCE_TOPIC_1, testRecord1);
        } catch (final TopologyException exception) {
            final String str =
                    String.format(
                            "Invalid topology: Topology add source of type String for topic: %s cannot contain regex pattern for " +
                                    "input record topic: %s and hence cannot process the message.",
                            pattern2Source1,
                            SOURCE_TOPIC_1);
            assertEquals(str, exception.getMessage());
        }
    }

    @Test
    public void shouldNotCreateStateDirectoryForStatelessTopology() {
        setup();
        final String stateDir = config.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        final File appDir = new File(stateDir, config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertFalse(appDir.exists());
    }

    @Test
    public void shouldCreateStateDirectoryForStatefulTopology() {
        setup(Stores.persistentKeyValueStore("aggStore"));
        final String stateDir = config.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        final File appDir = new File(stateDir, config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

        assertTrue(appDir.exists());
        assertTrue(appDir.isDirectory());

        final TaskId taskId = new TaskId(0, 0);
        assertTrue(new File(appDir, taskId.toString()).exists());
    }

    @Test
    public void shouldEnqueueLaterOutputsAfterEarlierOnes() {
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), "input");
        topology.addProcessor(
            "recursiveProcessor",
            () -> new Processor<String, String, String, String>() {
                private ProcessorContext<String, String> context;

                @Override
                public void init(final ProcessorContext<String, String> context) {
                    this.context = context;
                }

                @Override
                public void process(final Record<String, String> record) {
                    final String value = record.value();
                    if (!value.startsWith("recurse-")) {
                        context.forward(record.withValue("recurse-" + value), "recursiveSink");
                    }
                    context.forward(record, "sink");
                }
            },
            "source"
        );
        topology.addSink("recursiveSink", "input", new StringSerializer(), new StringSerializer(), "recursiveProcessor");
        topology.addSink("sink", "output", new StringSerializer(), new StringSerializer(), "recursiveProcessor");

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology)) {
            final TestInputTopic<String, String> in = topologyTestDriver.createInputTopic("input", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> out = topologyTestDriver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());

            // given the topology above, we expect to see the output _first_ echo the input
            // and _then_ print it with "recurse-" prepended.

            in.pipeInput("B", "beta");
            final List<KeyValue<String, String>> events = out.readKeyValuesToList();
            assertThat(
                events,
                is(Arrays.asList(
                    new KeyValue<>("B", "beta"),
                    new KeyValue<>("B", "recurse-beta")
                ))
            );

        }
    }

    @Test
    public void shouldApplyGlobalUpdatesCorrectlyInRecursiveTopologies() {
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), "input");
        topology.addGlobalStore(
            Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("global-store"), Serdes.String(), Serdes.String()).withLoggingDisabled(),
            "globalSource",
            new StringDeserializer(),
            new StringDeserializer(),
            "global-topic",
            "globalProcessor",
            () -> new Processor<String, String, Void, Void>() {
                private KeyValueStore<String, String> stateStore;

                @Override
                public void init(final ProcessorContext<Void, Void> context) {
                    stateStore = context.getStateStore("global-store");
                }

                @Override
                public void process(final Record<String, String> record) {
                    stateStore.put(record.key(), record.value());
                }
            }
        );
        topology.addProcessor(
            "recursiveProcessor",
            () -> new Processor<String, String, String, String>() {
                private ProcessorContext<String, String> context;

                @Override
                public void init(final ProcessorContext<String, String> context) {
                    this.context = context;
                }

                @Override
                public void process(final Record<String, String> record) {
                    final String value = record.value();
                    if (!value.startsWith("recurse-")) {
                        context.forward(record.withValue("recurse-" + value), "recursiveSink");
                    }
                    context.forward(record, "sink");
                    context.forward(record, "globalSink");
                }
            },
            "source"
        );
        topology.addSink("recursiveSink", "input", new StringSerializer(), new StringSerializer(), "recursiveProcessor");
        topology.addSink("sink", "output", new StringSerializer(), new StringSerializer(), "recursiveProcessor");
        topology.addSink("globalSink", "global-topic", new StringSerializer(), new StringSerializer(), "recursiveProcessor");

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology)) {
            final TestInputTopic<String, String> in = topologyTestDriver.createInputTopic("input", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> globalTopic = topologyTestDriver.createOutputTopic("global-topic", new StringDeserializer(), new StringDeserializer());

            in.pipeInput("A", "alpha");

            // expect the global store to correctly reflect the last update
            final KeyValueStore<String, String> keyValueStore = topologyTestDriver.getKeyValueStore("global-store");
            assertThat(keyValueStore, notNullValue());
            assertThat(keyValueStore.get("A"), is("recurse-alpha"));

            // and also just make sure the test really sent both events to the topic.
            final List<KeyValue<String, String>> events = globalTopic.readKeyValuesToList();
            assertThat(
                events,
                is(Arrays.asList(
                    new KeyValue<>("A", "alpha"),
                    new KeyValue<>("A", "recurse-alpha")
                ))
            );
        }
    }

    @Test
    public void shouldRespectTaskIdling() {
        final Properties properties = new Properties();
        // This is the key to this test. Wall-clock time doesn't advance automatically in TopologyTestDriver,
        // so with an idle time specified, TTD can't just expect all enqueued records to be processable.
        properties.setProperty(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "1000");

        final Topology topology = new Topology();
        topology.addSource("source1", new StringDeserializer(), new StringDeserializer(), "input1");
        topology.addSource("source2", new StringDeserializer(), new StringDeserializer(), "input2");
        topology.addSink("sink", "output", new StringSerializer(), new StringSerializer(), "source1", "source2");

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, properties)) {
            final TestInputTopic<String, String> in1 = topologyTestDriver.createInputTopic("input1", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> in2 = topologyTestDriver.createInputTopic("input2", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> out = topologyTestDriver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());

            in1.pipeInput("A", "alpha");
            topologyTestDriver.advanceWallClockTime(Duration.ofMillis(1));

            // only one input has records, and it's only been one ms
            assertThat(out.readKeyValuesToList(), is(Collections.emptyList()));

            in2.pipeInput("B", "beta");

            // because both topics have records, we can process (even though it's only been one ms)
            // but after processing A (the earlier record), we now only have one input queued, so
            // task idling takes effect again
            assertThat(
                out.readKeyValuesToList(),
                is(Collections.singletonList(
                    new KeyValue<>("A", "alpha")
                ))
            );

            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));

            // now that one second has elapsed, the idle time has expired, and we can process B
            assertThat(
                out.readKeyValuesToList(),
                is(Collections.singletonList(
                    new KeyValue<>("B", "beta")
                ))
            );
        }
    }
}
