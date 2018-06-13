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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Arrays;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class TopologyTestDriverTest {
    private final static String SOURCE_TOPIC_1 = "source-topic-1";
    private final static String SOURCE_TOPIC_2 = "source-topic-2";
    private final static String SINK_TOPIC_1 = "sink-topic-1";
    private final static String SINK_TOPIC_2 = "sink-topic-2";

    private final ConsumerRecordFactory<byte[], byte[]> consumerRecordFactory = new ConsumerRecordFactory<>(
        new ByteArraySerializer(),
        new ByteArraySerializer());

    private final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

    private final byte[] key1 = new byte[0];
    private final byte[] value1 = new byte[0];
    private final long timestamp1 = 42L;
    private final ConsumerRecord<byte[], byte[]> consumerRecord1 = consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, headers, timestamp1);

    private final byte[] key2 = new byte[0];
    private final byte[] value2 = new byte[0];
    private final long timestamp2 = 43L;
    private final ConsumerRecord<byte[], byte[]> consumerRecord2 = consumerRecordFactory.create(SOURCE_TOPIC_2, key2, value2, timestamp2);

    private TopologyTestDriver testDriver;
    private final Properties config = new Properties() {
        {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "test-TopologyTestDriver");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
            put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        }
    };
    private KeyValueStore<String, Long> store;

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();
    private final ConsumerRecordFactory<String, Long> recordFactory = new ConsumerRecordFactory<>(
        new StringSerializer(),
        new LongSerializer());

    private final boolean eosEnabled;

    @Parameterized.Parameters(name = "Eos enabled = {0}")
    public static Collection<Object[]> data() {
        final List<Object[]> values = new ArrayList<>();
        for (final boolean eosEnabled : Arrays.asList(true, false)) {
            values.add(new Object[] {eosEnabled});
        }
        return values;
    }

    public TopologyTestDriverTest(final boolean eosEnabled) {
        this.eosEnabled = eosEnabled;
        if (eosEnabled) {
            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        }
    }

    private final static class Record {
        private final Object key;
        private final Object value;
        private final long timestamp;
        private final long offset;
        private final String topic;
        private final Headers headers;

        Record(final ConsumerRecord consumerRecord,
               final long newOffset) {
            key = consumerRecord.key();
            value = consumerRecord.value();
            timestamp = consumerRecord.timestamp();
            offset = newOffset;
            topic = consumerRecord.topic();
            headers = consumerRecord.headers();
        }

        Record(final Object key,
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
            return "key: " + key + ", value: " + value + ", timestamp: " + timestamp + ", offset: " + offset + ", topic: " + topic;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Record record = (Record) o;
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

    private final class MockPunctuator implements Punctuator {
        private final List<Long> punctuatedAt = new LinkedList<>();

        @Override
        public void punctuate(final long timestamp) {
            punctuatedAt.add(timestamp);
        }
    }

    private final class MockProcessor implements Processor {
        private final Collection<Punctuation> punctuations;
        private ProcessorContext context;

        private boolean initialized = false;
        private boolean closed = false;
        private final List<Record> processedRecords = new ArrayList<>();

        MockProcessor(final Collection<Punctuation> punctuations) {
            this.punctuations = punctuations;
        }

        @Override
        public void init(final ProcessorContext context) {
            initialized = true;
            this.context = context;
            for (final Punctuation punctuation : punctuations) {
                this.context.schedule(punctuation.intervalMs, punctuation.punctuationType, punctuation.callback);
            }
        }

        @Override
        public void process(final Object key, final Object value) {
            processedRecords.add(new Record(key, value, context.headers(), context.timestamp(), context.offset(), context.topic()));
            context.forward(key, value);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final List<MockProcessor> mockProcessors = new ArrayList<>();

    private final class MockProcessorSupplier implements ProcessorSupplier {
        private final Collection<Punctuation> punctuations;

        private MockProcessorSupplier() {
            this(Collections.emptySet());
        }

        private MockProcessorSupplier(final Collection<Punctuation> punctuations) {
            this.punctuations = punctuations;
        }

        @Override
        public Processor get() {
            final MockProcessor mockProcessor = new MockProcessor(punctuations);
            mockProcessors.add(mockProcessor);
            return mockProcessor;
        }
    }

    @After
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
                Stores.<Bytes, byte[]>keyValueStoreBuilder(Stores.inMemoryKeyValueStore(sourceTopicName + "-globalStore"), null, null).withLoggingDisabled(),
                sourceTopicName,
                null,
                null,
                sourceTopicName,
                sourceTopicName + "-processor",
                new MockProcessorSupplier()
            );
        }

        return topology;
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
        // As testDriver is already closed, bypassing @After tearDown testDriver.close().
        testDriver = null;
    }

    @Test
    public void shouldThrowForUnknownTopic() {
        final String unknownTopic = "unknownTopic";
        final ConsumerRecordFactory<byte[], byte[]> consumerRecordFactory = new ConsumerRecordFactory<>(
            "unknownTopic",
            new ByteArraySerializer(),
            new ByteArraySerializer());

        testDriver = new TopologyTestDriver(new Topology(), config);
        try {
            testDriver.pipeInput(consumerRecordFactory.create((byte[]) null));
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException exception) {
            assertEquals("Unknown topic: " + unknownTopic, exception.getMessage());
        }
    }

    @Test
    public void shouldProcessRecordForTopic() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology(), config);

        testDriver.pipeInput(consumerRecord1);
        final ProducerRecord outputRecord = testDriver.readOutput(SINK_TOPIC_1);

        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_1, outputRecord.topic());
    }

    @Test
    public void shouldSetRecordMetadata() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology(), config);

        testDriver.pipeInput(consumerRecord1);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        final Record expectedResult = new Record(consumerRecord1, 0L);

        assertThat(record, equalTo(expectedResult));
    }

    @Test
    public void shouldSendRecordViaCorrectSourceTopic() {
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(SOURCE_TOPIC_1, SOURCE_TOPIC_2), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        testDriver.pipeInput(consumerRecord1);

        assertEquals(1, processedRecords1.size());
        assertEquals(0, processedRecords2.size());

        Record record = processedRecords1.get(0);
        Record expectedResult = new Record(consumerRecord1, 0L);
        assertThat(record, equalTo(expectedResult));

        testDriver.pipeInput(consumerRecord2);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        record = processedRecords2.get(0);
        expectedResult = new Record(consumerRecord2, 0L);
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
            new Serializer<Object>() {
                @Override
                public byte[] serialize(final String topic, final Object data) {
                    if (data instanceof Long) {
                        return Serdes.Long().serializer().serialize(topic, (Long) data);
                    }
                    return Serdes.Integer().serializer().serialize(topic, (Integer) data);
                }
                @Override
                public void close() {}
                @Override
                public void configure(final Map configs, final boolean isKey) {}
            },
            new Serializer<Object>() {
                @Override
                public byte[] serialize(final String topic, final Object data) {
                    if (data instanceof String) {
                        return Serdes.String().serializer().serialize(topic, (String) data);
                    }
                    return Serdes.Double().serializer().serialize(topic, (Double) data);
                }
                @Override
                public void close() {}
                @Override
                public void configure(final Map configs, final boolean isKey) {}
            },
            processor);

        testDriver = new TopologyTestDriver(topology, config);

        final ConsumerRecordFactory<Long, String> source1Factory = new ConsumerRecordFactory<>(
            SOURCE_TOPIC_1,
            Serdes.Long().serializer(),
            Serdes.String().serializer());
        final ConsumerRecordFactory<Integer, Double> source2Factory = new ConsumerRecordFactory<>(
            SOURCE_TOPIC_2,
            Serdes.Integer().serializer(),
            Serdes.Double().serializer());

        final Long source1Key = 42L;
        final String source1Value = "anyString";
        final Integer source2Key = 73;
        final Double source2Value = 3.14;

        final ConsumerRecord<byte[], byte[]> consumerRecord1 = source1Factory.create(source1Key, source1Value);
        final ConsumerRecord<byte[], byte[]> consumerRecord2 = source2Factory.create(source2Key, source2Value);

        testDriver.pipeInput(consumerRecord1);
        OutputVerifier.compareKeyValue(
            testDriver.readOutput(SINK_TOPIC_1, Serdes.Long().deserializer(), Serdes.String().deserializer()),
            source1Key,
            source1Value);

        testDriver.pipeInput(consumerRecord2);
        OutputVerifier.compareKeyValue(
            testDriver.readOutput(SINK_TOPIC_1, Serdes.Integer().deserializer(), Serdes.Double().deserializer()),
            source2Key,
            source2Value);
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

        testDriver = new TopologyTestDriver(topology, config);

        final ConsumerRecordFactory<Long, String> source1Factory = new ConsumerRecordFactory<>(
            SOURCE_TOPIC_1,
            Serdes.Long().serializer(),
            Serdes.String().serializer());
        final ConsumerRecordFactory<Integer, Double> source2Factory = new ConsumerRecordFactory<>(
            SOURCE_TOPIC_2,
            Serdes.Integer().serializer(),
            Serdes.Double().serializer());

        final Long source1Key = 42L;
        final String source1Value = "anyString";
        final Integer source2Key = 73;
        final Double source2Value = 3.14;

        final ConsumerRecord<byte[], byte[]> consumerRecord1 = source1Factory.create(source1Key, source1Value);
        final ConsumerRecord<byte[], byte[]> consumerRecord2 = source2Factory.create(source2Key, source2Value);

        testDriver.pipeInput(consumerRecord1);
        OutputVerifier.compareKeyValue(
            testDriver.readOutput(SINK_TOPIC_1, Serdes.Long().deserializer(), Serdes.String().deserializer()),
            source1Key,
            source1Value);

        testDriver.pipeInput(consumerRecord2);
        OutputVerifier.compareKeyValue(
            testDriver.readOutput(SINK_TOPIC_2, Serdes.Integer().deserializer(), Serdes.Double().deserializer()),
            source2Key,
            source2Value);
    }

    @Test
    public void shouldProcessConsumerRecordList() {
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(SOURCE_TOPIC_1, SOURCE_TOPIC_2), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        final List<ConsumerRecord<byte[], byte[]>> testRecords = new ArrayList<>(2);
        testRecords.add(consumerRecord1);
        testRecords.add(consumerRecord2);

        testDriver.pipeInput(testRecords);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        Record record = processedRecords1.get(0);
        Record expectedResult = new Record(consumerRecord1, 0L);
        assertThat(record, equalTo(expectedResult));

        record = processedRecords2.get(0);
        expectedResult = new Record(consumerRecord2, 0L);
        assertThat(record, equalTo(expectedResult));
    }

    @Test
    public void shouldForwardRecordsFromSubtopologyToSubtopology() {
        testDriver = new TopologyTestDriver(setupTopologyWithTwoSubtopologies(), config);

        testDriver.pipeInput(consumerRecord1);

        ProducerRecord outputRecord = testDriver.readOutput(SINK_TOPIC_1);
        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_1, outputRecord.topic());

        outputRecord = testDriver.readOutput(SINK_TOPIC_2);
        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC_2, outputRecord.topic());
    }

    @Test
    public void shouldPopulateGlobalStore() {
        testDriver = new TopologyTestDriver(setupGlobalStoreTopology(SOURCE_TOPIC_1), config);

        final KeyValueStore<byte[], byte[]> globalStore = testDriver.getKeyValueStore(SOURCE_TOPIC_1 + "-globalStore");
        Assert.assertNotNull(globalStore);
        Assert.assertNotNull(testDriver.getAllStateStores().get(SOURCE_TOPIC_1 + "-globalStore"));

        testDriver.pipeInput(consumerRecord1);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        final Record expectedResult = new Record(consumerRecord1, 0L);
        assertThat(record, equalTo(expectedResult));
    }

    @Test
    public void shouldPunctuateOnStreamsTime() {
        final MockPunctuator mockPunctuator = new MockPunctuator();
        testDriver = new TopologyTestDriver(
            setupSingleProcessorTopology(10L, PunctuationType.STREAM_TIME, mockPunctuator),
            config);

        final List<Long> expectedPunctuations = new LinkedList<>();

        expectedPunctuations.add(42L);
        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 42L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 42L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(51L);
        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 51L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 52L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(61L);
        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 61L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 65L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(71L);
        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 71L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 72L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(95L);
        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 95L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(101L);
        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 101L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.pipeInput(consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, 102L));
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));
    }

    @Test
    public void shouldPunctuateOnWallClockTime() {
        final MockPunctuator mockPunctuator = new MockPunctuator();
        testDriver = new TopologyTestDriver(
            setupSingleProcessorTopology(10L, PunctuationType.WALL_CLOCK_TIME, mockPunctuator),
            config,
            0);

        final List<Long> expectedPunctuations = new LinkedList<>();

        testDriver.advanceWallClockTime(5L);
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(14L);
        testDriver.advanceWallClockTime(9L);
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        testDriver.advanceWallClockTime(1L);
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(35L);
        testDriver.advanceWallClockTime(20L);
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));

        expectedPunctuations.add(40L);
        testDriver.advanceWallClockTime(5L);
        assertThat(mockPunctuator.punctuatedAt, equalTo(expectedPunctuations));
    }

    @Test
    public void shouldReturnAllStores() {
        final Topology topology = setupSourceSinkTopology();
        topology.addProcessor("processor", () -> null);
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
            () -> null);

        testDriver = new TopologyTestDriver(topology, config);

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
            () -> null);

        testDriver = new TopologyTestDriver(topology, config);

        final Set<String> expectedStoreNames = new HashSet<>();
        expectedStoreNames.add("store");
        expectedStoreNames.add("globalStore");
        assertThat(testDriver.getAllStateStores().keySet(), equalTo(expectedStoreNames));
    }

    private void setup() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("aggStore"),
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

    @Test
    public void shouldFlushStoreForFirstInput() {
        setup();
        testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
    }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        setup();
        testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));
        Assert.assertThat(store.get("a"), equalTo(21L));
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
    }

    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        setup();
        testDriver.pipeInput(recordFactory.create("input-topic", "a", 42L, 9999L));
        Assert.assertThat(store.get("a"), equalTo(42L));
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 42L);
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
    }

    @Test
    public void shouldUpdateStoreForNewKey() {
        setup();
        testDriver.pipeInput(recordFactory.create("input-topic", "b", 21L, 9999L));
        Assert.assertThat(store.get("b"), equalTo(21L));
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "b", 21L);
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
    }

    @Test
    public void shouldPunctuateIfEvenTimeAdvances() {
        setup();
        testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);

        testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L));
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));

        testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 10000L));
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
    }

    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        setup();
        testDriver.advanceWallClockTime(60000);
        OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L);
        Assert.assertNull(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer));
    }

    private class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long> {
        @Override
        public Processor<String, Long> get() {
            return new CustomMaxAggregator();
        }
    }

    private class CustomMaxAggregator implements Processor<String, Long> {
        ProcessorContext context;
        private KeyValueStore<String, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
            context.schedule(60000, PunctuationType.WALL_CLOCK_TIME, timestamp -> flushStore());
            context.schedule(10000, PunctuationType.STREAM_TIME, timestamp -> flushStore());
            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
        }

        @Override
        public void process(final String key, final Long value) {
            final Long oldValue = store.get(key);
            if (oldValue == null || value > oldValue) {
                store.put(key, value);
            }
        }

        private void flushStore() {
            final KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                final KeyValue<String, Long> next = it.next();
                context.forward(next.key, next.value);
            }
        }

        @Override
        public void close() {}
    }

    @Test
    public void shouldCleanUpPersistentStateStoresOnClose() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor(
            "storeProcessor",
            new ProcessorSupplier() {
                @Override
                public Processor get() {
                    return new Processor<String, Long>() {
                        private KeyValueStore<String, Long> store;

                        @Override
                        public void init(final ProcessorContext context) {
                            //noinspection unchecked
                            this.store = (KeyValueStore<String, Long>) context.getStateStore("storeProcessorStore");
                        }

                        @Override
                        public void process(final String key, final Long value) {
                            store.put(key, value);
                        }

                        @Override
                        public void close() {}
                    };
                }
            },
            "sourceProcessor"
        );
        topology.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("storeProcessorStore"), Serdes.String(), Serdes.Long()), "storeProcessor");

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-TopologyTestDriver-cleanup");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        {
            final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);
            Assert.assertNull(testDriver.getKeyValueStore("storeProcessorStore").get("a"));
            testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L));
            Assert.assertEquals(1L, testDriver.getKeyValueStore("storeProcessorStore").get("a"));
            testDriver.close();
        }

        {
            final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);
            Assert.assertNull(
                "Closing the prior test driver should have cleaned up this store and value.",
                testDriver.getKeyValueStore("storeProcessorStore").get("a")
            );
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
            Assert.assertNotNull(globalStore);
            Assert.assertNotNull(testDriver.getAllStateStores().get("globalStore"));
            final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
            testDriver.pipeInput(recordFactory.create("topic", "k1", "value1"));
            // we expect to have both in the global store, the one from pipeInput and the one from the producer
            Assert.assertEquals("value1", globalStore.get("k1"));
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

        final ConsumerRecord<byte[], byte[]> consumerRecord2 = consumerRecordFactory.create(consumerTopic2, key2, value2, timestamp2);

        testDriver = new TopologyTestDriver(setupMultipleSourcesPatternTopology(pattern2Source1, pattern2Source2), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        testDriver.pipeInput(consumerRecord1);

        assertEquals(1, processedRecords1.size());
        assertEquals(0, processedRecords2.size());

        final Record record1 = processedRecords1.get(0);
        final Record expectedResult1 = new Record(consumerRecord1, 0L);
        assertThat(record1, equalTo(expectedResult1));

        testDriver.pipeInput(consumerRecord2);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        final Record record2 = processedRecords2.get(0);
        final Record expectedResult2 = new Record(consumerRecord2, 0L);
        assertThat(record2, equalTo(expectedResult2));
    }

    @Test
    public void shouldProcessFromSourceThatMatchPattern() {
        final String sourceName = "source";
        final Pattern pattern2Source1 = Pattern.compile("source-topic-\\d");

        final Topology topology = new Topology();

        topology.addSource(sourceName, pattern2Source1);
        topology.addSink("sink", SINK_TOPIC_1, sourceName);

        testDriver = new TopologyTestDriver(topology, config);
        testDriver.pipeInput(consumerRecord1);

        final ProducerRecord outputRecord = testDriver.readOutput(SINK_TOPIC_1);
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

        testDriver = new TopologyTestDriver(topology, config);
        try {
            testDriver.pipeInput(consumerRecord1);
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
}
