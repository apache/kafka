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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopologyTestDriverTest {
    private final static String SOURCE_TOPIC_1 = "source-topic-1";
    private final static String SOURCE_TOPIC_2 = "source-topic-2";
    private final static String SINK_TOPIC = "sink-topic";

    private final ConsumerRecordFactory<byte[], byte[]> consumerRecordFactory = new ConsumerRecordFactory<>(
        new ByteArraySerializer(),
        new ByteArraySerializer());

    private final byte[] key1 = new byte[0];
    private final byte[] value1 = new byte[0];
    private final long timestamp1 = 42L;
    private final ConsumerRecord<byte[], byte[]> consumerRecord1 = consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, timestamp1);

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

    private final static class Record {
        final Object key;
        final Object value;
        final long timestamp;
        final long offset;
        final String topic;

        Record(final Object key,
               final Object value,
               final long timestamp,
               final long offset,
               final String topic) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.offset = offset;
            this.topic = topic;
        }
    }

    private final static class Punctuation {
        final long interval;
        final PunctuationType punctuationType;
        final Punctuator callback;

        Punctuation(final long interval,
                    final PunctuationType punctuationType,
                    final Punctuator callback) {
            this.interval = interval;
            this.punctuationType = punctuationType;
            this.callback = callback;
        }
    }

    private final class MockProcessor implements Processor {
        private final Collection<Punctuation> punctuations;
        private ProcessorContext context;

        boolean initialized = false;
        boolean closed = false;
        final List<Record> processedRecords = new ArrayList<>();

        MockProcessor() {
            this(Collections.<Punctuation>emptySet());
        }

        MockProcessor(final Collection<Punctuation> punctuations) {
            this.punctuations = punctuations;
        }

        @Override
        public void init(ProcessorContext context) {
            initialized = true;
            this.context = context;
            for (final Punctuation punctuation : punctuations) {
                this.context.schedule(punctuation.interval, punctuation.punctuationType, punctuation.callback);
            }
        }

        @Override
        public void process(Object key, Object value) {
            processedRecords.add(new Record(key, value, context.timestamp(), context.offset(), context.topic()));
            context.forward(key, value);
        }

        @Override
        public void punctuate(long timestamp) {} // deprecated

        @Override
        public void close() {
            closed = true;
        }
    }

    private final List<MockProcessor> mockProcessors = new ArrayList<>();

    private final class MockProcessorSupplier implements ProcessorSupplier {
        @Override
        public Processor get() {
            final MockProcessor mockProcessor = new MockProcessor();
            mockProcessors.add(mockProcessor);
            return mockProcessor;
        }
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    private Topology setupSourceSinkTopology() {
        final Topology topology = new Topology();

        final String sourceName = "source";

        topology.addSource(sourceName, SOURCE_TOPIC_1);
        topology.addSink("sink", SINK_TOPIC, sourceName);

        return topology;
    }

    private Topology setupSingleProcessorTopology() {
        final Topology topology = new Topology();

        final String sourceName = "source";

        topology.addSource(sourceName, SOURCE_TOPIC_1);
        topology.addProcessor("processor", new MockProcessorSupplier(), sourceName);

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
            topology.addProcessor(processorName, new MockProcessorSupplier(), sourceName);
            processorNames[i++] = processorName;
        }
        topology.addSink("sink-topic", SINK_TOPIC, processorNames);


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
            testDriver.process(consumerRecordFactory.create((byte[]) null));
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException exception) {
            assertEquals("Unknown topic: " + unknownTopic, exception.getMessage());
        }
    }

    @Test
    public void shouldProcessRecordForTopic() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology(), config);

        testDriver.process(consumerRecord1);
        final ProducerRecord outputRecord = testDriver.readOutput(SINK_TOPIC);

        assertEquals(key1, outputRecord.key());
        assertEquals(value1, outputRecord.value());
        assertEquals(SINK_TOPIC, outputRecord.topic());
    }

    @Test
    public void shouldSetRecordMetadata() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology(), config);

        testDriver.process(consumerRecord1);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        assertEquals(timestamp1, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC_1, record.topic);
    }

    @Test
    public void shouldSendRecordViaCorrectSourceTopic() {
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(SOURCE_TOPIC_1, SOURCE_TOPIC_2), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        testDriver.process(consumerRecord1);

        assertEquals(1, processedRecords1.size());
        assertEquals(0, processedRecords2.size());

        Record record = processedRecords1.get(0);
        assertEquals(key1, record.key);
        assertEquals(value1, record.value);
        assertEquals(timestamp1, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC_1, record.topic);

        testDriver.process(consumerRecord2);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        record = processedRecords2.get(0);
        assertEquals(key2, record.key);
        assertEquals(value2, record.value);
        assertEquals(timestamp2, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC_2, record.topic);
    }

    @Test
    public void shouldProcessConsumerRecordList() {
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(SOURCE_TOPIC_1, SOURCE_TOPIC_2), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        final List<ConsumerRecord<byte[], byte[]>> testRecords = new ArrayList<>(2);
        testRecords.add(consumerRecord1);
        testRecords.add(consumerRecord2);

        testDriver.process(testRecords);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        Record record = processedRecords1.get(0);
        assertEquals(key1, record.key);
        assertEquals(value1, record.value);
        assertEquals(timestamp1, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC_1, record.topic);

        record = processedRecords2.get(0);
        assertEquals(key2, record.key);
        assertEquals(value2, record.value);
        assertEquals(timestamp2, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC_2, record.topic);
    }

    @Test
    public void shouldPopulateGlobalStore() {
        testDriver = new TopologyTestDriver(setupGlobalStoreTopology(SOURCE_TOPIC_1), config);

        testDriver.process(consumerRecord1);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        assertEquals(timestamp1, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC_1, record.topic);
    }

}
