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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopologyTestDriverTest {
    private final static String SOURCE_TOPIC = "source-topic";
    private final static String SINK_TOPIC = "sink-topic";

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

    private final class MockProcessor implements Processor {
        boolean initialized = false;
        boolean closed = false;
        ProcessorContext context;
        final List<Record> processedRecords = new ArrayList<>();

        @Override
        public void init(ProcessorContext context) {
            initialized = true;
            this.context = context;
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

        topology.addSource(sourceName, SOURCE_TOPIC);
        topology.addSink("sink", SINK_TOPIC, sourceName);

        return topology;
    }

    private Topology setupSingleProcessorTopology() {
        final Topology topology = new Topology();

        final String sourceName = "source";

        topology.addSource(sourceName, SOURCE_TOPIC);
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
        testDriver = new TopologyTestDriver(new Topology(), config);

        final String unknownTopic = "unknownTopic";
        try {
            testDriver.process(unknownTopic, null, (byte[]) null);
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException exception) {
            assertEquals("Unknown topic: " + unknownTopic, exception.getMessage());
        }
    }

    @Test
    public void shouldProcessRecordForTopic() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology(), config);

        final byte[] key = new byte[0];
        final byte[] value = new byte[0];

        testDriver.process(SOURCE_TOPIC, key, value);
        final ProducerRecord outputRecord = testDriver.readOutput(SINK_TOPIC);

        assertEquals(key, outputRecord.key());
        assertEquals(value, outputRecord.value());
        assertEquals(SINK_TOPIC, outputRecord.topic());
    }

    @Test
    public void shouldSetRecordMetadata() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology(), config);

        final byte[] key = new byte[0];
        final byte[] value = new byte[0];
        final long timestamp = 42L;

        testDriver.process(SOURCE_TOPIC, key, value, timestamp);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        assertEquals(timestamp, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC, record.topic);
    }

    @Test
    public void shouldSerializeKeyAndValue() {
        testDriver = new TopologyTestDriver(setupSingleProcessorTopology(), config);

        final Serializer<String> stringSerializer = new StringSerializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final String key = "key";
        final Long value = 21L;

        testDriver.process(key, value, stringSerializer, longSerializer);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        assertArrayEquals(stringSerializer.serialize(SINK_TOPIC, key), (byte[]) record.key);
        assertArrayEquals(longSerializer.serialize(SINK_TOPIC, value), (byte[]) record.value);
    }

    @Test
    public void shouldProcessWithoutSpecifiedTopicName() {
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(SOURCE_TOPIC), config);

        testDriver.process((byte[]) null, (byte[]) null);

        assertEquals(1, mockProcessors.get(0).processedRecords.size());
    }

    @Test
    public void shouldSendRecordViaCorrectSourceTopic() {
        final String[] sourceTopics = new String[] {
            SOURCE_TOPIC + "-1",
            SOURCE_TOPIC + "-2",
        };
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(sourceTopics), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        final byte[][] keys = new byte[][] {new byte[0], new byte[0]};
        final byte[][] values = new byte[][] {new byte[0], new byte[0]};
        final long[] timestamps = new long[] {42L, 43L};

        testDriver.process(sourceTopics[0], keys[0], values[0], timestamps[0]);

        assertEquals(1, processedRecords1.size());
        assertEquals(0, processedRecords2.size());

        Record record = processedRecords1.get(0);
        assertEquals(keys[0], record.key);
        assertEquals(values[0], record.value);
        assertEquals(timestamps[0], record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(sourceTopics[0], record.topic);

        testDriver.process(sourceTopics[1], keys[1], values[1], timestamps[1]);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        record = processedRecords2.get(0);
        assertEquals(keys[1], record.key);
        assertEquals(values[1], record.value);
        assertEquals(timestamps[1], record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(sourceTopics[1], record.topic);
    }

    @Test
    public void shouldProcessTestRecord() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology(), config);

        final TestRecord testRecord = new TestRecord(SOURCE_TOPIC, new byte[0], new byte[0], 42);

        testDriver.process(testRecord);
        final ProducerRecord outputRecord = testDriver.readOutput(SINK_TOPIC);

        assertEquals(testRecord.key(), outputRecord.key());
        assertEquals(testRecord.value(), outputRecord.value());
    }

    @Test
    public void shouldProcessTestRecordCollection() {
        final String[] sourceTopics = new String[]{
            SOURCE_TOPIC + "-1",
            SOURCE_TOPIC + "-2",
        };
        testDriver = new TopologyTestDriver(setupMultipleSourceTopology(sourceTopics), config);

        final List<Record> processedRecords1 = mockProcessors.get(0).processedRecords;
        final List<Record> processedRecords2 = mockProcessors.get(1).processedRecords;

        final TestRecord testRecord1 = new TestRecord(sourceTopics[0], new byte[0], new byte[0], 42);
        final TestRecord testRecord2 = new TestRecord(sourceTopics[1], new byte[0], new byte[0], 21);

        final List<TestRecord> testRecords = new ArrayList<>(2);
        testRecords.add(testRecord1);
        testRecords.add(testRecord2);

        testDriver.process(testRecords);

        assertEquals(1, processedRecords1.size());
        assertEquals(1, processedRecords2.size());

        Record record = processedRecords1.get(0);
        assertEquals(testRecord1.key(), record.key);
        assertEquals(testRecord1.value(), record.value);
        assertEquals(testRecord1.timestamp(), record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(testRecord1.topicName(), record.topic);

        record = processedRecords2.get(0);
        assertEquals(testRecord2.key(), record.key);
        assertEquals(testRecord2.value(), record.value);
        assertEquals(testRecord2.timestamp(), record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(testRecord2.topicName(), record.topic);
    }

    @Test
    public void shouldPopulateGlobalStore() {
        testDriver = new TopologyTestDriver(setupGlobalStoreTopology(SOURCE_TOPIC), config);

        final byte[] key = new byte[0];
        final byte[] value = new byte[0];
        final long timestamp = 42L;

        testDriver.process(SOURCE_TOPIC, key, value, timestamp);

        final List<Record> processedRecords = mockProcessors.get(0).processedRecords;
        assertEquals(1, processedRecords.size());

        final Record record = processedRecords.get(0);
        assertEquals(timestamp, record.timestamp);
        assertEquals(0, record.offset);
        assertEquals(SOURCE_TOPIC, record.topic);
    }

    @Test
    public void shouldPopulateGlobalStoreWithoutSpecifiedTopicName() {
        testDriver = new TopologyTestDriver(setupGlobalStoreTopology(SOURCE_TOPIC), config);

        testDriver.process((byte[]) null, (byte[]) null);

        assertEquals(1, mockProcessors.get(0).processedRecords.size());
    }

}
