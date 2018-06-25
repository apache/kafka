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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ProcessorTopologyTest {

    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

    private static final String INPUT_TOPIC_1 = "input-topic-1";
    private static final String INPUT_TOPIC_2 = "input-topic-2";
    private static final String OUTPUT_TOPIC_1 = "output-topic-1";
    private static final String OUTPUT_TOPIC_2 = "output-topic-2";
    private static final String THROUGH_TOPIC_1 = "through-topic-1";

    private static final Header HEADER = new RecordHeader("key", "value".getBytes());
    private static final Headers HEADERS = new RecordHeaders(new Header[]{HEADER});

    private final TopologyWrapper topology = new TopologyWrapper();
    private final MockProcessorSupplier mockProcessorSupplier = new MockProcessorSupplier();
    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER, 0L);

    private TopologyTestDriver driver;
    private final Properties props = new Properties();


    @Before
    public void setup() {
        // Create a new directory in which we'll put all of the state for this test, enabling running tests in parallel ...
        final File localState = TestUtils.tempDirectory();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "processor-topology-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, localState.getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
    }

    @After
    public void cleanup() {
        props.clear();
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testTopologyMetadata() {
        topology.setApplicationId("X");

        topology.addSource("source-1", "topic-1");
        topology.addSource("source-2", "topic-2", "topic-3");
        topology.addProcessor("processor-1", new MockProcessorSupplier<>(), "source-1");
        topology.addProcessor("processor-2", new MockProcessorSupplier<>(), "source-1", "source-2");
        topology.addSink("sink-1", "topic-3", "processor-1");
        topology.addSink("sink-2", "topic-4", "processor-1", "processor-2");

        final ProcessorTopology processorTopology = topology.getInternalBuilder().build();

        assertEquals(6, processorTopology.processors().size());

        assertEquals(2, processorTopology.sources().size());

        assertEquals(3, processorTopology.sourceTopics().size());

        assertNotNull(processorTopology.source("topic-1"));

        assertNotNull(processorTopology.source("topic-2"));

        assertNotNull(processorTopology.source("topic-3"));

        assertEquals(processorTopology.source("topic-2"), processorTopology.source("topic-3"));
    }

    @Test
    public void testDrivingSimpleTopology() {
        int partition = 10;
        driver = new TopologyTestDriver(createSimpleTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition);
        assertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition);
        assertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key4", "value4"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key5", "value5"));
        assertNoOutputRecord(OUTPUT_TOPIC_2);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4", partition);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5", partition);
    }


    @Test
    public void testDrivingMultiplexingTopology() {
        driver = new TopologyTestDriver(createMultiplexingTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key4", "value4"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key5", "value5"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
    }

    @Test
    public void testDrivingMultiplexByNameTopology() {
        driver = new TopologyTestDriver(createMultiplexByNameTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key4", "value4"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key5", "value5"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
    }

    @Test
    public void testDrivingStatefulTopology() {
        String storeName = "entries";
        driver = new TopologyTestDriver(createStatefulTopology(storeName), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value4"));
        assertNoOutputRecord(OUTPUT_TOPIC_1);

        final KeyValueStore<String, String> store = driver.getKeyValueStore(storeName);
        assertEquals("value4", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    @Test
    public void shouldDriveGlobalStore() {
        final String storeName = "my-store";
        final String global = "global";
        final String topic = "topic";

        topology.addGlobalStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()).withLoggingDisabled(),
                global, STRING_DESERIALIZER, STRING_DESERIALIZER, topic, "processor", define(new StatefulProcessor(storeName)));

        driver = new TopologyTestDriver(topology, props);
        final KeyValueStore<String, String> globalStore = driver.getKeyValueStore(storeName);
        driver.pipeInput(recordFactory.create(topic, "key1", "value1"));
        driver.pipeInput(recordFactory.create(topic, "key2", "value2"));
        assertEquals("value1", globalStore.get("key1"));
        assertEquals("value2", globalStore.get("key2"));
    }

    @Test
    public void testDrivingSimpleMultiSourceTopology() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleMultiSourceTopology(partition), props);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition);
        assertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.pipeInput(recordFactory.create(INPUT_TOPIC_2, "key2", "value2"));
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2", partition);
        assertNoOutputRecord(OUTPUT_TOPIC_1);
    }

    @Test
    public void testDrivingForwardToSourceTopology() {
        driver = new TopologyTestDriver(createForwardToSourceTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3");
    }

    @Test
    public void testDrivingInternalRepartitioningTopology() {
        driver = new TopologyTestDriver(createInternalRepartitioningTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3"));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3");
    }

    @Test
    public void testDrivingInternalRepartitioningForwardingTimestampTopology() {
        driver = new TopologyTestDriver(createInternalRepartitioningWithValueTimestampTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1@1000"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2@2000"));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3@3000"));
        assertThat(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
                equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 1000L, "key1", "value1")));
        assertThat(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
                equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 2000L, "key2", "value2")));
        assertThat(driver.readOutput(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER),
                equalTo(new ProducerRecord<>(OUTPUT_TOPIC_1, null, 3000L, "key3", "value3")));
    }

    @Test
    public void shouldCreateStringWithSourceAndTopics() {
        topology.addSource("source", "topic1", "topic2");
        final ProcessorTopology processorTopology = topology.getInternalBuilder().build();
        final String result = processorTopology.toString();
        assertThat(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
    }

    @Test
    public void shouldCreateStringWithMultipleSourcesAndTopics() {
        topology.addSource("source", "topic1", "topic2");
        topology.addSource("source2", "t", "t1", "t2");
        final ProcessorTopology processorTopology = topology.getInternalBuilder().build();
        final String result = processorTopology.toString();
        assertThat(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
        assertThat(result, containsString("source2:\n\t\ttopics:\t\t[t, t1, t2]\n"));
    }

    @Test
    public void shouldCreateStringWithProcessors() {
        topology.addSource("source", "t")
                .addProcessor("processor", mockProcessorSupplier, "source")
                .addProcessor("other", mockProcessorSupplier, "source");
        final ProcessorTopology processorTopology = topology.getInternalBuilder().build();
        final String result = processorTopology.toString();
        assertThat(result, containsString("\t\tchildren:\t[processor, other]"));
        assertThat(result, containsString("processor:\n"));
        assertThat(result, containsString("other:\n"));
    }

    @Test
    public void shouldRecursivelyPrintChildren() {
        topology.addSource("source", "t")
                .addProcessor("processor", mockProcessorSupplier, "source")
                .addProcessor("child-one", mockProcessorSupplier, "processor")
                .addProcessor("child-one-one", mockProcessorSupplier, "child-one")
                .addProcessor("child-two", mockProcessorSupplier, "processor")
                .addProcessor("child-two-one", mockProcessorSupplier, "child-two");

        final String result = topology.getInternalBuilder().build().toString();
        assertThat(result, containsString("child-one:\n\t\tchildren:\t[child-one-one]"));
        assertThat(result, containsString("child-two:\n\t\tchildren:\t[child-two-one]"));
    }

    @Test
    public void shouldConsiderTimeStamps() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", 30L));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 10L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 20L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition, 30L);
    }

    @Test
    public void shouldConsiderModifiedTimeStamps() {
        final int partition = 10;
        driver = new TopologyTestDriver(createTimestampTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", 30L));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", partition, 20L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", partition, 30L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", partition, 40L);
    }

    @Test
    public void shouldConsiderHeaders() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleTopology(partition), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", HEADERS, 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", HEADERS, 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", HEADERS, 30L));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", HEADERS, partition, 10L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", HEADERS, partition, 20L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", HEADERS, partition, 30L);
    }

    @Test
    public void shouldAddHeaders() {
        driver = new TopologyTestDriver(createAddHeaderTopology(), props);
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key1", "value1", 10L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key2", "value2", 20L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC_1, "key3", "value3", 30L));
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1", HEADERS, 10L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2", HEADERS, 20L);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3", HEADERS, 30L);
    }

    private void assertNextOutputRecord(final String topic,
                                        final String key,
                                        final String value) {
        assertNextOutputRecord(topic, key, value, (Integer) null, 0L);
    }

    private void assertNextOutputRecord(final String topic,
                                        final String key,
                                        final String value,
                                        final Integer partition) {
        assertNextOutputRecord(topic, key, value, partition, 0L);
    }

    private void assertNextOutputRecord(final String topic,
                                        final String key,
                                        final String value,
                                        final Headers headers,
                                        final Long timestamp) {
        assertNextOutputRecord(topic, key, value, headers, null, timestamp);
    }

    private void assertNextOutputRecord(final String topic,
                                        final String key,
                                        final String value,
                                        final Integer partition,
                                        final Long timestamp) {
        assertNextOutputRecord(topic, key, value, new RecordHeaders(), partition, timestamp);
    }

    private void assertNextOutputRecord(final String topic,
                                        final String key,
                                        final String value,
                                        final Headers headers,
                                        final Integer partition,
                                        final Long timestamp) {
        final ProducerRecord<String, String> record = driver.readOutput(topic, STRING_DESERIALIZER, STRING_DESERIALIZER);
        assertEquals(topic, record.topic());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(partition, record.partition());
        assertEquals(timestamp, record.timestamp());
        assertEquals(headers, record.headers());
    }

    private void assertNoOutputRecord(final String topic) {
        assertNull(driver.readOutput(topic));
    }

    private StreamPartitioner<Object, Object> constantPartitioner(final Integer partition) {
        return new StreamPartitioner<Object, Object>() {
            @Override
            public Integer partition(final String topic, final Object key, final Object value, final int numPartitions) {
                return partition;
            }
        };
    }

    private Topology createSimpleTopology(final int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new ForwardingProcessor()), "source")
            .addSink("sink", OUTPUT_TOPIC_1, constantPartitioner(partition), "processor");
    }

    private Topology createTimestampTopology(final int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new TimestampProcessor()), "source")
            .addSink("sink", OUTPUT_TOPIC_1, constantPartitioner(partition), "processor");
    }

    private Topology createMultiplexingTopology() {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new MultiplexingProcessor(2)), "source")
            .addSink("sink1", OUTPUT_TOPIC_1, "processor")
            .addSink("sink2", OUTPUT_TOPIC_2, "processor");
    }

    private Topology createMultiplexByNameTopology() {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new MultiplexByNameProcessor(2)), "source")
            .addSink("sink0", OUTPUT_TOPIC_1, "processor")
            .addSink("sink1", OUTPUT_TOPIC_2, "processor");
    }

    private Topology createStatefulTopology(final String storeName) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new StatefulProcessor(storeName)), "source")
            .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()), "processor")
            .addSink("counts", OUTPUT_TOPIC_1, "processor");
    }

    private Topology createInternalRepartitioningTopology() {
        topology.addSource("source", INPUT_TOPIC_1)
            .addSink("sink0", THROUGH_TOPIC_1, "source")
            .addSource("source1", THROUGH_TOPIC_1)
            .addSink("sink1", OUTPUT_TOPIC_1, "source1");

        // use wrapper to get the internal topology builder to add internal topic
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.addInternalTopic(THROUGH_TOPIC_1);

        return topology;
    }

    private Topology createInternalRepartitioningWithValueTimestampTopology() {
        topology.addSource("source", INPUT_TOPIC_1)
                .addProcessor("processor", define(new ValueTimestampProcessor()), "source")
                .addSink("sink0", THROUGH_TOPIC_1, "processor")
                .addSource("source1", THROUGH_TOPIC_1)
                .addSink("sink1", OUTPUT_TOPIC_1, "source1");

        // use wrapper to get the internal topology builder to add internal topic
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.addInternalTopic(THROUGH_TOPIC_1);

        return topology;
    }

    private Topology createForwardToSourceTopology() {
        return topology.addSource("source-1", INPUT_TOPIC_1)
                .addSink("sink-1", OUTPUT_TOPIC_1, "source-1")
                .addSource("source-2", OUTPUT_TOPIC_1)
                .addSink("sink-2", OUTPUT_TOPIC_2, "source-2");
    }

    private Topology createSimpleMultiSourceTopology(int partition) {
        return topology.addSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
                .addProcessor("processor-1", define(new ForwardingProcessor()), "source-1")
                .addSink("sink-1", OUTPUT_TOPIC_1, constantPartitioner(partition), "processor-1")
                .addSource("source-2", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_2)
                .addProcessor("processor-2", define(new ForwardingProcessor()), "source-2")
                .addSink("sink-2", OUTPUT_TOPIC_2, constantPartitioner(partition), "processor-2");
    }

    private Topology createAddHeaderTopology() {
        return topology.addSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
                .addProcessor("processor-1", define(new AddHeaderProcessor()), "source-1")
                .addSink("sink-1", OUTPUT_TOPIC_1, "processor-1");
    }

    /**
     * A processor that simply forwards all messages to all children.
     */
    protected static class ForwardingProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(final String key, final String value) {
            context().forward(key, value);
        }
    }

    /**
     * A processor that simply forwards all messages to all children with advanced timestamps.
     */
    protected static class TimestampProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(final String key, final String value) {
            context().forward(key, value, To.all().withTimestamp(context().timestamp() + 10));
        }
    }

    protected static class AddHeaderProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(final String key, final String value) {
            context().headers().add(HEADER);
            context().forward(key, value);
        }
    }

    /**
     * A processor that removes custom timestamp information from messages and forwards modified messages to each child.
     * A message contains custom timestamp information if the value is in ".*@[0-9]+" format.
     */
    protected static class ValueTimestampProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(final String key, final String value) {
            context().forward(key, value.split("@")[0]);
        }
    }

    /**
     * A processor that forwards slightly-modified messages to each child.
     */
    protected static class MultiplexingProcessor extends AbstractProcessor<String, String> {
        private final int numChildren;

        MultiplexingProcessor(final int numChildren) {
            this.numChildren = numChildren;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void process(final String key, final String value) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(key, value + "(" + (i + 1) + ")", i);
            }
        }
    }

    /**
     * A processor that forwards slightly-modified messages to each named child.
     * Note: the children are assumed to be named "sink{child number}", e.g., sink1, or sink2, etc.
     */
    protected static class MultiplexByNameProcessor extends AbstractProcessor<String, String> {
        private final int numChildren;

        MultiplexByNameProcessor(final int numChildren) {
            this.numChildren = numChildren;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void process(final String key, final String value) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(key, value + "(" + (i + 1) + ")",  "sink" + i);
            }
        }
    }

    /**
     * A processor that stores each key-value pair in an in-memory key-value store registered with the context.
     */
    protected static class StatefulProcessor extends AbstractProcessor<String, String> {
        private KeyValueStore<String, String> store;
        private final String storeName;

        StatefulProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<String, String>) context.getStateStore(storeName);
        }

        @Override
        public void process(final String key, final String value) {
            store.put(key, value);
        }

        @Override
        public void close() {
            store.close();
        }
    }

    private <K, V> ProcessorSupplier<K, V> define(final Processor<K, V> processor) {
        return new ProcessorSupplier<K, V>() {
            @Override
            public Processor<K, V> get() {
                return processor;
            }
        };
    }

    /**
     * A custom timestamp extractor that extracts the timestamp from the record's value if the value is in ".*@[0-9]+"
     * format. Otherwise, it returns the record's timestamp or the default timestamp if the record's timestamp is negative.
    */
    public static class CustomTimestampExtractor implements TimestampExtractor {
        private static final long DEFAULT_TIMESTAMP = 1000L;

        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
            if (record.value().toString().matches(".*@[0-9]+"))
                return Long.parseLong(record.value().toString().split("@")[1]);

            if (record.timestamp() >= 0L)
                return record.timestamp();

            return DEFAULT_TIMESTAMP;
        }
    }
}
