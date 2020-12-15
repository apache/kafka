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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
    private final MockApiProcessorSupplier<?, ?, ?, ?> mockProcessorSupplier = new MockApiProcessorSupplier<>();

    private TopologyTestDriver driver;
    private final Properties props = new Properties();

    @Before
    public void setup() {
        // Create a new directory in which we'll put all of the state for this test, enabling running tests in parallel ...
        final File localState = TestUtils.tempDirectory();
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
        topology.addSource("source-1", "topic-1");
        topology.addSource("source-2", "topic-2", "topic-3");
        topology.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");
        topology.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-1", "source-2");
        topology.addSink("sink-1", "topic-3", "processor-1");
        topology.addSink("sink-2", "topic-4", "processor-1", "processor-2");

        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();

        assertEquals(6, processorTopology.processors().size());

        assertEquals(2, processorTopology.sources().size());

        assertEquals(3, processorTopology.sourceTopics().size());

        assertNotNull(processorTopology.source("topic-1"));

        assertNotNull(processorTopology.source("topic-2"));

        assertNotNull(processorTopology.source("topic-3"));

        assertEquals(processorTopology.source("topic-2"), processorTopology.source("topic-3"));
    }

    @Test
    public void shouldGetTerminalNodes() {
        topology.addSource("source-1", "topic-1");
        topology.addSource("source-2", "topic-2", "topic-3");
        topology.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");
        topology.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-1", "source-2");
        topology.addSink("sink-1", "topic-3", "processor-1");

        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();

        assertThat(processorTopology.terminalNodes(), equalTo(mkSet("processor-2", "sink-1")));
    }

    @Test
    public void shouldUpdateSourceTopicsWithNewMatchingTopic() {
        final String sourceNode = "source-1";
        final String topic = "topic-1";
        final String newTopic = "topic-2";
        topology.addSource(sourceNode, topic);
        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();
        assertThat(processorTopology.source(newTopic), is(nullValue()));

        processorTopology.updateSourceTopics(Collections.singletonMap(sourceNode, asList(topic, newTopic)));

        assertThat(processorTopology.source(newTopic).name(), equalTo(sourceNode));
    }

    @Test
    public void shouldUpdateSourceTopicsWithRemovedTopic() {
        final String sourceNode = "source-1";
        final String topic = "topic-1";
        final String topicToRemove = "topic-2";
        topology.addSource(sourceNode, topic, topicToRemove);
        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();
        assertThat(processorTopology.source(topicToRemove).name(), equalTo(sourceNode));

        processorTopology.updateSourceTopics(Collections.singletonMap(sourceNode, Collections.singletonList(topic)));

        assertThat(processorTopology.source(topicToRemove), is(nullValue()));
    }

    @Test
    public void shouldUpdateSourceTopicsWithAllTopicsRemoved() {
        final String sourceNode = "source-1";
        final String topic = "topic-1";
        topology.addSource(sourceNode, topic);
        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();
        assertThat(processorTopology.source(topic).name(), equalTo(sourceNode));

        processorTopology.updateSourceTopics(Collections.singletonMap(sourceNode, Collections.emptyList()));

        assertThat(processorTopology.source(topic), is(nullValue()));
    }

    @Test
    public void shouldUpdateSourceTopicsOnlyForSourceNodesWithinTheSubtopology() {
        final String sourceNodeWithinSubtopology = "source-1";
        final String sourceNodeOutsideSubtopology = "source-2";
        final String topicWithinSubtopology = "topic-1";
        final String topicOutsideSubtopology = "topic-2";
        topology.addSource(sourceNodeWithinSubtopology, topicWithinSubtopology);
        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();

        processorTopology.updateSourceTopics(mkMap(
            mkEntry(sourceNodeWithinSubtopology, Collections.singletonList(topicWithinSubtopology)),
            mkEntry(sourceNodeOutsideSubtopology, Collections.singletonList(topicOutsideSubtopology))
            )
        );

        assertThat(processorTopology.source(topicOutsideSubtopology), is(nullValue()));
        assertThat(processorTopology.sources().size(), equalTo(1));
    }

    @Test
    public void shouldThrowIfSourceNodeToUpdateDoesNotExist() {
        final String existingSourceNode = "source-1";
        final String nonExistingSourceNode = "source-2";
        final String topicOfExistingSourceNode = "topic-1";
        final String topicOfNonExistingSourceNode = "topic-2";
        topology.addSource(nonExistingSourceNode, topicOfNonExistingSourceNode);
        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> processorTopology.updateSourceTopics(Collections.singletonMap(
                existingSourceNode, Collections.singletonList(topicOfExistingSourceNode)
            ))
        );
        assertThat(exception.getMessage(), is("Node " + nonExistingSourceNode + " not found in full topology"));
    }

    @Test
    public void shouldThrowIfMultipleSourceNodeOfSameSubtopologySubscribedToSameTopic() {
        final String sourceNode = "source-1";
        final String updatedSourceNode = "source-2";
        final String doublySubscribedTopic = "topic-1";
        final String topic = "topic-2";
        topology.addSource(sourceNode, doublySubscribedTopic);
        topology.addSource(updatedSourceNode, topic);
        final ProcessorTopology processorTopology = topology.getInternalBuilder("X").buildTopology();

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> processorTopology.updateSourceTopics(mkMap(
                mkEntry(sourceNode, Collections.singletonList(doublySubscribedTopic)),
                mkEntry(updatedSourceNode, Arrays.asList(topic, doublySubscribedTopic))
            ))
        );
        assertThat(
            exception.getMessage(),
            startsWith("Topic " + doublySubscribedTopic + " was already registered to source node")
        );
    }

    @Test
    public void testDrivingSimpleTopology() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleTopology(partition), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1");
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1");
        assertTrue(outputTopic1.isEmpty());

        inputTopic.pipeInput("key2", "value2");
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2");
        assertTrue(outputTopic1.isEmpty());

        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key4", "value4");
        inputTopic.pipeInput("key5", "value5");
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3");
        assertNextOutputRecord(outputTopic1.readRecord(), "key4", "value4");
        assertNextOutputRecord(outputTopic1.readRecord(), "key5", "value5");
        assertTrue(outputTopic1.isEmpty());
    }


    @Test
    public void testDrivingMultiplexingTopology() {
        driver = new TopologyTestDriver(createMultiplexingTopology(), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());
        final TestOutputTopic<String, String> outputTopic2 =
                driver.createOutputTopic(OUTPUT_TOPIC_2, Serdes.String().deserializer(), Serdes.String().deserializer());
        inputTopic.pipeInput("key1", "value1");
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1(1)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key1", "value1(2)");

        inputTopic.pipeInput("key2", "value2");
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2(1)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2(2)");

        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key4", "value4");
        inputTopic.pipeInput("key5", "value5");
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3(1)");
        assertNextOutputRecord(outputTopic1.readRecord(), "key4", "value4(1)");
        assertNextOutputRecord(outputTopic1.readRecord(), "key5", "value5(1)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key3", "value3(2)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key4", "value4(2)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key5", "value5(2)");
    }

    @Test
    public void testDrivingMultiplexByNameTopology() {
        driver = new TopologyTestDriver(createMultiplexByNameTopology(), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTopic.pipeInput("key1", "value1");
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());
        final TestOutputTopic<String, String> outputTopic2 =
                driver.createOutputTopic(OUTPUT_TOPIC_2, Serdes.String().deserializer(), Serdes.String().deserializer());
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1(1)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key1", "value1(2)");

        inputTopic.pipeInput("key2", "value2");
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2(1)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2(2)");

        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key4", "value4");
        inputTopic.pipeInput("key5", "value5");
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3(1)");
        assertNextOutputRecord(outputTopic1.readRecord(), "key4", "value4(1)");
        assertNextOutputRecord(outputTopic1.readRecord(), "key5", "value5(1)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key3", "value3(2)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key4", "value4(2)");
        assertNextOutputRecord(outputTopic2.readRecord(), "key5", "value5(2)");
    }

    @Test
    public void testDrivingStatefulTopology() {
        final String storeName = "entries";
        driver = new TopologyTestDriver(createStatefulTopology(storeName), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        final TestOutputTopic<Integer, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.Integer().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key1", "value4");
        assertTrue(outputTopic1.isEmpty());

        final KeyValueStore<String, String> store = driver.getKeyValueStore(storeName);
        assertEquals("value4", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    @Test
    public void testDrivingConnectedStateStoreTopology() {
        driver = new TopologyTestDriver(createConnectedStateStoreTopology("connectedStore"), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        final TestOutputTopic<Integer, String> outputTopic1 =
            driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.Integer().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key1", "value4");
        assertTrue(outputTopic1.isEmpty());

        final KeyValueStore<String, String> store = driver.getKeyValueStore("connectedStore");
        assertEquals("value4", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    @Deprecated // testing old PAPI
    @Test
    public void testDrivingConnectedStateStoreInDifferentProcessorsTopologyWithOldAPI() {
        final String storeName = "connectedStore";
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
            Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String());
        topology
            .addSource("source1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addSource("source2", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_2)
            .addProcessor("processor1", defineWithStoresOldAPI(() -> new OldAPIStatefulProcessor(storeName), Collections.singleton(storeBuilder)), "source1")
            .addProcessor("processor2", defineWithStoresOldAPI(() -> new OldAPIStatefulProcessor(storeName), Collections.singleton(storeBuilder)), "source2")
            .addSink("counts", OUTPUT_TOPIC_1, "processor1", "processor2");

        driver = new TopologyTestDriver(topology, props);

        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        final TestOutputTopic<Integer, String> outputTopic1 =
            driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.Integer().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key1", "value4");
        assertTrue(outputTopic1.isEmpty());

        final KeyValueStore<String, String> store = driver.getKeyValueStore("connectedStore");
        assertEquals("value4", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    @Test
    public void testDrivingConnectedStateStoreInDifferentProcessorsTopology() {
        final String storeName = "connectedStore";
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
            Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String());
        topology
            .addSource("source1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addSource("source2", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_2)
            .addProcessor("processor1", defineWithStores(() -> new StatefulProcessor(storeName), Collections.singleton(storeBuilder)), "source1")
            .addProcessor("processor2", defineWithStores(() -> new StatefulProcessor(storeName), Collections.singleton(storeBuilder)), "source2")
            .addSink("counts", OUTPUT_TOPIC_1, "processor1", "processor2");

        driver = new TopologyTestDriver(topology, props);

        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        final TestOutputTopic<Integer, String> outputTopic1 =
            driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.Integer().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        inputTopic.pipeInput("key3", "value3");
        inputTopic.pipeInput("key1", "value4");
        assertTrue(outputTopic1.isEmpty());

        final KeyValueStore<String, String> store = driver.getKeyValueStore("connectedStore");
        assertEquals("value4", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    @Deprecated // testing old PAPI
    @Test
    public void shouldDriveGlobalStore() {
        final String storeName = "my-store";
        final String global = "global";
        final String topic = "topic";

        topology.addGlobalStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                Serdes.String(),
                Serdes.String()
            ).withLoggingDisabled(),
            global,
            STRING_DESERIALIZER,
            STRING_DESERIALIZER,
            topic,
            "processor",
            define(new OldAPIStatefulProcessor(storeName)));

        driver = new TopologyTestDriver(topology, props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(topic, STRING_SERIALIZER, STRING_SERIALIZER);
        final KeyValueStore<String, String> globalStore = driver.getKeyValueStore(storeName);
        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        assertEquals("value1", globalStore.get("key1"));
        assertEquals("value2", globalStore.get("key2"));
    }

    @Test
    public void testDrivingSimpleMultiSourceTopology() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleMultiSourceTopology(partition), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());
        final TestOutputTopic<String, String> outputTopic2 =
                driver.createOutputTopic(OUTPUT_TOPIC_2, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1");
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1");
        assertTrue(outputTopic2.isEmpty());

        final TestInputTopic<String, String> inputTopic2 = driver.createInputTopic(INPUT_TOPIC_2, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTopic2.pipeInput("key2", "value2");
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2");
        assertTrue(outputTopic2.isEmpty());
    }

    @Test
    public void testDrivingForwardToSourceTopology() {
        driver = new TopologyTestDriver(createForwardToSourceTopology(), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        inputTopic.pipeInput("key3", "value3");
        final TestOutputTopic<String, String> outputTopic2 =
                driver.createOutputTopic(OUTPUT_TOPIC_2, Serdes.String().deserializer(), Serdes.String().deserializer());
        assertNextOutputRecord(outputTopic2.readRecord(), "key1", "value1");
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2");
        assertNextOutputRecord(outputTopic2.readRecord(), "key3", "value3");
    }

    @Test
    public void testDrivingInternalRepartitioningTopology() {
        driver = new TopologyTestDriver(createInternalRepartitioningTopology(), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER, Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key2", "value2");
        inputTopic.pipeInput("key3", "value3");
        final TestOutputTopic<String, String> outputTopic1 = driver.createOutputTopic(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER);
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1");
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2");
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3");
    }

    @Test
    public void testDrivingInternalRepartitioningForwardingTimestampTopology() {
        driver = new TopologyTestDriver(createInternalRepartitioningWithValueTimestampTopology(), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        inputTopic.pipeInput("key1", "value1@1000");
        inputTopic.pipeInput("key2", "value2@2000");
        inputTopic.pipeInput("key3", "value3@3000");
        final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER);
        assertThat(outputTopic.readRecord(),
                equalTo(new TestRecord<>("key1", "value1", null, 1000L)));
        assertThat(outputTopic.readRecord(),
                equalTo(new TestRecord<>("key2", "value2", null, 2000L)));
        assertThat(outputTopic.readRecord(),
                equalTo(new TestRecord<>("key3", "value3", null, 3000L)));
    }

    @Test
    public void shouldCreateStringWithSourceAndTopics() {
        topology.addSource("source", "topic1", "topic2");
        final ProcessorTopology processorTopology = topology.getInternalBuilder().buildTopology();
        final String result = processorTopology.toString();
        assertThat(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
    }

    @Test
    public void shouldCreateStringWithMultipleSourcesAndTopics() {
        topology.addSource("source", "topic1", "topic2");
        topology.addSource("source2", "t", "t1", "t2");
        final ProcessorTopology processorTopology = topology.getInternalBuilder().buildTopology();
        final String result = processorTopology.toString();
        assertThat(result, containsString("source:\n\t\ttopics:\t\t[topic1, topic2]\n"));
        assertThat(result, containsString("source2:\n\t\ttopics:\t\t[t, t1, t2]\n"));
    }

    @Test
    public void shouldCreateStringWithProcessors() {
        topology.addSource("source", "t")
                .addProcessor("processor", mockProcessorSupplier, "source")
                .addProcessor("other", mockProcessorSupplier, "source");
        final ProcessorTopology processorTopology = topology.getInternalBuilder().buildTopology();
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

        final String result = topology.getInternalBuilder().buildTopology().toString();
        assertThat(result, containsString("child-one:\n\t\tchildren:\t[child-one-one]"));
        assertThat(result, containsString("child-two:\n\t\tchildren:\t[child-two-one]"));
    }

    @Test
    public void shouldConsiderTimeStamps() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleTopology(partition), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        inputTopic.pipeInput("key1", "value1", 10L);
        inputTopic.pipeInput("key2", "value2", 20L);
        inputTopic.pipeInput("key3", "value3", 30L);
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", 10L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3", 30L);
    }

    @Test
    public void shouldConsiderModifiedTimeStamps() {
        final int partition = 10;
        driver = new TopologyTestDriver(createTimestampTopology(partition), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        inputTopic.pipeInput("key1", "value1", 10L);
        inputTopic.pipeInput("key2", "value2", 20L);
        inputTopic.pipeInput("key3", "value3", 30L);
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", 30L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3", 40L);
    }

    @Test
    public void shouldConsiderModifiedTimeStampsForMultipleProcessors() {
        final int partition = 10;
        driver = new TopologyTestDriver(createMultiProcessorTimestampTopology(partition), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        final TestOutputTopic<String, String> outputTopic1 =
                driver.createOutputTopic(OUTPUT_TOPIC_1, Serdes.String().deserializer(), Serdes.String().deserializer());
        final TestOutputTopic<String, String> outputTopic2 =
                driver.createOutputTopic(OUTPUT_TOPIC_2, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key1", "value1", 10L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", 10L);
        assertNextOutputRecord(outputTopic2.readRecord(), "key1", "value1", 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", 15L);
        assertNextOutputRecord(outputTopic2.readRecord(), "key1", "value1", 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", 12L);
        assertNextOutputRecord(outputTopic2.readRecord(), "key1", "value1", 22L);
        assertTrue(outputTopic1.isEmpty());
        assertTrue(outputTopic2.isEmpty());

        inputTopic.pipeInput("key2", "value2", 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", 20L);
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2", 30L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", 25L);
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2", 30L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", 22L);
        assertNextOutputRecord(outputTopic2.readRecord(), "key2", "value2", 32L);
        assertTrue(outputTopic1.isEmpty());
        assertTrue(outputTopic2.isEmpty());
    }

    @Test
    public void shouldConsiderHeaders() {
        final int partition = 10;
        driver = new TopologyTestDriver(createSimpleTopology(partition), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        inputTopic.pipeInput(new TestRecord<>("key1", "value1", HEADERS, 10L));
        inputTopic.pipeInput(new TestRecord<>("key2", "value2", HEADERS, 20L));
        inputTopic.pipeInput(new TestRecord<>("key3", "value3", HEADERS, 30L));
        final TestOutputTopic<String, String> outputTopic1 = driver.createOutputTopic(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER);
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", HEADERS, 10L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", HEADERS, 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3", HEADERS, 30L);
    }

    @Test
    public void shouldAddHeaders() {
        driver = new TopologyTestDriver(createAddHeaderTopology(), props);
        final TestInputTopic<String, String> inputTopic = driver.createInputTopic(INPUT_TOPIC_1, STRING_SERIALIZER, STRING_SERIALIZER);
        inputTopic.pipeInput("key1", "value1", 10L);
        inputTopic.pipeInput("key2", "value2", 20L);
        inputTopic.pipeInput("key3", "value3", 30L);
        final TestOutputTopic<String, String> outputTopic1 = driver.createOutputTopic(OUTPUT_TOPIC_1, STRING_DESERIALIZER, STRING_DESERIALIZER);
        assertNextOutputRecord(outputTopic1.readRecord(), "key1", "value1", HEADERS, 10L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key2", "value2", HEADERS, 20L);
        assertNextOutputRecord(outputTopic1.readRecord(), "key3", "value3", HEADERS, 30L);
    }

    @Test
    public void statelessTopologyShouldNotHavePersistentStore() {
        final TopologyWrapper topology = new TopologyWrapper();
        final ProcessorTopology processorTopology = topology.getInternalBuilder("anyAppId").buildTopology();
        assertFalse(processorTopology.hasPersistentLocalStore());
        assertFalse(processorTopology.hasPersistentGlobalStore());
    }

    @Test
    public void inMemoryStoreShouldNotResultInPersistentLocalStore() {
        final ProcessorTopology processorTopology = createLocalStoreTopology(Stores.inMemoryKeyValueStore("my-store"));
        assertFalse(processorTopology.hasPersistentLocalStore());
    }

    @Test
    public void persistentLocalStoreShouldBeDetected() {
        final ProcessorTopology processorTopology = createLocalStoreTopology(Stores.persistentKeyValueStore("my-store"));
        assertTrue(processorTopology.hasPersistentLocalStore());
    }

    @Test
    public void inMemoryStoreShouldNotResultInPersistentGlobalStore() {
        final ProcessorTopology processorTopology = createGlobalStoreTopology(Stores.inMemoryKeyValueStore("my-store"));
        assertFalse(processorTopology.hasPersistentGlobalStore());
    }

    @Test
    public void persistentGlobalStoreShouldBeDetected() {
        final ProcessorTopology processorTopology = createGlobalStoreTopology(Stores.persistentKeyValueStore("my-store"));
        assertTrue(processorTopology.hasPersistentGlobalStore());
    }

    private ProcessorTopology createLocalStoreTopology(final KeyValueBytesStoreSupplier storeSupplier) {
        final TopologyWrapper topology = new TopologyWrapper();
        final String processor = "processor";
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String());
        topology.addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, "topic")
                .addProcessor(processor, () -> new StatefulProcessor(storeSupplier.name()), "source")
                .addStateStore(storeBuilder, processor);
        return topology.getInternalBuilder("anyAppId").buildTopology();
    }

    @Deprecated // testing old PAPI
    private ProcessorTopology createGlobalStoreTopology(final KeyValueBytesStoreSupplier storeSupplier) {
        final TopologyWrapper topology = new TopologyWrapper();
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(storeBuilder, "global", STRING_DESERIALIZER, STRING_DESERIALIZER, "topic", "processor",
                define(new OldAPIStatefulProcessor(storeSupplier.name())));
        return topology.getInternalBuilder("anyAppId").buildTopology();
    }

    private void assertNextOutputRecord(final TestRecord<String, String> record,
                                        final String key,
                                        final String value) {
        assertNextOutputRecord(record, key, value, 0L);
    }

    private void assertNextOutputRecord(final TestRecord<String, String> record,
                                        final String key,
                                        final String value,
                                        final Long timestamp) {
        assertNextOutputRecord(record, key, value, new RecordHeaders(), timestamp);
    }

    private void assertNextOutputRecord(final TestRecord<String, String> record,
                                        final String key,
                                        final String value,
                                        final Headers headers,
                                        final Long timestamp) {
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(timestamp, record.timestamp());
        assertEquals(headers, record.headers());
    }

    private StreamPartitioner<Object, Object> constantPartitioner(final Integer partition) {
        return (topic, key, value, numPartitions) -> partition;
    }

    private Topology createSimpleTopology(final int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", () -> (Processor<String, String, String, String>) new ForwardingProcessor(), "source")
            .addSink("sink", OUTPUT_TOPIC_1, constantPartitioner(partition), "processor");
    }

    private Topology createTimestampTopology(final int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", () -> (Processor<String, String, String, String>) new TimestampProcessor(), "source")
            .addSink("sink", OUTPUT_TOPIC_1, constantPartitioner(partition), "processor");
    }

    private Topology createMultiProcessorTimestampTopology(final int partition) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", () -> (Processor<String, String, String, String>) new FanOutTimestampProcessor("child1", "child2"), "source")
            .addProcessor("child1", () -> (Processor<String, String, String, String>) new ForwardingProcessor(), "processor")
            .addProcessor("child2", () -> (Processor<String, String, String, String>) new TimestampProcessor(), "processor")
            .addSink("sink1", OUTPUT_TOPIC_1, constantPartitioner(partition), "child1")
            .addSink("sink2", OUTPUT_TOPIC_2, constantPartitioner(partition), "child2");
    }

    @Deprecated // testing old PAPI
    private Topology createMultiplexingTopology() {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new MultiplexingProcessor(2)), "source")
            .addSink("sink1", OUTPUT_TOPIC_1, "processor")
            .addSink("sink2", OUTPUT_TOPIC_2, "processor");
    }

    @Deprecated // testing old PAPI
    private Topology createMultiplexByNameTopology() {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new MultiplexByNameProcessor(2)), "source")
            .addSink("sink0", OUTPUT_TOPIC_1, "processor")
            .addSink("sink1", OUTPUT_TOPIC_2, "processor");
    }

    @Deprecated // testing old PAPI
    private Topology createStatefulTopology(final String storeName) {
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", define(new OldAPIStatefulProcessor(storeName)), "source")
            .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String()), "processor")
            .addSink("counts", OUTPUT_TOPIC_1, "processor");
    }

    @Deprecated // testing old PAPI
    private Topology createConnectedStateStoreTopology(final String storeName) {
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String());
        return topology
            .addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
            .addProcessor("processor", defineWithStoresOldAPI(() -> new OldAPIStatefulProcessor(storeName), Collections.singleton(storeBuilder)), "source")
            .addSink("counts", OUTPUT_TOPIC_1, "processor");
    }

    private Topology createInternalRepartitioningTopology() {
        topology.addSource("source", INPUT_TOPIC_1)
            .addSink("sink0", THROUGH_TOPIC_1, "source")
            .addSource("source1", THROUGH_TOPIC_1)
            .addSink("sink1", OUTPUT_TOPIC_1, "source1");

        // use wrapper to get the internal topology builder to add internal topic
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.addInternalTopic(THROUGH_TOPIC_1, InternalTopicProperties.empty());

        return topology;
    }

    private Topology createInternalRepartitioningWithValueTimestampTopology() {
        topology.addSource("source", INPUT_TOPIC_1)
                .addProcessor("processor", () -> (Processor<String, String, String, String>) new ValueTimestampProcessor(), "source")
                .addSink("sink0", THROUGH_TOPIC_1, "processor")
                .addSource("source1", THROUGH_TOPIC_1)
                .addSink("sink1", OUTPUT_TOPIC_1, "source1");

        // use wrapper to get the internal topology builder to add internal topic
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.addInternalTopic(THROUGH_TOPIC_1, InternalTopicProperties.empty());

        return topology;
    }

    private Topology createForwardToSourceTopology() {
        return topology.addSource("source-1", INPUT_TOPIC_1)
                .addSink("sink-1", OUTPUT_TOPIC_1, "source-1")
                .addSource("source-2", OUTPUT_TOPIC_1)
                .addSink("sink-2", OUTPUT_TOPIC_2, "source-2");
    }

    private Topology createSimpleMultiSourceTopology(final int partition) {
        return topology.addSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
                .addProcessor("processor-1", () -> (Processor<String, String, String, String>) new ForwardingProcessor(), "source-1")
                .addSink("sink-1", OUTPUT_TOPIC_1, constantPartitioner(partition), "processor-1")
                .addSource("source-2", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_2)
                .addProcessor("processor-2", () -> (Processor<String, String, String, String>) new ForwardingProcessor(), "source-2")
                .addSink("sink-2", OUTPUT_TOPIC_2, constantPartitioner(partition), "processor-2");
    }

    private Topology createAddHeaderTopology() {
        return topology.addSource("source-1", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC_1)
                .addProcessor("processor-1", () -> (Processor<String, String, String, String>) new AddHeaderProcessor(), "source-1")
                .addSink("sink-1", OUTPUT_TOPIC_1, "processor-1");
    }

    /**
     * A processor that simply forwards all messages to all children.
     */
    protected static class ForwardingProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(final Record<String, String> record) {
            context.forward(record);
        }
    }

    /**
     * A processor that simply forwards all messages to all children with advanced timestamps.
     */
    protected static class TimestampProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(final Record<String, String> record) {
            context.forward(record.withTimestamp(record.timestamp() + 10));
        }
    }

    protected static class FanOutTimestampProcessor implements Processor<String, String, String, String> {
        private final String firstChild;
        private final String secondChild;
        private ProcessorContext<String, String> context;

        FanOutTimestampProcessor(final String firstChild,
                                 final String secondChild) {
            this.firstChild = firstChild;
            this.secondChild = secondChild;
        }

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(final Record<String, String> record) {
            context.forward(record);
            context.forward(record.withTimestamp(record.timestamp() + 5), firstChild);
            context.forward(record, secondChild);
            context.forward(record.withTimestamp(record.timestamp() + 2));
        }
    }

    protected static class AddHeaderProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(final Record<String, String> record) {
            // making a copy of headers for safety.
            final Record<String, String> toForward = record.withHeaders(record.headers());
            toForward.headers().add(HEADER);
            context.forward(toForward);
        }
    }

    /**
     * A processor that removes custom timestamp information from messages and forwards modified messages to each child.
     * A message contains custom timestamp information if the value is in ".*@[0-9]+" format.
     */
    protected static class ValueTimestampProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(final Record<String, String> record) {
            context.forward(record.withValue(record.value().split("@")[0]));
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

        @SuppressWarnings("deprecation") // need to test deprecated code until removed
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

        @SuppressWarnings("deprecation") // need to test deprecated code until removed
        @Override
        public void process(final String key, final String value) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(key, value + "(" + (i + 1) + ")", "sink" + i);
            }
        }
    }

    /**
     * A processor that stores each key-value pair in an in-memory key-value store registered with the context.
     */
    protected static class OldAPIStatefulProcessor extends AbstractProcessor<String, String> {
        private KeyValueStore<String, String> store;
        private final String storeName;

        OldAPIStatefulProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final org.apache.kafka.streams.processor.ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<String, String>) context.getStateStore(storeName);
        }

        @Override
        public void process(final String key, final String value) {
            store.put(key, value);
        }
    }

    /**
     * A processor that stores each key-value pair in an in-memory key-value store registered with the context.
     */
    protected static class StatefulProcessor implements Processor<String, String, Void, Void> {
        private KeyValueStore<String, String> store;
        private final String storeName;

        StatefulProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), record.value());
        }
    }

    private <K, V> org.apache.kafka.streams.processor.ProcessorSupplier<K, V> define(final org.apache.kafka.streams.processor.Processor<K, V> processor) {
        return () -> processor;
    }

    private <K, V> org.apache.kafka.streams.processor.ProcessorSupplier<K, V> defineWithStoresOldAPI(final Supplier<org.apache.kafka.streams.processor.Processor<K, V>> supplier,
                                                                                                     final Set<StoreBuilder<?>> stores) {
        return new org.apache.kafka.streams.processor.ProcessorSupplier<K, V>() {
            @Override
            public org.apache.kafka.streams.processor.Processor<K, V> get() {
                return supplier.get();
            }

            @Override
            public Set<StoreBuilder<?>> stores() {
                return stores;
            }
        };
    }

    private <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> defineWithStores(final Supplier<Processor<KIn, VIn, KOut, VOut>> supplier,
                                                                                            final Set<StoreBuilder<?>> stores) {
        return new ProcessorSupplier<KIn, VIn, KOut, VOut>() {
            @Override
            public Processor<KIn, VIn, KOut, VOut> get() {
                return supplier.get();
            }

            @Override
            public Set<StoreBuilder<?>> stores() {
                return stores;
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
        public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
            if (record.value().toString().matches(".*@[0-9]+")) {
                return Long.parseLong(record.value().toString().split("@")[1]);
            }

            if (record.timestamp() >= 0L) {
                return record.timestamp();
            }

            return DEFAULT_TIMESTAMP;
        }
    }
}
