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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockProcessorSupplier;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class TopologyTest {

    private final StoreBuilder<MockKeyValueStore> storeBuilder = EasyMock.createNiceMock(StoreBuilder.class);
    private final KeyValueStoreBuilder<?, ?> globalStoreBuilder = EasyMock.createNiceMock(KeyValueStoreBuilder.class);
    private final Topology topology = new Topology();
    private final InternalTopologyBuilder.TopologyDescription expectedDescription = new InternalTopologyBuilder.TopologyDescription();

    @Test
    public void shouldNotAllowNullNameWhenAddingSourceWithTopic() {
        assertThrows(NullPointerException.class, () -> topology.addSource((String) null, "topic"));
    }

    @Test
    public void shouldNotAllowNullNameWhenAddingSourceWithPattern() {
        assertThrows(NullPointerException.class, () -> topology.addSource(null, Pattern.compile(".*")));
    }

    @Test
    public void shouldNotAllowNullTopicsWhenAddingSoureWithTopic() {
        assertThrows(NullPointerException.class, () -> topology.addSource("source", (String[]) null));
    }

    @Test
    public void shouldNotAllowNullTopicsWhenAddingSourceWithPattern() {
        assertThrows(NullPointerException.class, () -> topology.addSource("source", (Pattern) null));
    }

    @Test
    public void shouldNotAllowZeroTopicsWhenAddingSource() {
        assertThrows(TopologyException.class, () -> topology.addSource("source"));
    }

    @Test
    public void shouldNotAllowNullNameWhenAddingProcessor() {
        assertThrows(NullPointerException.class, () -> topology.addProcessor(null, () -> new MockApiProcessorSupplier<>().get()));
    }

    @Test
    public void shouldNotAllowNullProcessorSupplierWhenAddingProcessor() {
        assertThrows(NullPointerException.class, () -> topology.addProcessor("name",
            (ProcessorSupplier<Object, Object, Object, Object>) null));
    }

    @Test
    public void shouldNotAllowNullNameWhenAddingSink() {
        assertThrows(NullPointerException.class, () -> topology.addSink(null, "topic"));
    }

    @Test
    public void shouldNotAllowNullTopicWhenAddingSink() {
        assertThrows(NullPointerException.class, () -> topology.addSink("name", (String) null));
    }

    @Test
    public void shouldNotAllowNullTopicChooserWhenAddingSink() {
        assertThrows(NullPointerException.class, () -> topology.addSink("name", (TopicNameExtractor<Object, Object>) null));
    }

    @Test
    public void shouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores() {
        assertThrows(NullPointerException.class, () -> topology.connectProcessorAndStateStores(null, "store"));
    }

    @Test
    public void shouldNotAllowNullStoreNameWhenConnectingProcessorAndStateStores() {
        assertThrows(NullPointerException.class, () -> topology.connectProcessorAndStateStores("processor", (String[]) null));
    }

    @Test
    public void shouldNotAllowZeroStoreNameWhenConnectingProcessorAndStateStores() {
        assertThrows(TopologyException.class, () -> topology.connectProcessorAndStateStores("processor"));
    }

    @Test
    public void shouldNotAddNullStateStoreSupplier() {
        assertThrows(NullPointerException.class, () -> topology.addStateStore(null));
    }

    @Test
    public void shouldNotAllowToAddSourcesWithSameName() {
        topology.addSource("source", "topic-1");
        try {
            topology.addSource("source", "topic-2");
            fail("Should throw TopologyException for duplicate source name");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddTopicTwice() {
        topology.addSource("source", "topic-1");
        try {
            topology.addSource("source-2", "topic-1");
            fail("Should throw TopologyException for already used topic");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void testPatternMatchesAlreadyProvidedTopicSource() {
        topology.addSource("source-1", "foo");
        try {
            topology.addSource("source-2", Pattern.compile("f.*"));
            fail("Should have thrown TopologyException for overlapping pattern with already registered topic");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void testNamedTopicMatchesAlreadyProvidedPattern() {
        topology.addSource("source-1", Pattern.compile("f.*"));
        try {
            topology.addSource("source-2", "foo");
            fail("Should have thrown TopologyException for overlapping topic with already registered pattern");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddProcessorWithSameName() {
        topology.addSource("source", "topic-1");
        topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        try {
            topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
            fail("Should throw TopologyException for duplicate processor name");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddProcessorWithEmptyParents() {
        topology.addSource("source", "topic-1");
        try {
            topology.addProcessor("processor", new MockApiProcessorSupplier<>());
            fail("Should throw TopologyException for processor without at least one parent node");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddProcessorWithNullParents() {
        topology.addSource("source", "topic-1");
        try {
            topology.addProcessor("processor", new MockApiProcessorSupplier<>(), (String) null);
            fail("Should throw NullPointerException for processor when null parent names are provided");
        } catch (final NullPointerException expected) { }
    }

    @Test
    public void shouldFailOnUnknownSource() {
        assertThrows(TopologyException.class, () -> topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source"));
    }

    @Test
    public void shouldFailIfNodeIsItsOwnParent() {
        assertThrows(TopologyException.class, () -> topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "processor"));
    }

    @Test
    public void shouldNotAllowToAddSinkWithSameName() {
        topology.addSource("source", "topic-1");
        topology.addSink("sink", "topic-2", "source");
        try {
            topology.addSink("sink", "topic-3", "source");
            fail("Should throw TopologyException for duplicate sink name");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddSinkWithEmptyParents() {
        topology.addSource("source", "topic-1");
        topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        try {
            topology.addSink("sink", "topic-2");
            fail("Should throw TopologyException for sink without at least one parent node");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddSinkWithNullParents() {
        topology.addSource("source", "topic-1");
        topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        try {
            topology.addSink("sink", "topic-2", (String) null);
            fail("Should throw NullPointerException for sink when null parent names are provided");
        } catch (final NullPointerException expected) { }
    }

    @Test
    public void shouldFailWithUnknownParent() {
        assertThrows(TopologyException.class, () -> topology.addSink("sink", "topic-2", "source"));
    }

    @Test
    public void shouldFailIfSinkIsItsOwnParent() {
        assertThrows(TopologyException.class, () -> topology.addSink("sink", "topic-2", "sink"));
    }

    @Test
    public void shouldFailIfSinkIsParent() {
        topology.addSource("source", "topic-1");
        topology.addSink("sink-1", "topic-2", "source");
        try {
            topology.addSink("sink-2", "topic-3", "sink-1");
            fail("Should throw TopologyException for using sink as parent");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddStateStoreToNonExistingProcessor() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        assertThrows(TopologyException.class, () -> topology.addStateStore(storeBuilder, "no-such-processor"));
    }

    @Test
    public void shouldNotAllowToAddStateStoreToSource() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addSource("source-1", "topic-1");
        try {
            topology.addStateStore(storeBuilder, "source-1");
            fail("Should have thrown TopologyException for adding store to source node");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddStateStoreToSink() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addSource("source-1", "topic-1");
        topology.addSink("sink-1", "topic-1", "source-1");
        try {
            topology.addStateStore(storeBuilder, "sink-1");
            fail("Should have thrown TopologyException for adding store to sink node");
        } catch (final TopologyException expected) { }
    }

    private void mockStoreBuilder() {
        EasyMock.expect(storeBuilder.name()).andReturn("store").anyTimes();
        EasyMock.expect(storeBuilder.logConfig()).andReturn(Collections.emptyMap());
        EasyMock.expect(storeBuilder.loggingEnabled()).andReturn(false);
    }

    @Test
    public void shouldNotAllowToAddStoreWithSameNameAndDifferentInstance() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addStateStore(storeBuilder);

        final StoreBuilder otherStoreBuilder = EasyMock.createNiceMock(StoreBuilder.class);
        EasyMock.expect(otherStoreBuilder.name()).andReturn("store").anyTimes();
        EasyMock.expect(otherStoreBuilder.logConfig()).andReturn(Collections.emptyMap());
        EasyMock.expect(otherStoreBuilder.loggingEnabled()).andReturn(false);
        EasyMock.replay(otherStoreBuilder);
        try {
            topology.addStateStore(otherStoreBuilder);
            fail("Should have thrown TopologyException for same store name with different StoreBuilder");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldAllowToShareStoreUsingSameStoreBuilder() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);

        topology.addSource("source", "topic-1");

        topology.addProcessor("processor-1", new MockProcessorSupplierProvidingStore<>(storeBuilder), "source");
        topology.addProcessor("processor-2", new MockProcessorSupplierProvidingStore<>(storeBuilder), "source");
    }

    private static class MockProcessorSupplierProvidingStore<K, V> extends MockApiProcessorSupplier<K, V, Void, Void> {
        private final StoreBuilder<MockKeyValueStore> storeBuilder;

        public MockProcessorSupplierProvidingStore(final StoreBuilder<MockKeyValueStore> storeBuilder) {
            this.storeBuilder = storeBuilder;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Collections.singleton(storeBuilder);
        }
    }

    @Test
    public void shouldThrowOnUnassignedStateStoreAccess() {
        final String sourceNodeName = "source";
        final String goodNodeName = "goodGuy";
        final String badNodeName = "badGuy";

        mockStoreBuilder();
        EasyMock.expect(storeBuilder.build()).andReturn(new MockKeyValueStore("store", false));
        EasyMock.replay(storeBuilder);
        topology
            .addSource(sourceNodeName, "topic")
            .addProcessor(goodNodeName, new LocalMockProcessorSupplier(), sourceNodeName)
            .addStateStore(
                storeBuilder,
                goodNodeName)
            .addProcessor(badNodeName, new LocalMockProcessorSupplier(), sourceNodeName);

        try {
            new TopologyTestDriver(topology);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            final String error = e.toString();
            final String expectedMessage = "org.apache.kafka.streams.errors.StreamsException: failed to initialize processor " + badNodeName;

            assertThat(error, equalTo(expectedMessage));
        }
    }

    private static class LocalMockProcessorSupplier implements ProcessorSupplier<Object, Object, Object, Object> {
        final static String STORE_NAME = "store";

        @Override
        public Processor<Object, Object, Object, Object> get() {
            return new Processor<Object, Object, Object, Object>() {
                @Override
                public void init(final ProcessorContext<Object, Object> context) {
                    context.getStateStore(STORE_NAME);
                }

                @Override
                public void process(final Record<Object, Object> record) { }
            };
        }
    }

    @Deprecated // testing old PAPI
    @Test
    public void shouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName() {
        EasyMock.expect(globalStoreBuilder.name()).andReturn("anyName").anyTimes();
        EasyMock.replay(globalStoreBuilder);
        assertThrows(TopologyException.class, () -> topology.addGlobalStore(
            globalStoreBuilder,
            "sameName",
            null,
            null,
            "anyTopicName",
            "sameName",
            new MockProcessorSupplier<>()));
    }

    @Test
    public void shouldDescribeEmptyTopology() {
        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void sinkShouldReturnNullTopicWithDynamicRouting() {
        final TopologyDescription.Sink expectedSinkNode =
            new InternalTopologyBuilder.Sink<>("sink", (key, value, record) -> record.topic() + "-" + key);

        assertThat(expectedSinkNode.topic(), equalTo(null));
    }

    @Test
    public void sinkShouldReturnTopicNameExtractorWithDynamicRouting() {
        final TopicNameExtractor<?, ?> topicNameExtractor = (key, value, record) -> record.topic() + "-" + key;
        final TopologyDescription.Sink expectedSinkNode =
            new InternalTopologyBuilder.Sink<>("sink", topicNameExtractor);

        assertThat(expectedSinkNode.topicNameExtractor(), equalTo(topicNameExtractor));
    }

    @Test
    public void singleSourceShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void singleSourceWithListOfTopicsShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic1", "topic2", "topic3");

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void singleSourcePatternShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", Pattern.compile("topic[0-9]"));

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void multipleSourcesShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.singleton(expectedSourceNode1)));

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(1,
                Collections.singleton(expectedSourceNode2)));

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(2,
                Collections.singleton(expectedSourceNode3)));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void sourceAndProcessorShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode = addProcessor("processor", expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void sourceAndProcessorWithStateShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final String[] store = new String[] {"store"};
        final TopologyDescription.Processor expectedProcessorNode =
            addProcessorWithNewStore("processor", store, expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }


    @Test
    public void sourceAndProcessorWithMultipleStatesShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final String[] stores = new String[] {"store1", "store2"};
        final TopologyDescription.Processor expectedProcessorNode =
            addProcessorWithNewStore("processor", stores, expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void sourceWithMultipleProcessorsShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1 = addProcessor("processor1", expectedSourceNode);
        final TopologyDescription.Processor expectedProcessorNode2 = addProcessor("processor2", expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedProcessorNode2);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void processorWithMultipleSourcesShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic0");
        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", Pattern.compile("topic[1-9]"));
        final TopologyDescription.Processor expectedProcessorNode = addProcessor("processor", expectedSourceNode1, expectedSourceNode2);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void multipleSourcesWithProcessorsShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        final TopologyDescription.Processor expectedProcessorNode1 = addProcessor("processor1", expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2 = addProcessor("processor2", expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3 = addProcessor("processor3", expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes1 = new HashSet<>();
        allNodes1.add(expectedSourceNode1);
        allNodes1.add(expectedProcessorNode1);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes1));

        final Set<TopologyDescription.Node> allNodes2 = new HashSet<>();
        allNodes2.add(expectedSourceNode2);
        allNodes2.add(expectedProcessorNode2);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(1, allNodes2));

        final Set<TopologyDescription.Node> allNodes3 = new HashSet<>();
        allNodes3.add(expectedSourceNode3);
        allNodes3.add(expectedProcessorNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(2, allNodes3));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void multipleSourcesWithSinksShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        final TopologyDescription.Sink expectedSinkNode1 = addSink("sink1", "sinkTopic1", expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Sink expectedSinkNode2 = addSink("sink2", "sinkTopic2", expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Sink expectedSinkNode3 = addSink("sink3", "sinkTopic3", expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes1 = new HashSet<>();
        allNodes1.add(expectedSourceNode1);
        allNodes1.add(expectedSinkNode1);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes1));

        final Set<TopologyDescription.Node> allNodes2 = new HashSet<>();
        allNodes2.add(expectedSourceNode2);
        allNodes2.add(expectedSinkNode2);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(1, allNodes2));

        final Set<TopologyDescription.Node> allNodes3 = new HashSet<>();
        allNodes3.add(expectedSourceNode3);
        allNodes3.add(expectedSinkNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(2, allNodes3));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void processorsWithSameSinkShouldHaveSameSubtopology() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1 = addProcessor("processor1", expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2 = addProcessor("processor2", expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3 = addProcessor("processor3", expectedSourceNode3);

        final TopologyDescription.Sink expectedSinkNode = addSink(
            "sink",
            "sinkTopic",
            expectedProcessorNode1,
            expectedProcessorNode2,
            expectedProcessorNode3);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode2);
        allNodes.add(expectedSourceNode3);
        allNodes.add(expectedProcessorNode3);
        allNodes.add(expectedSinkNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void processorsWithSharedStateShouldHaveSameSubtopology() {
        final String[] store1 = new String[] {"store1"};
        final String[] store2 = new String[] {"store2"};
        final String[] bothStores = new String[] {store1[0], store2[0]};

        final TopologyDescription.Source expectedSourceNode1 = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1 =
            addProcessorWithNewStore("processor1", store1, expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2 =
            addProcessorWithNewStore("processor2", store2, expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3 =
            addProcessorWithExistingStore("processor3", bothStores, expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode2);
        allNodes.add(expectedSourceNode3);
        allNodes.add(expectedProcessorNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void shouldDescribeGlobalStoreTopology() {
        addGlobalStoreToTopologyAndExpectedDescription("globalStore", "source", "globalTopic", "processor", 0);
        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void shouldDescribeMultipleGlobalStoreTopology() {
        addGlobalStoreToTopologyAndExpectedDescription("globalStore1", "source1", "globalTopic1", "processor1", 0);
        addGlobalStoreToTopologyAndExpectedDescription("globalStore2", "source2", "globalTopic2", "processor2", 1);
        assertThat(topology.describe(), equalTo(expectedDescription));
        assertThat(topology.describe().hashCode(), equalTo(expectedDescription.hashCode()));
    }

    @Test
    public void topologyWithDynamicRoutingShouldDescribeExtractorClass() {
        final StreamsBuilder builder  = new StreamsBuilder();

        final TopicNameExtractor<Object, Object> topicNameExtractor = new TopicNameExtractor<Object, Object>() {
            @Override
            public String extract(final Object key, final Object value, final RecordContext recordContext) {
                return recordContext.topic() + "-" + key;
            }

            @Override
            public String toString() {
                return "anonymous topic name extractor. topic is [recordContext.topic()]-[key]";
            }
        };
        builder.stream("input-topic").to(topicNameExtractor);
        final TopologyDescription describe = builder.build().describe();

        assertEquals(
                "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-SINK-0000000001\n" +
                "    Sink: KSTREAM-SINK-0000000001 (extractor class: anonymous topic name extractor. topic is [recordContext.topic()]-[key])\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
                describe.toString());
    }

    @Test
    public void kGroupedStreamZeroArgCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .count();
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000002\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void kGroupedStreamNamedMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .count(Materialized.as("count-store"));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000001\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000001 (stores: [count-store])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void kGroupedStreamAnonymousMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .count(Materialized.with(null, Serdes.Long()));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000003\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000002])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void timeWindowZeroArgCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(1)))
            .count();
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000002\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void timeWindowNamedMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(1)))
            .count(Materialized.as("count-store"));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000001\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000001 (stores: [count-store])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void timeWindowAnonymousMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(1)))
            .count(Materialized.with(null, Serdes.Long()));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000003\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000002])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void sessionWindowZeroArgCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .windowedBy(SessionWindows.with(ofMillis(1)))
            .count();
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000002\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void sessionWindowNamedMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .windowedBy(SessionWindows.with(ofMillis(1)))
            .count(Materialized.as("count-store"));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000001\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000001 (stores: [count-store])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void sessionWindowAnonymousMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .groupByKey()
            .windowedBy(SessionWindows.with(ofMillis(1)))
            .count(Materialized.with(null, Serdes.Long()));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
                "      --> KSTREAM-AGGREGATE-0000000003\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000002])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n",
            describe.toString()
        );
    }

    @Test
    public void tableZeroArgCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("input-topic")
            .groupBy((key, value) -> null)
            .count();
        final TopologyDescription describe = builder.build().describe();

        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [input-topic-STATE-STORE-0000000000])\n" +
                "      --> KTABLE-SELECT-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-SELECT-0000000003 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000005\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "    Sink: KSTREAM-SINK-0000000005 (topic: KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition)\n" +
                "      <-- KTABLE-SELECT-0000000003\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000006 (topics: [KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition])\n" +
                "      --> KTABLE-AGGREGATE-0000000007\n" +
                "    Processor: KTABLE-AGGREGATE-0000000007 (stores: [KTABLE-AGGREGATE-STATE-STORE-0000000004])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000006\n" +
                "\n",
            describe.toString()
        );
    }

    @Test
    public void tableNamedMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("input-topic")
            .groupBy((key, value) -> null)
            .count(Materialized.as("count-store"));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [input-topic-STATE-STORE-0000000000])\n" +
                "      --> KTABLE-SELECT-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-SELECT-0000000003 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000004\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "    Sink: KSTREAM-SINK-0000000004 (topic: count-store-repartition)\n" +
                "      <-- KTABLE-SELECT-0000000003\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000005 (topics: [count-store-repartition])\n" +
                "      --> KTABLE-AGGREGATE-0000000006\n" +
                "    Processor: KTABLE-AGGREGATE-0000000006 (stores: [count-store])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000005\n" +
                "\n",
            describe.toString()
        );
    }

    @Test
    public void tableAnonymousMaterializedCountShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("input-topic")
            .groupBy((key, value) -> null)
            .count(Materialized.with(null, Serdes.Long()));
        final TopologyDescription describe = builder.build().describe();
        assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [input-topic-STATE-STORE-0000000000])\n" +
                "      --> KTABLE-SELECT-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-SELECT-0000000003 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000005\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "    Sink: KSTREAM-SINK-0000000005 (topic: KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition)\n" +
                "      <-- KTABLE-SELECT-0000000003\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000006 (topics: [KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition])\n" +
                "      --> KTABLE-AGGREGATE-0000000007\n" +
                "    Processor: KTABLE-AGGREGATE-0000000007 (stores: [KTABLE-AGGREGATE-STATE-STORE-0000000004])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000006\n" +
                "\n",
            describe.toString()
        );
    }

    @Test
    public void kTableNonMaterializedMapValuesShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("input-topic");
        table.mapValues((readOnlyKey, value) -> null);
        final TopologyDescription describe = builder.build().describe();
        Assert.assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [])\n" +
                "      --> KTABLE-MAPVALUES-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-MAPVALUES-0000000003 (stores: [])\n" +
                "      --> none\n" +
                "      <-- KTABLE-SOURCE-0000000002\n\n",
            describe.toString());
    }

    @Test
    public void kTableAnonymousMaterializedMapValuesShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("input-topic");
        table.mapValues(
            (readOnlyKey, value) -> null,
            Materialized.with(null, null));
        final TopologyDescription describe = builder.build().describe();
        Assert.assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [])\n" +
                "      --> KTABLE-MAPVALUES-0000000004\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                // previously, this was
                //   Processor: KTABLE-MAPVALUES-0000000004 (stores: [KTABLE-MAPVALUES-STATE-STORE-0000000003]
                // but we added a change not to materialize non-queryable stores. This change shouldn't break compatibility.
                "    Processor: KTABLE-MAPVALUES-0000000004 (stores: [])\n" +
                "      --> none\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "\n",
            describe.toString());
    }

    @Test
    public void kTableNamedMaterializedMapValuesShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("input-topic");
        table.mapValues(
            (readOnlyKey, value) -> null,
            Materialized.<Object, Object, KeyValueStore<Bytes, byte[]>>as("store-name").withKeySerde(null).withValueSerde(null));
        final TopologyDescription describe = builder.build().describe();
        Assert.assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [])\n" +
                "      --> KTABLE-MAPVALUES-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-MAPVALUES-0000000003 (stores: [store-name])\n" +
                "      --> none\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "\n",
            describe.toString());
    }

    @Test
    public void kTableNonMaterializedFilterShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("input-topic");
        table.filter((key, value) -> false);
        final TopologyDescription describe = builder.build().describe();
        Assert.assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [])\n" +
                "      --> KTABLE-FILTER-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-FILTER-0000000003 (stores: [])\n" +
                "      --> none\n" +
                "      <-- KTABLE-SOURCE-0000000002\n\n",
            describe.toString());
    }

    @Test
    public void kTableAnonymousMaterializedFilterShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("input-topic");
        table.filter((key, value) -> false, Materialized.with(null, null));
        final TopologyDescription describe = builder.build().describe();
        Assert.assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [])\n" +
                "      --> KTABLE-FILTER-0000000004\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                // Previously, this was
                //   Processor: KTABLE-FILTER-0000000004 (stores: [KTABLE-FILTER-STATE-STORE-0000000003]
                // but we added a change not to materialize non-queryable stores. This change shouldn't break compatibility.
                "    Processor: KTABLE-FILTER-0000000004 (stores: [])\n" +
                "      --> none\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "\n",
            describe.toString());
    }

    @Test
    public void kTableNamedMaterializedFilterShouldPreserveTopologyStructure() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("input-topic");
        table.filter((key, value) -> false, Materialized.as("store-name"));
        final TopologyDescription describe = builder.build().describe();

        Assert.assertEquals(
            "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [input-topic])\n" +
                "      --> KTABLE-SOURCE-0000000002\n" +
                "    Processor: KTABLE-SOURCE-0000000002 (stores: [])\n" +
                "      --> KTABLE-FILTER-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: KTABLE-FILTER-0000000003 (stores: [store-name])\n" +
                "      --> none\n" +
                "      <-- KTABLE-SOURCE-0000000002\n" +
                "\n",
            describe.toString());
    }

    @Test
    public void topologyWithStaticTopicNameExtractorShouldRespectEqualHashcodeContract() {
        final Topology topologyA = topologyWithStaticTopicName();
        final Topology topologyB = topologyWithStaticTopicName();
        assertThat(topologyA.describe(), equalTo(topologyB.describe()));
        assertThat(topologyA.describe().hashCode(), equalTo(topologyB.describe().hashCode()));
    }

    private Topology topologyWithStaticTopicName() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("from-topic-name").to("to-topic-name");
        return builder.build();
    }

    private TopologyDescription.Source addSource(final String sourceName,
                                                 final String... sourceTopic) {
        topology.addSource(null, sourceName, null, null, null, sourceTopic);
        final StringBuilder allSourceTopics = new StringBuilder(sourceTopic[0]);
        for (int i = 1; i < sourceTopic.length; ++i) {
            allSourceTopics.append(", ").append(sourceTopic[i]);
        }
        return new InternalTopologyBuilder.Source(sourceName, new HashSet<>(Arrays.asList(sourceTopic)), null);
    }

    private TopologyDescription.Source addSource(final String sourceName,
                                                 final Pattern sourcePattern) {
        topology.addSource(null, sourceName, null, null, null, sourcePattern);
        return new InternalTopologyBuilder.Source(sourceName, null, sourcePattern);
    }

    private TopologyDescription.Processor addProcessor(final String processorName,
                                                       final TopologyDescription.Node... parents) {
        return addProcessorWithNewStore(processorName, new String[0], parents);
    }

    private TopologyDescription.Processor addProcessorWithNewStore(final String processorName,
                                                                   final String[] storeNames,
                                                                   final TopologyDescription.Node... parents) {
        return addProcessorWithStore(processorName, storeNames, true, parents);
    }

    private TopologyDescription.Processor addProcessorWithExistingStore(final String processorName,
                                                                        final String[] storeNames,
                                                                        final TopologyDescription.Node... parents) {
        return addProcessorWithStore(processorName, storeNames, false, parents);
    }

    private TopologyDescription.Processor addProcessorWithStore(final String processorName,
                                                                final String[] storeNames,
                                                                final boolean newStores,
                                                                final TopologyDescription.Node... parents) {
        final String[] parentNames = new String[parents.length];
        for (int i = 0; i < parents.length; ++i) {
            parentNames[i] = parents[i].name();
        }

        topology.addProcessor(processorName, new MockApiProcessorSupplier<>(), parentNames);
        if (newStores) {
            for (final String store : storeNames) {
                final StoreBuilder<?> storeBuilder = EasyMock.createNiceMock(StoreBuilder.class);
                EasyMock.expect(storeBuilder.name()).andReturn(store).anyTimes();
                EasyMock.replay(storeBuilder);
                topology.addStateStore(storeBuilder, processorName);
            }
        } else {
            topology.connectProcessorAndStateStores(processorName, storeNames);
        }
        final TopologyDescription.Processor expectedProcessorNode =
            new InternalTopologyBuilder.Processor(processorName, new HashSet<>(Arrays.asList(storeNames)));

        for (final TopologyDescription.Node parent : parents) {
            ((InternalTopologyBuilder.AbstractNode) parent).addSuccessor(expectedProcessorNode);
            ((InternalTopologyBuilder.AbstractNode) expectedProcessorNode).addPredecessor(parent);
        }

        return expectedProcessorNode;
    }

    private TopologyDescription.Sink addSink(final String sinkName,
                                             final String sinkTopic,
                                             final TopologyDescription.Node... parents) {
        final String[] parentNames = new String[parents.length];
        for (int i = 0; i < parents.length; ++i) {
            parentNames[i] = parents[i].name();
        }

        topology.addSink(sinkName, sinkTopic, null, null, null, parentNames);
        final TopologyDescription.Sink expectedSinkNode =
            new InternalTopologyBuilder.Sink(sinkName, sinkTopic);

        for (final TopologyDescription.Node parent : parents) {
            ((InternalTopologyBuilder.AbstractNode) parent).addSuccessor(expectedSinkNode);
            ((InternalTopologyBuilder.AbstractNode) expectedSinkNode).addPredecessor(parent);
        }

        return expectedSinkNode;
    }

    @Deprecated // testing old PAPI
    private void addGlobalStoreToTopologyAndExpectedDescription(final String globalStoreName,
                                                                final String sourceName,
                                                                final String globalTopicName,
                                                                final String processorName,
                                                                final int id) {
        final KeyValueStoreBuilder<?, ?> globalStoreBuilder = EasyMock.createNiceMock(KeyValueStoreBuilder.class);
        EasyMock.expect(globalStoreBuilder.name()).andReturn(globalStoreName).anyTimes();
        EasyMock.replay(globalStoreBuilder);
        topology.addGlobalStore(
            globalStoreBuilder,
            sourceName,
            null,
            null,
            null,
            globalTopicName,
            processorName,
            new MockProcessorSupplier<>());

        final TopologyDescription.GlobalStore expectedGlobalStore = new InternalTopologyBuilder.GlobalStore(
            sourceName,
            processorName,
            globalStoreName,
            globalTopicName,
            id);

        expectedDescription.addGlobalStore(expectedGlobalStore);
    }
}
