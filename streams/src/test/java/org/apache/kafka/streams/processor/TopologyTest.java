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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

// TODO (remove this comment) Test name ok, we just use InternalTopologyBuilder for now in this test until Topology gets added
public class TopologyTest {
    // TODO change from InternalTopologyBuilder to Topology
    private final InternalTopologyBuilder topology = new InternalTopologyBuilder();
    private final InternalTopologyBuilder.TopologyDescription expectedDescription = new InternalTopologyBuilder.TopologyDescription();

    @Test
    public void shouldDescribeEmptyTopology() {
        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void singleSourceShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void singleSourceWithListOfTopicsShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic1", "topic2", "topic3");

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void singleSourcePatternShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", Pattern.compile("topic[0-9]"));

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void multipleSourcesShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode1)));

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(1,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode2)));

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(2,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode3)));

        assertThat(topology.describe(), equalTo(expectedDescription));
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
    }

    @Test
    public void sourceAndProcessorWithStateShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final String[] store = new String[] {"store"};
        final TopologyDescription.Processor expectedProcessorNode
            = addProcessorWithNewStore("processor", store, expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
    }


    @Test
    public void sourceAndProcessorWithMultipleStatesShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final String[] stores = new String[] {"store1", "store2"};
        final TopologyDescription.Processor expectedProcessorNode
            = addProcessorWithNewStore("processor", stores, expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
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
    }

    @Test
    public void processorsWithSharedStateShouldHaveSameSubtopology() {
        final String[] store1 = new String[] {"store1"};
        final String[] store2 = new String[] {"store2"};
        final String[] bothStores = new String[] {store1[0], store2[0]};

        final TopologyDescription.Source expectedSourceNode1 = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1
            = addProcessorWithNewStore("processor1", store1, expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2
            = addProcessorWithNewStore("processor2", store2, expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3
            = addProcessorWithExistingStore("processor3", bothStores, expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode2);
        allNodes.add(expectedSourceNode3);
        allNodes.add(expectedProcessorNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void shouldDescribeGlobalStoreTopology() {
        addGlobalStoreToTopologyAndExpectedDescription("globalStore", "source", "globalTopic", "processor");
        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    @Test
    public void shouldDescribeMultipleGlobalStoreTopology() {
        addGlobalStoreToTopologyAndExpectedDescription("globalStore1", "source1", "globalTopic1", "processor1");
        addGlobalStoreToTopologyAndExpectedDescription("globalStore2", "source2", "globalTopic2", "processor2");
        assertThat(topology.describe(), equalTo(expectedDescription));
    }

    private TopologyDescription.Source addSource(final String sourceName,
                                                 final String... sourceTopic) {
        topology.addSource(null, sourceName, null, null, null, sourceTopic);
        String allSourceTopics = sourceTopic[0];
        for (int i = 1; i < sourceTopic.length; ++i) {
            allSourceTopics += ", " + sourceTopic[i];
        }
        return new InternalTopologyBuilder.Source(sourceName, allSourceTopics);
    }

    private TopologyDescription.Source addSource(final String sourceName,
                                                 final Pattern sourcePattern) {
        topology.addSource(null, sourceName, null, null, null, sourcePattern);
        return new InternalTopologyBuilder.Source(sourceName, sourcePattern.toString());
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

        topology.addProcessor(processorName, new MockProcessorSupplier(), parentNames);
        if (newStores) {
            for (final String store : storeNames) {
                topology.addStateStore(new MockStateStoreSupplier(store, false), processorName);
            }
        } else {
            topology.connectProcessorAndStateStores(processorName, storeNames);
        }
        final TopologyDescription.Processor expectedProcessorNode
            = new InternalTopologyBuilder.Processor(processorName, new HashSet<>(Arrays.asList(storeNames)));

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
        final TopologyDescription.Sink expectedSinkNode
            = new InternalTopologyBuilder.Sink(sinkName, sinkTopic);

        for (final TopologyDescription.Node parent : parents) {
            ((InternalTopologyBuilder.AbstractNode) parent).addSuccessor(expectedSinkNode);
            ((InternalTopologyBuilder.AbstractNode) expectedSinkNode).addPredecessor(parent);
        }

        return expectedSinkNode;
    }

    private void addGlobalStoreToTopologyAndExpectedDescription(final String globalStoreName,
                                                                final String sourceName,
                                                                final String globalTopicName,
                                                                final String processorName) {
        topology.addGlobalStore(
            new MockStateStoreSupplier(globalStoreName, false, false),
            sourceName,
            null,
            null,
            null,
            globalTopicName,
            processorName,
            new MockProcessorSupplier());

        final TopologyDescription.GlobalStore expectedGlobalStore = new InternalTopologyBuilder.GlobalStore(
            sourceName,
            processorName,
            globalStoreName,
            globalTopicName);

        expectedDescription.addGlobalStore(expectedGlobalStore);
    }

}
