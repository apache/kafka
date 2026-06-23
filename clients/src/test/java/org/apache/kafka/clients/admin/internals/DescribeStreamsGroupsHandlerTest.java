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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescriptionStatus;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DescribeStreamsGroupsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId = "group-id";
    private final Node coordinator = new Node(1, "host", 1234);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBuildRequestSetsIncludeTopologyDescription(boolean includeTopologyDescription) {
        DescribeStreamsGroupsHandler handler =
            new DescribeStreamsGroupsHandler(false, includeTopologyDescription, logContext);

        StreamsGroupDescribeRequest.Builder builder =
            handler.buildBatchedRequest(1, Set.of(CoordinatorKey.byGroupId(groupId)));
        StreamsGroupDescribeRequestData data = builder.build().data();

        assertEquals(includeTopologyDescription, data.includeTopologyDescription());
        assertEquals(List.of(groupId), data.groupIds());
    }

    @ParameterizedTest
    @EnumSource(value = StreamsGroupTopologyDescriptionStatus.class, names = {"NOT_REQUESTED", "NOT_STORED", "ERROR"})
    public void testTopologyDescriptionAbsentWhenStatusNotAvailable(StreamsGroupTopologyDescriptionStatus status) {
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = newDescribedGroup()
            .setTopologyDescriptionStatus(status.id())
            .setTopologyDescription(null);

        StreamsGroupDescription description = describe(describedGroup);

        assertEquals(status, description.topologyDescriptionStatus());
        assertTrue(description.topologyDescription().isEmpty());
    }

    @Test
    public void testTopologyDescriptionParsedWhenAvailable() {
        StreamsGroupDescribeResponseData.TopologyDescriptionNode source =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("source")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_SOURCE)
                .setSourceTopics(List.of("input"))
                .setSuccessors(List.of("processor"));
        StreamsGroupDescribeResponseData.TopologyDescriptionNode processor =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("processor")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_PROCESSOR)
                .setStores(List.of("store"))
                .setSuccessors(List.of("sink"));
        StreamsGroupDescribeResponseData.TopologyDescriptionNode sink =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("sink")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_SINK)
                .setSinkTopic("output")
                .setSuccessors(List.of());

        StreamsGroupDescribeResponseData.TopologyDescription wire =
            new StreamsGroupDescribeResponseData.TopologyDescription()
                .setSubtopologies(List.of(
                    new StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology()
                        .setSubtopologyId("0")
                        .setNodes(List.of(source, processor, sink))))
                .setGlobalStores(List.of());

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = newDescribedGroup()
            .setTopologyDescriptionStatus(StreamsGroupTopologyDescriptionStatus.AVAILABLE.id())
            .setTopologyDescription(wire);

        StreamsGroupDescription description = describe(describedGroup);

        assertEquals(StreamsGroupTopologyDescriptionStatus.AVAILABLE, description.topologyDescriptionStatus());
        assertTrue(description.topologyDescription().isPresent());

        StreamsGroupTopologyDescription topology = description.topologyDescription().get();
        assertEquals(1, topology.subtopologies().size());
        assertTrue(topology.globalStores().isEmpty());

        StreamsGroupTopologyDescription.Subtopology subtopology = topology.subtopologies().iterator().next();
        assertEquals("0", subtopology.id());
        List<StreamsGroupTopologyDescription.Node> nodes = List.copyOf(subtopology.nodes());
        assertEquals(3, nodes.size());

        StreamsGroupTopologyDescription.Source parsedSource =
            assertInstanceOf(StreamsGroupTopologyDescription.Source.class, nodes.get(0));
        assertEquals("source", parsedSource.name());
        assertEquals(Set.of("input"), parsedSource.topics());
        assertEquals(Set.of("processor"), parsedSource.successors());
        // Sources have no predecessors.
        assertTrue(parsedSource.predecessors().isEmpty());

        StreamsGroupTopologyDescription.Processor parsedProcessor =
            assertInstanceOf(StreamsGroupTopologyDescription.Processor.class, nodes.get(1));
        assertEquals(Set.of("store"), parsedProcessor.stores());
        assertEquals(Set.of("sink"), parsedProcessor.successors());
        // Predecessor reconstructed from the source's successor list.
        assertEquals(Set.of("source"), parsedProcessor.predecessors());

        StreamsGroupTopologyDescription.Sink parsedSink =
            assertInstanceOf(StreamsGroupTopologyDescription.Sink.class, nodes.get(2));
        assertEquals("output", parsedSink.topic().orElse(null));
        assertTrue(parsedSink.successors().isEmpty());
        assertEquals(Set.of("processor"), parsedSink.predecessors());
    }

    @Test
    public void testGlobalStorePredecessorReconstruction() {
        StreamsGroupDescribeResponseData.TopologyDescriptionNode globalSource =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("global-source")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_SOURCE)
                .setSourceTopics(List.of("global-topic"))
                .setSuccessors(List.of("global-processor"));
        StreamsGroupDescribeResponseData.TopologyDescriptionNode globalProcessor =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("global-processor")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_PROCESSOR)
                .setStores(List.of("global-store"))
                .setSuccessors(List.of());

        StreamsGroupDescribeResponseData.TopologyDescription wire =
            new StreamsGroupDescribeResponseData.TopologyDescription()
                .setSubtopologies(List.of())
                .setGlobalStores(List.of(
                    new StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore()
                        .setSource(globalSource)
                        .setProcessor(globalProcessor)));

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = newDescribedGroup()
            .setTopologyDescriptionStatus(StreamsGroupTopologyDescriptionStatus.AVAILABLE.id())
            .setTopologyDescription(wire);

        StreamsGroupTopologyDescription topology = describe(describedGroup).topologyDescription().orElseThrow();
        assertEquals(1, topology.globalStores().size());

        StreamsGroupTopologyDescription.GlobalStore globalStore = topology.globalStores().iterator().next();
        assertTrue(globalStore.source().predecessors().isEmpty());
        assertEquals(Set.of("global-processor"), globalStore.source().successors());
        assertEquals(Set.of("global-source"), globalStore.processor().predecessors());
    }

    @Test
    public void testTopologyDescriptionNotRequestedByDefault() {
        StreamsGroupDescription description = describe(newDescribedGroup());
        assertEquals(StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED, description.topologyDescriptionStatus());
        assertFalse(description.topologyDescription().isPresent());
    }

    @Test
    public void testUnknownTopologyDescriptionStatusFailsOnlyAffectedGroup() {
        // A future broker version may add a topology description status id that this client does not know about.
        byte unknownStatusId = (byte) 99;
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = newDescribedGroup()
            .setTopologyDescriptionStatus(unknownStatusId)
            .setTopologyDescription(null);

        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, true, logContext);
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result = handler.handleResponse(
            coordinator,
            Set.of(key),
            new StreamsGroupDescribeResponse(new StreamsGroupDescribeResponseData()
                .setGroups(List.of(describedGroup))));

        // The unknown status must surface as a per-group failure rather than propagating an exception.
        assertTrue(result.completedKeys.isEmpty());
        assertEquals(Set.of(key), result.failedKeys.keySet());
        assertInstanceOf(IllegalStateException.class, result.failedKeys.get(key));
    }

    @Test
    public void testAvailableStatusWithMissingTopologyDescriptionFailsOnlyAffectedGroup() {
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = newDescribedGroup()
            .setTopologyDescriptionStatus(StreamsGroupTopologyDescriptionStatus.AVAILABLE.id())
            .setTopologyDescription(null);

        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, true, logContext);
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result = handler.handleResponse(
            coordinator,
            Set.of(key),
            new StreamsGroupDescribeResponse(new StreamsGroupDescribeResponseData()
                .setGroups(List.of(describedGroup))));

        assertTrue(result.completedKeys.isEmpty());
        assertEquals(Set.of(key), result.failedKeys.keySet());
        assertInstanceOf(IllegalStateException.class, result.failedKeys.get(key));
    }

    @Test
    public void testUnknownTopologyNodeTypeFailsOnlyAffectedGroup() {
        // A future broker version may add a topology node type that this client does not know about.
        StreamsGroupDescribeResponseData.TopologyDescriptionNode unknownNode =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("mystery")
                .setNodeType((byte) 99)
                .setSuccessors(List.of());
        StreamsGroupDescribeResponseData.TopologyDescription wire =
            new StreamsGroupDescribeResponseData.TopologyDescription()
                .setSubtopologies(List.of(
                    new StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology()
                        .setSubtopologyId("0")
                        .setNodes(List.of(unknownNode))))
                .setGlobalStores(List.of());

        assertGroupFailed(wire);
    }

    @Test
    public void testMalformedGlobalStoreFailsOnlyAffectedGroup() {
        // A global store must be a source/processor pair; here the "source" is itself a sink node.
        StreamsGroupDescribeResponseData.TopologyDescriptionNode notASource =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("global-sink")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_SINK)
                .setSinkTopic("output")
                .setSuccessors(List.of());
        StreamsGroupDescribeResponseData.TopologyDescriptionNode processor =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("global-processor")
                .setNodeType(DescribeStreamsGroupsHandler.NODE_TYPE_PROCESSOR)
                .setStores(List.of("global-store"))
                .setSuccessors(List.of());
        StreamsGroupDescribeResponseData.TopologyDescription wire =
            new StreamsGroupDescribeResponseData.TopologyDescription()
                .setSubtopologies(List.of())
                .setGlobalStores(List.of(
                    new StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore()
                        .setSource(notASource)
                        .setProcessor(processor)));

        assertGroupFailed(wire);
    }

    private void assertGroupFailed(StreamsGroupDescribeResponseData.TopologyDescription wire) {
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = newDescribedGroup()
            .setTopologyDescriptionStatus(StreamsGroupTopologyDescriptionStatus.AVAILABLE.id())
            .setTopologyDescription(wire);

        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, true, logContext);
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result = handler.handleResponse(
            coordinator,
            Set.of(key),
            new StreamsGroupDescribeResponse(new StreamsGroupDescribeResponseData()
                .setGroups(List.of(describedGroup))));

        assertTrue(result.completedKeys.isEmpty());
        assertEquals(Set.of(key), result.failedKeys.keySet());
        assertInstanceOf(IllegalStateException.class, result.failedKeys.get(key));
    }

    private StreamsGroupDescribeResponseData.DescribedGroup newDescribedGroup() {
        return new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setGroupState("Stable")
            .setGroupEpoch(1)
            .setAssignmentEpoch(1)
            .setTopology(new StreamsGroupDescribeResponseData.Topology().setEpoch(1).setSubtopologies(List.of()));
    }

    private StreamsGroupDescription describe(StreamsGroupDescribeResponseData.DescribedGroup describedGroup) {
        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, true, logContext);
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result = handler.handleResponse(
            coordinator,
            Set.of(key),
            new StreamsGroupDescribeResponse(new StreamsGroupDescribeResponseData()
                .setGroups(List.of(describedGroup))));
        assertTrue(result.failedKeys.isEmpty(), () -> "Unexpected failures: " + result.failedKeys);
        return result.completedKeys.get(key);
    }
}
