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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DescribeStreamsGroupsHandlerTest {

    private static final byte NODE_TYPE_SOURCE = 1;
    private static final byte NODE_TYPE_PROCESSOR = 2;
    private static final byte NODE_TYPE_SINK = 3;

    private final LogContext logContext = new LogContext();
    private final String groupId = "group-id";
    private final Node coordinator = new Node(1, "host", 1234);

    @Test
    public void testBuildRequestWithoutTopologyDescription() {
        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, logContext);
        StreamsGroupDescribeRequest request = handler.buildBatchedRequest(1, Set.of(CoordinatorKey.byGroupId(groupId))).build();

        assertFalse(request.data().includeAuthorizedOperations());
        assertFalse(request.data().includeTopologyDescription());
    }

    @Test
    public void testBuildRequestWithTopologyDescription() {
        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(true, true, logContext);
        StreamsGroupDescribeRequest request = handler.buildBatchedRequest(1, Set.of(CoordinatorKey.byGroupId(groupId))).build();

        assertTrue(request.data().includeAuthorizedOperations());
        assertTrue(request.data().includeTopologyDescription());
    }

    @Test
    public void testHandleResponseWithoutTopologyDescription() {
        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, logContext);
        StreamsGroupDescribeResponse response = new StreamsGroupDescribeResponse(
            new StreamsGroupDescribeResponseData().setGroups(List.of(
                validDescribedGroup(null)
            )));
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result =
            handler.handleResponse(coordinator, Set.of(CoordinatorKey.byGroupId(groupId)), response);

        StreamsGroupDescription description = result.completedKeys.get(CoordinatorKey.byGroupId(groupId));
        assertEquals(Optional.empty(), description.topologyDescription());
    }

    @Test
    public void testHandleResponseWithTopologyDescription() {
        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, true, logContext);
        StreamsGroupDescribeResponse response = new StreamsGroupDescribeResponse(
            new StreamsGroupDescribeResponseData().setGroups(List.of(
                validDescribedGroup(sampleTopology())
            )));
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result =
            handler.handleResponse(coordinator, Set.of(CoordinatorKey.byGroupId(groupId)), response);

        StreamsGroupDescription description = result.completedKeys.get(CoordinatorKey.byGroupId(groupId));
        assertTrue(description.topologyDescription().isPresent());
        StreamsGroupTopologyDescription topology = description.topologyDescription().get();

        // 1 subtopology, 3 nodes — source -> processor -> sink
        assertEquals(1, topology.subtopologies().size());
        StreamsGroupTopologyDescription.Subtopology subtopology = topology.subtopologies().iterator().next();
        assertEquals("0", subtopology.id());
        assertEquals(3, subtopology.nodes().size());

        List<StreamsGroupTopologyDescription.Node> nodes = List.copyOf(subtopology.nodes());
        StreamsGroupTopologyDescription.Node source = nodes.stream()
            .filter(n -> n.name().equals("source")).findFirst().orElseThrow();
        StreamsGroupTopologyDescription.Node processor = nodes.stream()
            .filter(n -> n.name().equals("processor")).findFirst().orElseThrow();
        StreamsGroupTopologyDescription.Node sink = nodes.stream()
            .filter(n -> n.name().equals("sink")).findFirst().orElseThrow();

        assertInstanceOf(StreamsGroupTopologyDescription.Source.class, source);
        assertInstanceOf(StreamsGroupTopologyDescription.Processor.class, processor);
        assertInstanceOf(StreamsGroupTopologyDescription.Sink.class, sink);

        assertEquals(Set.of("input-topic"), ((StreamsGroupTopologyDescription.Source) source).topics());
        assertEquals(Set.of("my-store"), ((StreamsGroupTopologyDescription.Processor) processor).stores());
        assertEquals(Optional.of("output-topic"), ((StreamsGroupTopologyDescription.Sink) sink).topic());

        // predecessors / successors
        assertTrue(source.predecessors().isEmpty());
        assertEquals(Set.of("processor"), source.successors());
        assertEquals(Set.of("source"), processor.predecessors());
        assertEquals(Set.of("sink"), processor.successors());
        assertEquals(Set.of("processor"), sink.predecessors());
        assertTrue(sink.successors().isEmpty());

        assertTrue(topology.globalStores().isEmpty());
    }

    @Test
    public void testHandleResponseWithGlobalStore() {
        DescribeStreamsGroupsHandler handler = new DescribeStreamsGroupsHandler(false, true, logContext);
        StreamsGroupDescribeResponseData.TopologyDescription topo = sampleTopology();
        topo.globalStores().add(new StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore()
            .setSource(new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("global-source").setNodeType(NODE_TYPE_SOURCE)
                .setSourceTopics(List.of("global-topic")))
            .setProcessor(new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("global-processor").setNodeType(NODE_TYPE_PROCESSOR)
                .setStores(List.of("global-store"))));

        StreamsGroupDescribeResponse response = new StreamsGroupDescribeResponse(
            new StreamsGroupDescribeResponseData().setGroups(List.of(validDescribedGroup(topo))));
        AdminApiHandler.ApiResult<CoordinatorKey, StreamsGroupDescription> result =
            handler.handleResponse(coordinator, Set.of(CoordinatorKey.byGroupId(groupId)), response);

        StreamsGroupTopologyDescription topology = result.completedKeys.get(CoordinatorKey.byGroupId(groupId))
            .topologyDescription().orElseThrow();
        assertEquals(1, topology.globalStores().size());
        StreamsGroupTopologyDescription.GlobalStore global = topology.globalStores().iterator().next();
        assertEquals("global-source", global.source().name());
        assertEquals(Set.of("global-topic"), global.source().topics());
        assertEquals("global-processor", global.processor().name());
        assertEquals(Set.of("global-store"), global.processor().stores());
    }

    private StreamsGroupDescribeResponseData.DescribedGroup validDescribedGroup(
            StreamsGroupDescribeResponseData.TopologyDescription topology) {
        return new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.NONE.code())
            .setGroupState("STABLE")
            .setTopology(new StreamsGroupDescribeResponseData.Topology()
                .setEpoch(1)
                .setSubtopologies(List.of()))
            .setTopologyDescription(topology);
    }

    private StreamsGroupDescribeResponseData.TopologyDescription sampleTopology() {
        StreamsGroupDescribeResponseData.TopologyDescriptionNode source =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("source")
                .setNodeType(NODE_TYPE_SOURCE)
                .setSourceTopics(List.of("input-topic"))
                .setSuccessors(List.of("processor"));
        StreamsGroupDescribeResponseData.TopologyDescriptionNode processor =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("processor")
                .setNodeType(NODE_TYPE_PROCESSOR)
                .setStores(List.of("my-store"))
                .setSuccessors(List.of("sink"));
        StreamsGroupDescribeResponseData.TopologyDescriptionNode sink =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName("sink")
                .setNodeType(NODE_TYPE_SINK)
                .setSinkTopic("output-topic");

        return new StreamsGroupDescribeResponseData.TopologyDescription()
            .setSubtopologies(List.of(
                new StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology()
                    .setSubtopologyId("0")
                    .setNodes(List.of(source, processor, sink))
            ));
    }
}
