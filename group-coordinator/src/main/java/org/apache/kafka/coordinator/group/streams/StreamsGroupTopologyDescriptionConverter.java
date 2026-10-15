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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.GlobalStore;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Node;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Processor;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Sink;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Source;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Subtopology;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

/**
 * Translates between the wire form of a streams topology description (as carried on
 * {@code StreamsGroupTopologyDescriptionUpdateRequest}) and the broker-side
 * {@link StreamsGroupTopologyDescription} POJO that is handed to the plugin.
 *
 * <p>String collections are wrapped in {@link LinkedHashSet} so that the wire ordering
 * is preserved through to the POJO and any downstream pretty-printing.
 */
public final class StreamsGroupTopologyDescriptionConverter {

    static final byte NODE_TYPE_SOURCE = 1;
    static final byte NODE_TYPE_PROCESSOR = 2;
    static final byte NODE_TYPE_SINK = 3;

    private StreamsGroupTopologyDescriptionConverter() {
    }

    public static StreamsGroupTopologyDescription fromRequest(
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire
    ) {
        List<Subtopology> subtopologies = wire.subtopologies().stream()
            .map(StreamsGroupTopologyDescriptionConverter::convertSubtopology)
            .toList();
        List<GlobalStore> globalStores = wire.globalStores().stream()
            .map(StreamsGroupTopologyDescriptionConverter::convertGlobalStore)
            .toList();
        return new StreamsGroupTopologyDescription(subtopologies, globalStores);
    }

    private static Subtopology convertSubtopology(
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology wire
    ) {
        List<Node> nodes = wire.nodes().stream()
            .map(StreamsGroupTopologyDescriptionConverter::convertNode)
            .toList();
        return new Subtopology(wire.subtopologyId(), nodes);
    }

    private static GlobalStore convertGlobalStore(
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore wire
    ) {
        Node source = convertNode(wire.source());
        Node processor = convertNode(wire.processor());
        if (!(source instanceof Source) || !(processor instanceof Processor)) {
            throw new InvalidRequestException(
                "Global store must be composed of a source and a processor node."
            );
        }
        return new GlobalStore((Source) source, (Processor) processor);
    }

    /**
     * Sibling of {@link #fromRequest}: translates the broker-side {@link StreamsGroupTopologyDescription}
     * POJO returned by {@code plugin.getTopology} into the describe-response wire schema. The two
     * schemas share field names but live in different generated message classes; they are kept in
     * sync by this converter.
     *
     * <p>This method assumes every collection on the POJO (subtopologies, nodes, successors,
     * topics, stores, etc.) is non-null. The {@link StreamsGroupTopologyDescription} record and
     * its nested types enforce that invariant in their canonical constructors via
     * {@code Objects.requireNonNull} + {@code List.copyOf} / {@code Collections.unmodifiableSet},
     * so a well-formed plugin response can never reach this method with a null collection. A
     * pathological plugin that bypasses the constructor invariant would surface as an
     * {@code NullPointerException} here; that is caught at the only call site
     * ({@code StreamsGroupTopologyDescriptionManager#attachTopologyDescriptions}) and folded
     * into a per-group {@code TOPOLOGY_DESCRIPTION_STATUS_ERROR}, so the rest of the describe
     * batch is unaffected.
     */
    public static StreamsGroupDescribeResponseData.TopologyDescription toDescribeResponse(
        StreamsGroupTopologyDescription topology
    ) {
        List<StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology> subtopologies =
            new ArrayList<>(topology.subtopologies().size());
        for (Subtopology subtopology : topology.subtopologies()) {
            List<StreamsGroupDescribeResponseData.TopologyDescriptionNode> nodes =
                new ArrayList<>(subtopology.nodes().size());
            for (Node node : subtopology.nodes()) {
                nodes.add(toResponseNode(node));
            }
            subtopologies.add(
                new StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology()
                    .setSubtopologyId(subtopology.id())
                    .setNodes(nodes)
            );
        }
        List<StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore> globalStores =
            new ArrayList<>(topology.globalStores().size());
        for (GlobalStore globalStore : topology.globalStores()) {
            globalStores.add(
                new StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore()
                    .setSource(toResponseNode(globalStore.source()))
                    .setProcessor(toResponseNode(globalStore.processor()))
            );
        }
        return new StreamsGroupDescribeResponseData.TopologyDescription()
            .setSubtopologies(subtopologies)
            .setGlobalStores(globalStores);
    }

    private static StreamsGroupDescribeResponseData.TopologyDescriptionNode toResponseNode(Node node) {
        StreamsGroupDescribeResponseData.TopologyDescriptionNode wire =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName(node.name())
                .setSuccessors(new ArrayList<>(node.successors()));
        if (node instanceof Source source) {
            wire.setNodeType(NODE_TYPE_SOURCE);
            wire.setSourceTopics(new ArrayList<>(source.topics()));
        } else if (node instanceof Processor processor) {
            wire.setNodeType(NODE_TYPE_PROCESSOR);
            wire.setStores(new ArrayList<>(processor.stores()));
        } else if (node instanceof Sink sink) {
            wire.setNodeType(NODE_TYPE_SINK);
            sink.topic().ifPresent(wire::setSinkTopic);
        } else {
            throw new IllegalStateException(
                "Unknown topology node type: " + node.getClass().getName()
            );
        }
        return wire;
    }

    private static Node convertNode(
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wire
    ) {
        return switch (wire.nodeType()) {
            case NODE_TYPE_SOURCE -> new Source(
                wire.name(),
                new LinkedHashSet<>(wire.sourceTopics()),
                new LinkedHashSet<>(wire.successors())
            );
            case NODE_TYPE_PROCESSOR -> new Processor(
                wire.name(),
                new LinkedHashSet<>(wire.stores()),
                new LinkedHashSet<>(wire.successors())
            );
            case NODE_TYPE_SINK -> new Sink(
                wire.name(),
                Optional.ofNullable(wire.sinkTopic()),
                new LinkedHashSet<>(wire.successors())
            );
            default -> throw new InvalidRequestException(
                "Unknown topology node type: " + wire.nodeType()
            );
        };
    }
}
