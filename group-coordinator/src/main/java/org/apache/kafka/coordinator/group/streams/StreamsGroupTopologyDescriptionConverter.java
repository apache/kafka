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
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.GlobalStore;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Node;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Processor;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Sink;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Source;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Subtopology;

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
