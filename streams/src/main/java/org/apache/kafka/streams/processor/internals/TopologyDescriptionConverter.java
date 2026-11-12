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

import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.streams.TopologyDescription;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TopologyDescriptionConverter {

    private static final byte NODE_TYPE_SOURCE = 1;
    private static final byte NODE_TYPE_PROCESSOR = 2;
    private static final byte NODE_TYPE_SINK = 3;

    private TopologyDescriptionConverter() {}

    public static StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription toWire(final TopologyDescription description,
                                                                                              final Function<String, String> topicNameDecorator) {
        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription();

        wire.setSubtopologies(description.subtopologies().stream()
            .sorted(Comparator.comparingInt(TopologyDescription.Subtopology::id))
            .map(s -> toWireSubtopology(s, topicNameDecorator))
            .collect(Collectors.toList()));

        wire.setGlobalStores(description.globalStores().stream()
            .sorted(Comparator.comparingInt(TopologyDescription.GlobalStore::id))
            .map(g -> toWireGlobalStore(g, topicNameDecorator))
            .collect(Collectors.toList()));

        return wire;
    }

    private static StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology toWireSubtopology(
            final TopologyDescription.Subtopology subtopology,
            final Function<String, String> topicNameDecorator) {
        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology wire =
                new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology();
        wire.setSubtopologyId(String.valueOf(subtopology.id()));
        wire.setNodes(subtopology.nodes().stream()
                .sorted(Comparator.comparing(TopologyDescription.Node::name))
                .map(n -> toWireNode(n, topicNameDecorator))
                .collect(Collectors.toList()));
        return wire;
    }

    private static StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode toWireNode(
            final TopologyDescription.Node node,
            final Function<String, String> topicNameDecorator) {
        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wire =
                new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode();
        wire.setName(node.name());
        wire.setSuccessors(node.successors().stream()
                .map(TopologyDescription.Node::name)
                .sorted()
                .collect(Collectors.toList()));

        if (node instanceof TopologyDescription.Source) {
            final TopologyDescription.Source source = (TopologyDescription.Source) node;
            wire.setNodeType(NODE_TYPE_SOURCE);
            wire.setSourceTopics(source.topicSet() == null
                    ? new ArrayList<>()
                    : source.topicSet().stream()
                        .map(topicNameDecorator)
                        .sorted()
                        .collect(Collectors.toList()));
        } else if (node instanceof TopologyDescription.Sink) {
            final TopologyDescription.Sink sink = (TopologyDescription.Sink) node;
            wire.setNodeType(NODE_TYPE_SINK);
            wire.setSinkTopic(topicNameDecorator.apply(sink.topic()));
        } else if (node instanceof TopologyDescription.Processor) {
            final TopologyDescription.Processor processor = (TopologyDescription.Processor) node;
            wire.setNodeType(NODE_TYPE_PROCESSOR);
            wire.setStores(processor.stores().stream().sorted().collect(Collectors.toList()));
        } else {
            throw new IllegalStateException("Unknown node type: " + node.getClass().getName());
        }
        return wire;
    }

    private static StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore toWireGlobalStore(
            final TopologyDescription.GlobalStore globalStore,
            final Function<String, String> topicNameDecorator) {
        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore wire =
                new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore();
        wire.setSource(toWireNode(globalStore.source(), topicNameDecorator));
        wire.setProcessor(toWireNode(globalStore.processor(), topicNameDecorator));
        return wire;
    }
}
