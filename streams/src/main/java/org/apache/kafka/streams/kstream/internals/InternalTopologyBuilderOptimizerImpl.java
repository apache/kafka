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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * The {@code TopologyOptimizer} used to optimize topology built using the DSL.
 * This implementation traverses the graph and makes calls to the {@link InternalTopologyBuilder} instead
 * of calls from ineritors of {@link AbstractStream}
 */
public class InternalTopologyBuilderOptimizerImpl implements TopologyOptimizer {

    private final InternalTopologyBuilder internalTopologyBuilder;

    public InternalTopologyBuilderOptimizerImpl(InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
    }

    private static final Logger LOG = LoggerFactory.getLogger(InternalTopologyBuilderOptimizerImpl.class);

    @Override
    public void optimize(StreamsTopologyGraph topologyGraph) {
        final Deque<StreamsGraphNode> graphNodeStack = new ArrayDeque<>();

        graphNodeStack.push(topologyGraph.root);

        while (!graphNodeStack.isEmpty()) {
            final StreamsGraphNode streamGraphNode = graphNodeStack.pop();

            buildAndMaybeOptimize(internalTopologyBuilder, streamGraphNode);

            for (StreamsGraphNode descendant : streamGraphNode.getDescendants()) {
                graphNodeStack.push(descendant);
            }
        }
    }

    private void buildAndMaybeOptimize(final InternalTopologyBuilder internalTopologyBuilder,
                                       final StreamsGraphNode descendant) {

        final StreamsGraphNode.TopologyNodeType nodeType = descendant.getType();
        final ProcessDetails processDetails = descendant.getProcessed();
        String topic;

        switch (nodeType) {

            case TOPOLOGY_PARENT:
                LOG.info("Root of entire topology will process descendant nodes");
                break;

            case SOURCE:
                final Deserializer keyDeserializer = processDetails.consumedKeySerde() == null ? null : processDetails.consumedKeySerde().deserializer();
                final Deserializer valDeserializer = processDetails.consumedValueSerde() == null ? null : processDetails.consumedValueSerde().deserializer();
                internalTopologyBuilder.addSource(processDetails.getConsumedResetPolicy(),
                                                  descendant.name(),
                                                  processDetails.getConsumedTimestampExtractor(),
                                                  keyDeserializer,
                                                  valDeserializer,
                                                  processDetails.getSourceTopicArray());

                break;

            case MAP:
                internalTopologyBuilder.addProcessor(descendant.name(),
                                                     processDetails.getProcessorSupplier(),
                                                     descendant.getPredecessorName());

                break;

            case SINK:
                final Serializer keySerializer = processDetails.producedKeySerde() == null ? null : processDetails.producedKeySerde().serializer();
                final Serializer valSerializer = processDetails.producedValueSerde() == null ? null : processDetails.producedValueSerde().serializer();
                final StreamPartitioner partitioner = processDetails.streamPartitioner();
                topic = processDetails.getSinkTopic();

                internalTopologyBuilder.addSink(descendant.name(),
                                                topic,
                                                keySerializer,
                                                valSerializer,
                                                partitioner,
                                                descendant.getPredecessorName());
                break;

            case KTABLE:
                topic = processDetails.getSourceTopicArray()[0];

                internalTopologyBuilder.addSource(processDetails.getConsumedResetPolicy(),
                                                  descendant.name,
                                                  processDetails.getConsumedTimestampExtractor(),
                                                  processDetails.consumedKeySerde().deserializer(),
                                                  processDetails.consumedValueSerde().deserializer(),
                                                  topic);

                internalTopologyBuilder.addProcessor(descendant.name(),
                                                     processDetails.getProcessorSupplier(),
                                                     descendant.getPredecessorName());

                if (processDetails.getStoreBuilder() != null) {
                    internalTopologyBuilder.addStateStore(processDetails.getStoreBuilder(), descendant.name());
                    internalTopologyBuilder.connectSourceStoreAndTopic(processDetails.getStoreBuilder().name(), topic);

                } else if (processDetails.getStoreSupplier() != null) {
                    internalTopologyBuilder.addStateStore(processDetails.getStoreSupplier(), descendant.name());
                    internalTopologyBuilder.connectSourceStoreAndTopic(processDetails.getStoreSupplier().name(), topic);
                }

                break;

            case GROUP_BY:
                //TODO Need to figure out how to handle this case
                break;
            default:
                throw new TopologyException("Unrecognized TopologyNodeType " + nodeType);

        }
    }
}
