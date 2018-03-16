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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
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
@SuppressWarnings("unchecked")
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

        System.out.println("Root node " + topologyGraph.root + " descendants " + topologyGraph.root.descendants);

        while (!graphNodeStack.isEmpty()) {
            final StreamsGraphNode streamGraphNode = graphNodeStack.pop();

            buildAndMaybeOptimize(internalTopologyBuilder, streamGraphNode);

            for (StreamsGraphNode descendant : streamGraphNode.getDescendants()) {
                if (streamGraphNode.descendants.size() > 1) {
                    System.out.println("Adding to bottom of stack " + descendant + "descendants " + descendant.descendants);
                    graphNodeStack.addLast(descendant);
                } else {
                    System.out.println("Adding to top of stack " + descendant + "descendants " + descendant.descendants);
                    graphNodeStack.push(descendant);
                }
            }
        }
    }

    private void buildAndMaybeOptimize(final InternalTopologyBuilder internalTopologyBuilder,
                                       final StreamsGraphNode descendant) {

        final StreamsGraphNode.TopologyNodeType nodeType = descendant.getType();
        final ProcessDetails processDetails = descendant.getProcessed();
        Deserializer keyDeserializer;
        Deserializer valDeserializer;
        Serializer keySerializer;
        Serializer valSerializer;
        String topic;

        switch (nodeType) {

            case TOPOLOGY_PARENT:
                LOG.info("Root of entire topology will process descendant nodes");
                break;

            case SOURCE:
                keyDeserializer = getDeserializer(processDetails.consumedKeySerde());
                valDeserializer = getDeserializer(processDetails.consumedValueSerde());

                if (processDetails.getSourcePattern() != null) {
                    internalTopologyBuilder.addSource(processDetails.getConsumedResetPolicy(),
                                                      descendant.name(),
                                                      processDetails.getConsumedTimestampExtractor(),
                                                      keyDeserializer,
                                                      valDeserializer,
                                                      processDetails.getSourcePattern());
                } else {
                    internalTopologyBuilder.addSource(processDetails.getConsumedResetPolicy(),
                                                      descendant.name(),
                                                      processDetails.getConsumedTimestampExtractor(),
                                                      keyDeserializer,
                                                      valDeserializer,
                                                      processDetails.getSourceTopicArray());
                }

                break;

            case SINK:
                keySerializer = getSerializer(processDetails.producedKeySerde());
                valSerializer = getSerializer(processDetails.producedValueSerde());
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
                keyDeserializer = getDeserializer(processDetails.consumedKeySerde());
                valDeserializer = getDeserializer(processDetails.consumedValueSerde());

                internalTopologyBuilder.addSource(processDetails.getConsumedResetPolicy(),
                                                  descendant.predecessorName,
                                                  processDetails.getConsumedTimestampExtractor(),
                                                  keyDeserializer,
                                                  valDeserializer,
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

            case PROCESSING:
            case SELECT_KEY:
            case MAP:
            case FLATMAP:

                internalTopologyBuilder.addProcessor(descendant.name(),
                                                     processDetails.getProcessorSupplier(),
                                                     descendant.getPredecessorName());

                break;
            case PROCESSOR:
            case TRANSFORM:
            case TRANSFORM_VALUES:
                internalTopologyBuilder.addProcessor(descendant.name(),
                                                     processDetails.getProcessorSupplier(),
                                                     descendant.getPredecessorName());

                if (processDetails.getStoreNames() != null && processDetails.getStoreNames().length > 0) {
                    internalTopologyBuilder.connectProcessorAndStateStores(descendant.name(), processDetails.getStoreNames());
                }
                break;

            case JOIN:
                JoinGraphNode jgn = (JoinGraphNode) descendant;

                internalTopologyBuilder.addProcessor(jgn.thisWindowStreamName, jgn.thisWindowedStreamProcessor, jgn.leftHandSideCallingStream);
                internalTopologyBuilder.addProcessor(jgn.otherWindowStreamName, jgn.otherWindowedStreamProcessor, jgn.otherStreamName);
                internalTopologyBuilder.addProcessor(jgn.joinThisName, jgn.joinThisProcessor, jgn.thisWindowStreamName);
                internalTopologyBuilder.addProcessor(jgn.joinOtherName, jgn.joinOtherProcessor, jgn.otherWindowStreamName);
                internalTopologyBuilder.addProcessor(jgn.joinMergeName, jgn.joinMergeProcessor, jgn.joinThisName, jgn.joinOtherName);
                internalTopologyBuilder.addStateStore(jgn.thisWindowBuilder, jgn.thisWindowStreamName, jgn.joinOtherName);
                internalTopologyBuilder.addStateStore(jgn.otherWindowBuilder, jgn.otherWindowStreamName, jgn.joinThisName);

                break;

            case REPARTITION:
                RepartitionGraphNode rgn = (RepartitionGraphNode) descendant;

                keySerializer = rgn.keySerde != null ? rgn.keySerde.serializer() : null;
                valSerializer = rgn.valueSerde != null ? rgn.valueSerde.serializer() : null;
                keyDeserializer = rgn.keySerde != null ? rgn.keySerde.deserializer() : null;
                valDeserializer = rgn.valueSerde != null ? rgn.valueSerde.deserializer() : null;

                internalTopologyBuilder.addInternalTopic(rgn.repartitionTopic);
                internalTopologyBuilder.addProcessor(rgn.filterName, rgn.processorSupplier, rgn.name());

                internalTopologyBuilder.addSink(rgn.sinkName, rgn.repartitionTopic, keySerializer, valSerializer,
                                                null, rgn.filterName);
                internalTopologyBuilder.addSource(null, rgn.sourceName, new FailOnInvalidTimestamp(),
                                                  keyDeserializer, valDeserializer, rgn.repartitionTopic);
                break;

            case STREAM_KTABLE_JOIN:

                internalTopologyBuilder.addProcessor(descendant.name(), processDetails.getProcessorSupplier(), descendant.getPredecessorName());
                internalTopologyBuilder.connectProcessorAndStateStores(descendant.name(), processDetails.getStoreNames());
                internalTopologyBuilder.connectProcessors(descendant.name(), processDetails.getConnectProcessorName());

                break;

            case STREAM_GLOBAL_TABLE_JOIN:
                internalTopologyBuilder.addProcessor(descendant.name(), processDetails.getProcessorSupplier(), descendant.getPredecessorName());

                break;

            case GLOBAL_KTABLE:
                topic = processDetails.getSourceTopicArray()[0];
                keyDeserializer = getDeserializer(processDetails.consumedKeySerde());
                valDeserializer = getDeserializer(processDetails.consumedValueSerde());

                internalTopologyBuilder.addGlobalStore(processDetails.getStoreBuilder(),
                                               descendant.name,
                                               processDetails.getConsumedTimestampExtractor(),
                                               keyDeserializer,
                                               valDeserializer,
                                               topic,
                                               descendant.getPredecessorName(),
                                               processDetails.getkTableSource());

                break;

            case AGGREGATE:

                internalTopologyBuilder.addProcessor(descendant.name(), processDetails.getProcessorSupplier(), processDetails.getConnectProcessorName());
                if (processDetails.getStoreBuilder() != null) {
                    internalTopologyBuilder.addStateStore(processDetails.getStoreBuilder(), descendant.name());
                } else if (processDetails.getStoreSupplier() != null) {
                    internalTopologyBuilder.addStateStore(processDetails.getStoreSupplier(), descendant.name());
                }
                break;
            default:
                throw new TopologyException("Unrecognized TopologyNodeType " + nodeType);

        }
    }


    private Serializer getSerializer(Serde serde) {
        return serde == null ? null : serde.serializer();
    }

    private Deserializer getDeserializer(Serde serde) {
        return serde == null ? null : serde.deserializer();
    }

}
