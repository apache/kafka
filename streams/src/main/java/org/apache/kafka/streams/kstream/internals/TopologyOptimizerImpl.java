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
 * of calls from inheritors of {@link AbstractStream}
 */
@SuppressWarnings("unchecked")
public class TopologyOptimizerImpl implements TopologyOptimizer {


    private static final Logger LOG = LoggerFactory.getLogger(TopologyOptimizerImpl.class);

    @Override
    public void optimize(StreamsTopologyGraph topologyGraph, InternalTopologyBuilder internalTopologyBuilder) {
        final Deque<StreamsGraphNode> graphNodeStack = new ArrayDeque<>();

        graphNodeStack.push(topologyGraph.root);

        LOG.debug(String.format("Root node %s descendants %s", topologyGraph.root, topologyGraph.root.descendants));

        while (!graphNodeStack.isEmpty()) {
            final StreamsGraphNode streamGraphNode = graphNodeStack.pop();

            buildAndMaybeOptimize(internalTopologyBuilder, streamGraphNode);

            for (StreamsGraphNode descendant : streamGraphNode.getDescendants()) {
                if (streamGraphNode.descendants.size() > 1) {
                    LOG.debug(String.format("Adding to bottom of stack %s descendants %s", descendant, descendant.descendants));
                    graphNodeStack.addLast(descendant);
                } else {
                    LOG.debug(String.format("Adding to top of stack %s descendants %s", descendant, descendant.descendants));
                    graphNodeStack.push(descendant);
                }
            }
        }
    }

    private void buildAndMaybeOptimize(final InternalTopologyBuilder internalTopologyBuilder,
                                       final StreamsGraphNode descendant) {

        final TopologyNodeType nodeType = descendant.getType();
        final ProcessDetails processDetails = descendant.getProcessed();

        switch (nodeType) {

            case TOPOLOGY_PARENT:
                LOG.info("Root of entire topology will process descendant nodes");
                break;

            case SOURCE:
                buildSourceNode(internalTopologyBuilder, descendant, processDetails);

                break;

            case SINK:
                buildSinkNode(internalTopologyBuilder, descendant, processDetails);
                break;

            case KTABLE:
                buildKTableNode(internalTopologyBuilder, descendant, processDetails);

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

                buildProcessingNodeWithPossibleStateStore(internalTopologyBuilder, descendant, processDetails);
                break;

            case STREAM_STREAM_JOIN:
                buildStreamStreamJoin(internalTopologyBuilder, (StreamStreamJoinGraphNode) descendant);

                break;

            case SOURCE_SINK:
                buildSourceSinkNode(internalTopologyBuilder, (SourceSinkNode) descendant, processDetails);

                break;

            case STREAM_KTABLE_JOIN:

                buildStreamKTableJoin(internalTopologyBuilder, descendant, processDetails);

                break;

            case KTABLE_KTABLE_JOIN:

                buildKTableKTableJoin(internalTopologyBuilder, (KTableJoinGraphNode) descendant, processDetails);

                break;

            case STREAM_GLOBAL_TABLE_JOIN:
                internalTopologyBuilder.addProcessor(descendant.name(), processDetails.getProcessorSupplier(), descendant.getPredecessorName());

                break;

            case GLOBAL_KTABLE:
                buildGlobalKTable(internalTopologyBuilder, descendant, processDetails);

                break;

            case AGGREGATE:

                buildAggregateProcessor(internalTopologyBuilder, descendant, processDetails);

                break;
            default:
                throw new TopologyException("Unrecognized TopologyNodeType " + nodeType);

        }
    }

    private void buildAggregateProcessor(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant, ProcessDetails processDetails) {
        internalTopologyBuilder.addProcessor(descendant.name(), processDetails.getProcessorSupplier(), processDetails.getConnectProcessorName());
        if (processDetails.getStoreBuilder() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreBuilder(), descendant.name());
        } else if (processDetails.getStoreSupplier() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreSupplier(), descendant.name());
        }
    }

    private void buildGlobalKTable(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant, ProcessDetails processDetails) {

        String topic = processDetails.getSourceTopicArray()[0];
        Deserializer keyDeserializer = getDeserializer(processDetails.consumedKeySerde());
        Deserializer valDeserializer = getDeserializer(processDetails.consumedValueSerde());

        internalTopologyBuilder.addGlobalStore(processDetails.getStoreBuilder(),
                                               descendant.name,
                                               processDetails.getConsumedTimestampExtractor(),
                                               keyDeserializer,
                                               valDeserializer,
                                               topic,
                                               descendant.getPredecessorName(),
                                               processDetails.getkTableSource());
    }

    private void buildKTableKTableJoin(InternalTopologyBuilder internalTopologyBuilder, KTableJoinGraphNode ktg, ProcessDetails processDetails) {

        internalTopologyBuilder.addProcessor(ktg.joinThisName, ktg.joinThisProcessor, ktg.name);
        internalTopologyBuilder.addProcessor(ktg.joinOtherName, ktg.joinOtherProcessor, ktg.otherKTableName);
        internalTopologyBuilder.addProcessor(ktg.joinMerggeName, ktg.joinMergeProcessor, ktg.joinThisName, ktg.joinOtherName);
        internalTopologyBuilder.connectProcessorAndStateStores(ktg.joinThisName, ktg.joinThisStoreNames);
        internalTopologyBuilder.connectProcessorAndStateStores(ktg.joinOtherName, ktg.joinOtherStoreNames);

        if (processDetails != null && processDetails.getStoreBuilder() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreBuilder(), ktg.joinMerggeName);
        } else if (processDetails != null && processDetails.getStoreSupplier() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreSupplier(), ktg.joinMerggeName);
        }
    }

    private void buildStreamKTableJoin(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant, ProcessDetails processDetails) {
        internalTopologyBuilder.addProcessor(descendant.name(), processDetails.getProcessorSupplier(), descendant.getPredecessorName());
        internalTopologyBuilder.connectProcessorAndStateStores(descendant.name(), processDetails.getStoreNames());
        internalTopologyBuilder.connectProcessors(descendant.name(), processDetails.getConnectProcessorName());
    }

    private void buildSourceSinkNode(InternalTopologyBuilder internalTopologyBuilder, SourceSinkNode rgn, ProcessDetails processDetails) {

        Serializer keySerializer = rgn.keySerde != null ? rgn.keySerde.serializer() : null;
        Serializer valSerializer = rgn.valueSerde != null ? rgn.valueSerde.serializer() : null;
        Deserializer keyDeserializer = rgn.keySerde != null ? rgn.keySerde.deserializer() : null;
        Deserializer valDeserializer = rgn.valueSerde != null ? rgn.valueSerde.deserializer() : null;

        internalTopologyBuilder.addInternalTopic(rgn.sinkTopic);
        internalTopologyBuilder.addProcessor(rgn.funcOrFilterName, rgn.processorSupplier, rgn.name());

        valDeserializer = rgn.changedDeserializer != null ? rgn.changedDeserializer : valDeserializer;
        valSerializer = rgn.changedSerializer != null ? rgn.changedSerializer : valSerializer;

        internalTopologyBuilder.addSink(rgn.sinkName, rgn.sinkTopic, keySerializer, valSerializer,
                                        null, rgn.funcOrFilterName);
        internalTopologyBuilder.addSource(null, rgn.sourceName, new FailOnInvalidTimestamp(),
                                          keyDeserializer, valDeserializer, rgn.sinkTopic);

        if (processDetails != null && processDetails.getStoreSupplier() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreSupplier(), rgn.funcOrFilterName);
        } else if (processDetails != null && processDetails.getMaterialized() != null) {
            internalTopologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(processDetails.getMaterializedInternal())
                                                      .materialize(), rgn.funcOrFilterName);
        }
    }

    private void buildStreamStreamJoin(InternalTopologyBuilder internalTopologyBuilder, StreamStreamJoinGraphNode jgn) {

        internalTopologyBuilder.addProcessor(jgn.thisWindowStreamName, jgn.thisWindowedStreamProcessor, jgn.leftHandSideCallingStream);
        internalTopologyBuilder.addProcessor(jgn.otherWindowStreamName, jgn.otherWindowedStreamProcessor, jgn.otherStreamName);
        internalTopologyBuilder.addProcessor(jgn.joinThisName, jgn.joinThisProcessor, jgn.thisWindowStreamName);
        internalTopologyBuilder.addProcessor(jgn.joinOtherName, jgn.joinOtherProcessor, jgn.otherWindowStreamName);
        internalTopologyBuilder.addProcessor(jgn.joinMergeName, jgn.joinMergeProcessor, jgn.joinThisName, jgn.joinOtherName);
        internalTopologyBuilder.addStateStore(jgn.thisWindowBuilder, jgn.thisWindowStreamName, jgn.joinOtherName);
        internalTopologyBuilder.addStateStore(jgn.otherWindowBuilder, jgn.otherWindowStreamName, jgn.joinThisName);
    }

    private void buildProcessingNodeWithPossibleStateStore(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant,
                                                           ProcessDetails processDetails) {
        internalTopologyBuilder.addProcessor(descendant.name(),
                                             processDetails.getProcessorSupplier(),
                                             descendant.getPredecessorName());

        if (processDetails.getStoreNames() != null && processDetails.getStoreNames().length > 0) {
            internalTopologyBuilder.connectProcessorAndStateStores(descendant.name(), processDetails.getStoreNames());
        }

        if (processDetails.getStoreSupplier() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreSupplier(), descendant.name());
        } else if (processDetails.getStoreBuilder() != null) {
            internalTopologyBuilder.addStateStore(processDetails.getStoreBuilder(), descendant.name());
        } else if (processDetails.getMaterialized() != null) {
            internalTopologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(processDetails.getMaterializedInternal()).materialize());
        }
    }

    private void buildKTableNode(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant, ProcessDetails processDetails) {

        String topic = processDetails.getSourceTopicArray()[0];
        Deserializer keyDeserializer = getDeserializer(processDetails.consumedKeySerde());
        Deserializer valDeserializer = getDeserializer(processDetails.consumedValueSerde());

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
    }

    private void buildSinkNode(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant, ProcessDetails processDetails) {

        Serializer keySerializer = getSerializer(processDetails.producedKeySerde());
        Serializer valSerializer = getSerializer(processDetails.producedValueSerde());
        final StreamPartitioner partitioner = processDetails.streamPartitioner();
        String topic = processDetails.getSinkTopic();

        internalTopologyBuilder.addSink(descendant.name(),
                                        topic,
                                        keySerializer,
                                        valSerializer,
                                        partitioner,
                                        descendant.getPredecessorName());
    }

    private void buildSourceNode(InternalTopologyBuilder internalTopologyBuilder, StreamsGraphNode descendant, ProcessDetails processDetails) {
        Deserializer keyDeserializer = getDeserializer(processDetails.consumedKeySerde());
        Deserializer valDeserializer = getDeserializer(processDetails.consumedValueSerde());

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
    }


    private Serializer getSerializer(Serde serde) {
        return serde == null ? null : serde.serializer();
    }

    private Deserializer getDeserializer(Serde serde) {
        return serde == null ? null : serde.deserializer();
    }

}
