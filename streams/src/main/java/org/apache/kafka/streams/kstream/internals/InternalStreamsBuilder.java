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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class InternalStreamsBuilder implements InternalNameProvider {

    final InternalTopologyBuilder internalTopologyBuilder;
    private final AtomicInteger index = new AtomicInteger(0);

    private final AtomicInteger nodeIdCounter = new AtomicInteger(0);
    private final Map<StreamsGraphNode, Set<StreamsGraphNode>> repartitioningNodeToRepartitioned = new HashMap<>();
    private final Map<StreamsGraphNode, StreamSinkNode> stateStoreNodeToSinkNodes = new HashMap<>();

    private static final String TOPOLOGY_ROOT = "root";
    private static final Logger LOG = LoggerFactory.getLogger(InternalStreamsBuilder.class);

    protected final StreamsGraphNode root = new StreamsGraphNode(TOPOLOGY_ROOT, false) {
        @Override
        void writeToTopology(InternalTopologyBuilder topologyBuilder) {
            // no-op for root node
        }
    };

    public InternalStreamsBuilder(final InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
    }

    public <K, V> KStream<K, V> stream(final Collection<String> topics,
                                       final ConsumedInternal<K, V> consumed) {
        final String name = newProcessorName(KStreamImpl.SOURCE_NAME);

        StreamSourceNode<K, V> streamSourceNode = new StreamSourceNode<>(name,
                                                                         topics,
                                                                         consumed);

        root.addChildNode(streamSourceNode);
        streamSourceNode.setParentNode(root);

        addNode(streamSourceNode);

        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          name,
                                          consumed.timestampExtractor(),
                                          consumed.keyDeserializer(),
                                          consumed.valueDeserializer(),
                                          topics.toArray(new String[topics.size()]));

        return new KStreamImpl<>(this, name, Collections.singleton(name), false, streamSourceNode);
    }

    public <K, V> KStream<K, V> stream(final Pattern topicPattern,
                                       final ConsumedInternal<K, V> consumed) {
        final String name = newProcessorName(KStreamImpl.SOURCE_NAME);

        StreamSourceNode<K, V> streamPatternSourceNode = new StreamSourceNode<>(name,
                                                                                topicPattern,
                                                                                consumed);
        root.addChildNode(streamPatternSourceNode);
        streamPatternSourceNode.setParentNode(root);

        addNode(streamPatternSourceNode);
        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          name,
                                          consumed.timestampExtractor(),
                                          consumed.keyDeserializer(),
                                          consumed.valueDeserializer(),
                                          topicPattern);

        return new KStreamImpl<>(this,
                                 name,
                                 Collections.singleton(name),
                                 false,
                                 streamPatternSourceNode);
    }

    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(final String topic,
                                     final ConsumedInternal<K, V> consumed,
                                     final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {

        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = new KeyValueStoreMaterializer<>(materialized).materialize();

        final String source = newProcessorName(KStreamImpl.SOURCE_NAME);
        final String name = newProcessorName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeBuilder.name());
        final ProcessorParameters processorParameters = new ProcessorParameters<>(processorSupplier, name);

        StatefulSourceNode.StatefulSourceNodeBuilder<K, V> statefulSourceNodeBuilder = StatefulSourceNode.statefulSourceNodeBuilder();


        StatefulSourceNode<K, V> statefulSourceNode = statefulSourceNodeBuilder.withNodeName(name)
                                                                               .withSourceName(source)
                                                                               .withStoreBuilder(storeBuilder)
                                                                               .withConsumedInternal(consumed)
                                                                               .withProcessorParameters(processorParameters)
                                                                               .withTopic(topic)
                                                                               .build();

        root.addChildNode(statefulSourceNode);
        statefulSourceNode.setParentNode(root);
        addNode(statefulSourceNode);

        
        internalTopologyBuilder.addStateStore(storeBuilder, name);
        internalTopologyBuilder.markSourceStoreAndTopic(storeBuilder, topic);

        internalTopologyBuilder.addSource(consumed.offsetResetPolicy(),
                                          source,
                                          consumed.timestampExtractor(),
                                          consumed.keyDeserializer(),
                                          consumed.valueDeserializer(),
                                          topic);
        internalTopologyBuilder.addProcessor(name, processorSupplier, source);

        return new KTableImpl<>(this,
                                       name,
                                       processorSupplier,
                                       consumed.keySerde(),
                                       consumed.valueSerde(),
                                       Collections.singleton(source),
                                       storeBuilder.name(),
                                       materialized.isQueryable(),
                                       statefulSourceNode);
    }

    @SuppressWarnings("unchecked")
    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                 final ConsumedInternal<K, V> consumed,
                                                 final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(consumed, "consumed can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        // explicitly disable logging for global stores
        materialized.withLoggingDisabled();
        final StoreBuilder storeBuilder = new KeyValueStoreMaterializer<>(materialized).materialize();
        final String sourceName = newProcessorName(KStreamImpl.SOURCE_NAME);
        final String processorName = newProcessorName(KTableImpl.SOURCE_NAME);
        final KTableSource<K, V> tableSource = new KTableSource<>(storeBuilder.name());

        final ProcessorParameters processorParameters = new ProcessorParameters(tableSource, processorName);

        StatefulSourceNode<K, V> statefulSourceNode = StatefulSourceNode.statefulSourceNodeBuilder().withStoreBuilder(storeBuilder)
                                                                                                    .withSourceName(sourceName)
                                                                                                    .withConsumedInternal(consumed)
                                                                                                    .withTopic(topic)
                                                                                                    .withProcessorParameters(processorParameters)
                                                                                                    .isGlobalKTable(true)
                                                                                                    .build();

        root.addChildNode(statefulSourceNode);
        statefulSourceNode.setParentNode(root);
        addNode(statefulSourceNode);

        addNode(statefulSourceNode);

        internalTopologyBuilder.addGlobalStore(storeBuilder,
                                               sourceName,
                                               consumed.timestampExtractor(),
                                               consumed.keyDeserializer(),
                                               consumed.valueDeserializer(),
                                               topic,
                                               processorName,
                                               tableSource);

        return new GlobalKTableImpl<>(new KTableSourceValueGetterSupplier<K, V>(storeBuilder.name()), materialized.isQueryable());
    }

    @Override
    public String newProcessorName(final String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }

    @Override
    public String newStoreName(final String prefix) {
        return prefix + String.format(KTableImpl.STATE_STORE_NAME + "%010d", index.getAndIncrement());
    }

    public synchronized void addStateStore(final StoreBuilder builder) {
        internalTopologyBuilder.addStateStore(builder);
    }

    public synchronized void addGlobalStore(final StoreBuilder<KeyValueStore> storeBuilder,
                                            final String sourceName,
                                            final String topic,
                                            final ConsumedInternal consumed,
                                            final String processorName,
                                            final ProcessorSupplier stateUpdateSupplier) {
        // explicitly disable logging for global stores
        storeBuilder.withLoggingDisabled();
        internalTopologyBuilder.addGlobalStore(storeBuilder,
                                               sourceName,
                                               consumed.timestampExtractor(),
                                               consumed.keyDeserializer(),
                                               consumed.valueDeserializer(),
                                               topic,
                                               processorName,
                                               stateUpdateSupplier);
    }

    public synchronized void addGlobalStore(final StoreBuilder<KeyValueStore> storeBuilder,
                                            final String topic,
                                            final ConsumedInternal consumed,
                                            final ProcessorSupplier stateUpdateSupplier) {
        // explicitly disable logging for global stores
        storeBuilder.withLoggingDisabled();
        final String sourceName = newProcessorName(KStreamImpl.SOURCE_NAME);
        final String processorName = newProcessorName(KTableImpl.SOURCE_NAME);
        addGlobalStore(storeBuilder,
                       sourceName,
                       topic,
                       consumed,
                       processorName,
                       stateUpdateSupplier);
    }


    void addNode(final StreamsGraphNode node) {
        //TODO uncomment when actually building graph
//        node.setId(nodeIdCounter.getAndIncrement());
//        node.setInternalStreamsBuilder(this);
//
//        LOG.debug("Adding node {}", node);
//
//        if (node.parentNode() == null && !node.nodeName().equals(TOPOLOGY_ROOT)) {
//            throw new IllegalStateException(
//                "Nodes should not have a null parent node.  Name: " + node.nodeName() + " Type: "
//                + node.getClass().getSimpleName());
//        }
//
//        if (node.triggersRepartitioning()) {
//            repartitioningNodeToRepartitioned.put(node, new HashSet<StreamsGraphNode>());
//        } else if (node.repartitionRequired()) {
//            StreamsGraphNode currentNode = node;
//            while (currentNode != null) {
//                final StreamsGraphNode parentNode = currentNode.parentNode();
//                if (parentNode.triggersRepartitioning()) {
//                    repartitioningNodeToRepartitioned.get(parentNode).add(node);
//                    break;
//                }
//                currentNode = parentNode.parentNode();
//            }
//        }
    }

    public StreamsGraphNode root() {
        return root;
    }

    /**
     * Used for hints when a node in the topology triggers a repartition and the repartition flag
     * is propagated down through the descendant nodes of the topology.  This can be used to help make an
     * optimization where the triggering node does an eager "through" operation and the child nodes can ignore
     * the need to repartition.
     *
     * @return Map&lt;StreamGraphNode, Set&lt;StreamGraphNode&gt;&gt;
     */
    public Map<StreamsGraphNode, Set<StreamsGraphNode>> getRepartitioningNodeToRepartitioned() {
        Map<StreamsGraphNode, Set<StreamsGraphNode>> copy = new HashMap<>(repartitioningNodeToRepartitioned);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Used for hints when an Aggregation operation is directly output to a Sink topic.
     * This map can be used to help optimize this case and use the Sink topic as the changelog topic
     * for the state store of the aggregation.
     *
     * @return Map&lt;StreamGraphNode, StreamSinkNode&gt;
     */
    public Map<StreamsGraphNode, StreamSinkNode> getStateStoreNodeToSinkNodes() {
        Map<StreamsGraphNode, StreamSinkNode> copy = new HashMap<>(stateStoreNodeToSinkNodes);
        return Collections.unmodifiableMap(copy);
    }
}
