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
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StreamSourceNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.TableSourceNode;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class InternalStreamsBuilder implements InternalNameProvider {

    final InternalTopologyBuilder internalTopologyBuilder;
    private final AtomicInteger index = new AtomicInteger(0);
    private boolean topologyBuilt;

    private final AtomicInteger nodeIdCounter = new AtomicInteger(0);
    private final NodeIdComparator nodeIdComparator = new NodeIdComparator();

    private static final String TOPOLOGY_ROOT = "root";
    private static final Logger LOG = LoggerFactory.getLogger(InternalStreamsBuilder.class);

    protected final StreamsGraphNode root = new StreamsGraphNode(TOPOLOGY_ROOT, false) {
        @Override
        public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
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
        maybeAddNodeForOptimizationMetadata(streamSourceNode);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false, streamSourceNode);
    }

    public <K, V> KStream<K, V> stream(final Pattern topicPattern,
                                       final ConsumedInternal<K, V> consumed) {
        final String name = newProcessorName(KStreamImpl.SOURCE_NAME);

        StreamSourceNode<K, V> streamPatternSourceNode = new StreamSourceNode<>(name,
                                                                                topicPattern,
                                                                                consumed);
        root.addChildNode(streamPatternSourceNode);
        maybeAddNodeForOptimizationMetadata(streamPatternSourceNode);

        return new KStreamImpl<>(this,
                                 name,
                                 Collections.singleton(name),
                                 false,
                                 streamPatternSourceNode);
    }

    @SuppressWarnings("unchecked")
    public <K, V, S extends StateStore> KTable<K, V> table(final String topic,
                                                           final ConsumedInternal<K, V> consumed,
                                                           final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {

        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = new KeyValueStoreMaterializer<>(materialized).materialize();

        final String source = newProcessorName(KStreamImpl.SOURCE_NAME);
        final String name = newProcessorName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeBuilder.name());
        final ProcessorParameters processorParameters = new ProcessorParameters<>(processorSupplier, name);

        TableSourceNode.TableSourceNodeBuilder<K, V, S> tableSourceNodeBuilder = TableSourceNode.tableSourceNodeBuilder();

        TableSourceNode<K, V, S> tableSourceNode = tableSourceNodeBuilder.withNodeName(name)
                                                                               .withSourceName(source)
                                                                               .withStoreBuilder(storeBuilder)
                                                                               .withConsumedInternal(consumed)
                                                                               .withProcessorParameters(processorParameters)
                                                                               .withTopic(topic)
                                                                               .build();

        root.addChildNode(tableSourceNode);
        maybeAddNodeForOptimizationMetadata(tableSourceNode);

        return new KTableImpl<>(this,
                                name,
                                processorSupplier,
                                consumed.keySerde(),
                                consumed.valueSerde(),
                                Collections.singleton(source),
                                storeBuilder.name(),
                                materialized.isQueryable(),
                                tableSourceNode);
    }

    @SuppressWarnings("unchecked")
    public <K, V, S extends StateStore> GlobalKTable<K, V> globalTable(final String topic,
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

        TableSourceNode<K, V, S> tableSourceNode = TableSourceNode.tableSourceNodeBuilder().withStoreBuilder(storeBuilder)
                                                                                                    .withSourceName(sourceName)
                                                                                                    .withConsumedInternal(consumed)
                                                                                                    .withTopic(topic)
                                                                                                    .withProcessorParameters(processorParameters)
                                                                                                    .isGlobalKTable(true)
                                                                                                    .build();

        root.addChildNode(tableSourceNode);
        maybeAddNodeForOptimizationMetadata(tableSourceNode);

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

    void maybeAddNodeForOptimizationMetadata(final StreamsGraphNode node) {
        node.setId(nodeIdCounter.getAndIncrement());
        node.setInternalStreamsBuilder(this);
    }

    public void buildAndOptimizeTopology() {
        if (!topologyBuilt) {

            final PriorityQueue<StreamsGraphNode> graphNodePriorityQueue = new PriorityQueue<>(5, nodeIdComparator);

            graphNodePriorityQueue.offer(root);

            while (!graphNodePriorityQueue.isEmpty()) {
                final StreamsGraphNode streamGraphNode = graphNodePriorityQueue.remove();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding nodes to topology {} child nodes {}", streamGraphNode, streamGraphNode.children());
                }

                streamGraphNode.writeToTopology(internalTopologyBuilder);

                for (StreamsGraphNode graphNode : streamGraphNode.children()) {
                    graphNodePriorityQueue.offer(graphNode);
                }
            }

            topologyBuilt = true;
        }
    }


    public StreamsGraphNode root() {
        return root;
    }

    private static class NodeIdComparator implements Comparator<StreamsGraphNode>, Serializable {

        @Override
        public int compare(final StreamsGraphNode o1,
                           final StreamsGraphNode o2) {
            return o1.id().compareTo(o2.id());
        }
    }
}
