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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.kstream.internals.StreamsTopologyGraph.TOPOLOGY_ROOT;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.GLOBAL_KTABLE;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.KTABLE;
import static org.apache.kafka.streams.kstream.internals.TopologyNodeType.SOURCE;

public class InternalStreamsBuilder implements InternalNameProvider {

    final InternalTopologyBuilder internalTopologyBuilder;

    private final StreamsTopologyGraph topologyGraph;

    private final AtomicInteger index = new AtomicInteger(0);

    public InternalStreamsBuilder(final InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
        this.topologyGraph = new StreamsTopologyGraphImpl();
    }

    public <K, V> KStream<K, V> stream(final Collection<String> topics,
                                       final ConsumedInternal<K, V> consumed) {
        final String name = newProcessorName(KStreamImpl.SOURCE_NAME);

        ProcessDetails processDetails = ProcessDetails.builder().withConsumed(consumed).withSourceTopics(topics).build();
        StreamsGraphNode node = new StreamsGraphNode(name, SOURCE, false, processDetails, TOPOLOGY_ROOT);

        topologyGraph.addNode(node);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    public <K, V> KStream<K, V> stream(final Pattern topicPattern, final ConsumedInternal<K, V> consumed) {
        final String name = newProcessorName(KStreamImpl.SOURCE_NAME);

        ProcessDetails processDetails = ProcessDetails.builder().withSourcePattern(topicPattern).withConsumed(consumed).build();
        StreamsGraphNode node = new StreamsGraphNode(name, SOURCE, false, processDetails, TOPOLOGY_ROOT);
        topologyGraph.addNode(node);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public <K, V> KTable<K, V> table(final String topic,
                                     final ConsumedInternal<K, V> consumed,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        final String source = newProcessorName(KStreamImpl.SOURCE_NAME);
        final String name = newProcessorName(KTableImpl.SOURCE_NAME);

        ProcessDetails.Builder processDetailsBuilder = ProcessDetails.builder();

        final KTable<K, V> kTable = createKTable(consumed,
                                                 topic,
                                                 storeSupplier.name(),
                                                 true,
                                                 source,
                                                 name,
                                                 processDetailsBuilder);

        processDetailsBuilder.withSourceTopics(Collections.singleton(topic))
            .withStoreSupplier(storeSupplier)
            .withSourceName(source)
            .withConsumed(consumed);
        StreamsGraphNode graphNode = new StreamsGraphNode(name, KTABLE, false, processDetailsBuilder.build(), TOPOLOGY_ROOT);
        topologyGraph.addNode(graphNode);

        return kTable;
    }

    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(final String topic,
                                     final ConsumedInternal<K, V> consumed,
                                     final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = new KeyValueStoreMaterializer<>(materialized)
            .materialize();

        final String source = newProcessorName(KStreamImpl.SOURCE_NAME);
        final String name = newProcessorName(KTableImpl.SOURCE_NAME);

        ProcessDetails.Builder processDetailsBuilder = ProcessDetails.builder();

        final KTable<K, V> kTable = createKTable(consumed,
                                                 topic,
                                                 storeBuilder.name(),
                                                 materialized.isQueryable(),
                                                 source,
                                                 name,
                                                 processDetailsBuilder);

        processDetailsBuilder.withSourceTopics(Collections.singleton(topic))
            .withStoreBuilder(storeBuilder)
            .withSourceName(source)
            .withConsumed(consumed);
        StreamsGraphNode graphNode = new StreamsGraphNode(name, KTABLE, false, processDetailsBuilder.build(), TOPOLOGY_ROOT);
        topologyGraph.addNode(graphNode);

        return kTable;
    }


    private <K, V> KTable<K, V> createKTable(final ConsumedInternal<K, V> consumed,
                                             final String topic,
                                             final String storeName,
                                             final boolean isQueryable,
                                             final String source,
                                             final String name,
                                             final ProcessDetails.Builder processDetailsBuilder) {
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeName);

        processDetailsBuilder.withProcessorSupplier(processorSupplier);

        return new KTableImpl<>(this, name, processorSupplier,
                                consumed.keySerde(), consumed.valueSerde(), Collections.singleton(source), storeName, isQueryable);
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

        ProcessDetails processDetails = ProcessDetails.builder().withConsumed(consumed)
            .withStoreBuilder(storeBuilder)
            .withKTableSource(tableSource)
            .withConnectProcessorName(processorName)
            .withSourceTopics(Collections.singleton(topic)).build();

        StreamsGraphNode streamsGraphNode = new StreamsGraphNode(sourceName, GLOBAL_KTABLE, false, processDetails, TOPOLOGY_ROOT);

        topologyGraph.addNode(streamsGraphNode);

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

    public void addNode(StreamsGraphNode graphNode) {
        topologyGraph.addNode(graphNode);
    }

    public StreamsTopologyGraph getTopologyGraph() {
        return topologyGraph;
    }
}
