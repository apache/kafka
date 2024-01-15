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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.internals.graph.GroupedTableOperationRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;

/**
 * The implementation class of {@link KGroupedTable}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class KGroupedTableImpl<K, V> extends AbstractStream<K, V> implements KGroupedTable<K, V> {

    private static final String AGGREGATE_NAME = "KTABLE-AGGREGATE-";

    private static final String REDUCE_NAME = "KTABLE-REDUCE-";

    private final String userProvidedRepartitionTopicName;

    private final Initializer<Long> countInitializer = () -> 0L;

    private final Aggregator<K, V, Long> countAdder = (aggKey, value, aggregate) -> aggregate + 1L;

    private final Aggregator<K, V, Long> countSubtractor = (aggKey, value, aggregate) -> aggregate - 1L;

    private GraphNode repartitionGraphNode;

    KGroupedTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final Set<String> subTopologySourceNodes,
                      final GroupedInternal<K, V> groupedInternal,
                      final GraphNode graphNode) {
        super(name, groupedInternal.keySerde(), groupedInternal.valueSerde(), subTopologySourceNodes, graphNode, builder);

        this.userProvidedRepartitionTopicName = groupedInternal.name();
    }

    private <VAgg> KTable<K, VAgg> doAggregate(final ProcessorSupplier<K, Change<V>, K, Change<VAgg>> aggregateSupplier,
                                         final NamedInternal named,
                                         final String functionName,
                                         final MaterializedInternal<K, VAgg, KeyValueStore<Bytes, byte[]>> materialized) {

        final String sinkName = named.suffixWithOrElseGet("-sink", builder, KStreamImpl.SINK_NAME);
        final String sourceName = named.suffixWithOrElseGet("-source", builder, KStreamImpl.SOURCE_NAME);
        final String funcName = named.orElseGenerateWithPrefix(builder, functionName);
        final String repartitionTopic = (userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : materialized.storeName())
            + KStreamImpl.REPARTITION_TOPIC_SUFFIX;

        if (repartitionGraphNode == null || userProvidedRepartitionTopicName == null) {
            repartitionGraphNode = createRepartitionNode(sinkName, sourceName, repartitionTopic);
        }


        // the passed in StreamsGraphNode must be the parent of the repartition node
        builder.addGraphNode(this.graphNode, repartitionGraphNode);

        final StatefulProcessorNode statefulProcessorNode = new StatefulProcessorNode<>(
            funcName,
            new ProcessorParameters<>(aggregateSupplier, funcName),
            new KeyValueStoreMaterializer<>(materialized)
        );
        statefulProcessorNode.setOutputVersioned(materialized.storeSupplier() instanceof VersionedBytesStoreSupplier);

        // now the repartition node must be the parent of the StateProcessorNode
        builder.addGraphNode(repartitionGraphNode, statefulProcessorNode);

        // return the KTable representation with the intermediate topic as the sources
        return new KTableImpl<>(funcName,
                                materialized.keySerde(),
                                materialized.valueSerde(),
                                Collections.singleton(sourceName),
                                materialized.queryableStoreName(),
                                aggregateSupplier,
                                statefulProcessorNode,
                                builder);
    }

    private GroupedTableOperationRepartitionNode<K, V> createRepartitionNode(final String sinkName,
                                                                             final String sourceName,
                                                                             final String topic) {

        return GroupedTableOperationRepartitionNode.<K, V>groupedTableOperationNodeBuilder()
            .withRepartitionTopic(topic)
            .withSinkName(sinkName)
            .withSourceName(sourceName)
            .withKeySerde(keySerde)
            .withValueSerde(valueSerde)
            .withNodeName(sourceName).build();
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> adder,
                               final Reducer<V> subtractor,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return reduce(adder, subtractor, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> adder,
                               final Reducer<V> subtractor,
                               final Named named,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(valueSerde);
        }
        final ProcessorSupplier<K, Change<V>, K, Change<V>> aggregateSupplier = new KTableReduce<>(
            materializedInternal.storeName(),
            adder,
            subtractor);
        return doAggregate(aggregateSupplier, new NamedInternal(named), REDUCE_NAME, materializedInternal);
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> adder,
                               final Reducer<V> subtractor) {
        return reduce(adder, subtractor, Materialized.with(keySerde, valueSerde));
    }


    @Override
    public KTable<K, Long> count(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return count(NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, Long> count(final Named named, final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(Serdes.Long());
        }

        final ProcessorSupplier<K, Change<V>, K, Change<Long>> aggregateSupplier = new KTableAggregate<>(
            materializedInternal.storeName(),
            countInitializer,
            countAdder,
            countSubtractor);

        return doAggregate(aggregateSupplier, new NamedInternal(named), AGGREGATE_NAME, materializedInternal);
    }

    @Override
    public KTable<K, Long> count() {
        return count(Materialized.with(keySerde, Serdes.Long()));
    }

    @Override
    public KTable<K, Long> count(final Named named) {
        return count(named, Materialized.with(keySerde, Serdes.Long()));
    }

    @Override
    public <VAgg> KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                                        final Aggregator<? super K, ? super V, VAgg> adder,
                                        final Aggregator<? super K, ? super V, VAgg> subtractor,
                                        final Materialized<K, VAgg, KeyValueStore<Bytes, byte[]>> materialized) {
        return aggregate(initializer, adder, subtractor, NamedInternal.empty(), materialized);
    }

    @Override
    public <VAgg> KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                                        final Aggregator<? super K, ? super V, VAgg> adder,
                                        final Aggregator<? super K, ? super V, VAgg> subtractor,
                                        final Named named,
                                        final Materialized<K, VAgg, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VAgg, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        final ProcessorSupplier<K, Change<V>, K, Change<VAgg>> aggregateSupplier = new KTableAggregate<>(
            materializedInternal.storeName(),
            initializer,
            adder,
            subtractor);
        return doAggregate(aggregateSupplier, new NamedInternal(named), AGGREGATE_NAME, materializedInternal);
    }

    @Override
    public <VAgg> KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                                      final Aggregator<? super K, ? super V, VAgg> adder,
                                      final Aggregator<? super K, ? super V, VAgg> subtractor,
                                      final Named named) {
        return aggregate(initializer, adder, subtractor, named, Materialized.with(keySerde, null));
    }

    @Override
    public <VAgg> KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                                      final Aggregator<? super K, ? super V, VAgg> adder,
                                      final Aggregator<? super K, ? super V, VAgg> subtractor) {
        return aggregate(initializer, adder, subtractor, Materialized.with(keySerde, null));
    }

}
