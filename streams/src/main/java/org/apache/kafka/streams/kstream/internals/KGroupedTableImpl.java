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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.internals.graph.GroupedTableOperationRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Objects;

/**
 * The implementation class of {@link KGroupedTable}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class KGroupedTableImpl<K, V> extends AbstractStream<K> implements KGroupedTable<K, V> {

    private static final String AGGREGATE_NAME = "KTABLE-AGGREGATE-";

    private static final String REDUCE_NAME = "KTABLE-REDUCE-";

    protected final Serde<K> keySerde;
    protected final Serde<V> valSerde;
    private final Initializer<Long> countInitializer = new Initializer<Long>() {
        @Override
        public Long apply() {
            return 0L;
        }
    };

    private final Aggregator<K, V, Long> countAdder = new Aggregator<K, V, Long>() {
        @Override
        public Long apply(K aggKey, V value, Long aggregate) {
            return aggregate + 1L;
        }
    };

    private Aggregator<K, V, Long> countSubtractor = new Aggregator<K, V, Long>() {
        @Override
        public Long apply(K aggKey, V value, Long aggregate) {
            return aggregate - 1L;
        }
    };

    KGroupedTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final String sourceName,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final StreamsGraphNode streamsGraphNode) {
        super(builder, name, Collections.singleton(sourceName), streamsGraphNode);
        this.keySerde = keySerde;
        this.valSerde = valSerde;
    }

    private <T> void buildAggregate(final ProcessorSupplier<K, Change<V>> aggregateSupplier,
                                    final String topic,
                                    final String funcName,
                                    final String sourceName,
                                    final String sinkName) {

        final Serializer<? extends K> keySerializer = keySerde == null ? null : keySerde.serializer();
        final Deserializer<? extends K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
        final Serializer<? extends V> valueSerializer = valSerde == null ? null : valSerde.serializer();
        final Deserializer<? extends V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

        final ChangedSerializer<? extends V> changedValueSerializer = new ChangedSerializer<>(valueSerializer);
        final ChangedDeserializer<? extends V> changedValueDeserializer = new ChangedDeserializer<>(valueDeserializer);

        // send the aggregate key-value pairs to the intermediate topic for partitioning
        builder.internalTopologyBuilder.addInternalTopic(topic);
        builder.internalTopologyBuilder.addSink(sinkName, topic, keySerializer, changedValueSerializer, null, this.name);

        // read the intermediate topic with RecordMetadataTimestampExtractor
        builder.internalTopologyBuilder.addSource(null, sourceName, new FailOnInvalidTimestamp(), keyDeserializer, changedValueDeserializer, topic);

        // aggregate the values with the aggregator and local store
        builder.internalTopologyBuilder.addProcessor(funcName, aggregateSupplier, sourceName);
    }

    private <T> KTable<K, T> doAggregate(final ProcessorSupplier<K, Change<V>> aggregateSupplier,
                                         final String functionName,
                                         final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
        final String sinkName = builder.newProcessorName(KStreamImpl.SINK_NAME);
        final String sourceName = builder.newProcessorName(KStreamImpl.SOURCE_NAME);
        final String funcName = builder.newProcessorName(functionName);
        final String topic = materialized.storeName() + KStreamImpl.REPARTITION_TOPIC_SUFFIX;


        buildAggregate(aggregateSupplier,
                       topic,
                       funcName,
                       sourceName,
                       sinkName
        );

        builder.internalTopologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(materialized)
                                                          .materialize(), funcName);


        StreamsGraphNode repartitionNode = createRepartitionNode(sinkName,
                                                                 sourceName,
                                                                 topic);
        addGraphNode(repartitionNode);

        StatefulProcessorNode statefulProcessorNode = createStatefulProcessorNode(materialized,
                                                                                  funcName,
                                                                                  aggregateSupplier);

        repartitionNode.addChildNode(statefulProcessorNode);


        // return the KTable representation with the intermediate topic as the sources
        return new KTableImpl<>(builder,
                                funcName,
                                aggregateSupplier,
                                Collections.singleton(sourceName),
                                materialized.storeName(),
                                materialized.isQueryable(),
                                statefulProcessorNode);
    }

    @SuppressWarnings("unchecked")
    private <T> StatefulProcessorNode createStatefulProcessorNode(final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized,
                                                                  final String functionName,
                                                                  final ProcessorSupplier aggregateSupplier) {

        ProcessorParameters aggregateFunctionProcessorParams = new ProcessorParameters<>(aggregateSupplier, functionName);

        return StatefulProcessorNode.statefulProcessorNodeBuilder()
            .withNodeName(functionName)
            .withProcessorParameters(aggregateFunctionProcessorParams)
            .withStoreBuilder(new KeyValueStoreMaterializer(materialized).materialize()).build();
    }

    @SuppressWarnings("unchecked")
    private GroupedTableOperationRepartitionNode createRepartitionNode(final String sinkName,
                                                                       final String sourceName,
                                                                       final String topic) {

        return GroupedTableOperationRepartitionNode.groupedTableOperationNodeBuilder()
            .withRepartitionTopic(topic)
            .withSinkName(sinkName)
            .withSourceName(sourceName)
            .withKeySerde(keySerde)
            .withValueSerde(valSerde)
            .withNodeName(sourceName).build();
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> adder,
                               final Reducer<V> subtractor,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(valSerde);
        }
        final ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableReduce<>(materializedInternal.storeName(),
                                                                                     adder,
                                                                                     subtractor);
        return doAggregate(aggregateSupplier, REDUCE_NAME, materializedInternal);
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> adder,
                               final Reducer<V> subtractor) {
        return reduce(adder, subtractor, Materialized.<K, V, KeyValueStore<Bytes, byte[]>>with(keySerde, valSerde));
    }

    @Override
    public KTable<K, Long> count(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(Serdes.Long());
        }

        final ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
                countInitializer,
                countAdder,
                countSubtractor);

        return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
    }

    @Override
    public KTable<K, Long> count() {
        return count(Materialized.<K, Long, KeyValueStore<Bytes, byte[]>>with(keySerde, Serdes.Long()));
    }

    @Override
    public <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                        final Aggregator<? super K, ? super V, VR> adder,
                                        final Aggregator<? super K, ? super V, VR> subtractor,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        final ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
                                                                                        initializer,
                                                                                        adder,
                                                                                        subtractor);
        return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
    }

    @Override
    public <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                                      final Aggregator<? super K, ? super V, T> adder,
                                      final Aggregator<? super K, ? super V, T> subtractor) {
        return aggregate(initializer, adder, subtractor, Materialized.<K, T, KeyValueStore<Bytes, byte[]>>with(keySerde, null));
    }

}
