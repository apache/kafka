/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

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

    public KGroupedTableImpl(KStreamBuilder topology,
                             String name,
                             String sourceName,
                             Serde<K> keySerde,
                             Serde<V> valSerde) {
        super(topology, name, Collections.singleton(sourceName));
        this.keySerde = keySerde;
        this.valSerde = valSerde;
    }

    @Override
    public <T> KTable<K, T> aggregate(Initializer<T> initializer,
                                      Aggregator<K, V, T> adder,
                                      Aggregator<K, V, T> subtractor,
                                      Serde<T> aggValueSerde,
                                      String storeName) {

        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(storeName, "storeName can't be null");
        ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(storeName, initializer, adder, subtractor);
        return doAggregate(aggregateSupplier, aggValueSerde, AGGREGATE_NAME, storeName);
    }

    @Override
    public <T> KTable<K, T> aggregate(Initializer<T> initializer,
                            Aggregator<K, V, T> adder,
                            Aggregator<K, V, T> subtractor,
                            String storeName) {

        return aggregate(initializer, adder, subtractor, null, storeName);
    }

    private <T> KTable<K, T> doAggregate(ProcessorSupplier<K, Change<V>> aggregateSupplier,
                                         Serde<T> aggValueSerde,
                                         String functionName,
                                         String storeName) {
        String sinkName = topology.newName(KStreamImpl.SINK_NAME);
        String sourceName = topology.newName(KStreamImpl.SOURCE_NAME);
        String funcName = topology.newName(functionName);

        String topic = storeName + KStreamImpl.REPARTITION_TOPIC_SUFFIX;

        Serializer<K> keySerializer = keySerde == null ? null : keySerde.serializer();
        Deserializer<K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
        Serializer<V> valueSerializer = valSerde == null ? null : valSerde.serializer();
        Deserializer<V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

        ChangedSerializer<V> changedValueSerializer = new ChangedSerializer<>(valueSerializer);
        ChangedDeserializer<V> changedValueDeserializer = new ChangedDeserializer<>(valueDeserializer);

        StateStoreSupplier aggregateStore = Stores.create(storeName)
            .withKeys(keySerde)
            .withValues(aggValueSerde)
            .persistent()
            .enableCaching()
            .build();

        // send the aggregate key-value pairs to the intermediate topic for partitioning
        topology.addInternalTopic(topic);
        topology.addSink(sinkName, topic, keySerializer, changedValueSerializer, this.name);

        // read the intermediate topic
        topology.addSource(sourceName, keyDeserializer, changedValueDeserializer, topic);

        // aggregate the values with the aggregator and local store
        topology.addProcessor(funcName, aggregateSupplier, sourceName);
        topology.addStateStore(aggregateStore, funcName);

        // return the KTable representation with the intermediate topic as the sources
        return new KTableImpl<>(topology, funcName, aggregateSupplier, Collections.singleton(sourceName), storeName);
    }

    @Override
    public KTable<K, V> reduce(Reducer<V> adder,
                               Reducer<V> subtractor,
                               String storeName) {
        Objects.requireNonNull(adder, "adder can't be null");
        Objects.requireNonNull(subtractor, "subtractor can't be null");
        Objects.requireNonNull(storeName, "storeName can't be null");
        ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableReduce<>(storeName, adder, subtractor);
        return doAggregate(aggregateSupplier, valSerde, REDUCE_NAME, storeName);
    }

    @Override
    public KTable<K, Long> count(String storeName) {
        return this.aggregate(
                new Initializer<Long>() {
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<K, V, Long>() {
                    @Override
                    public Long apply(K aggKey, V value, Long aggregate) {
                        return aggregate + 1L;
                    }
                }, new Aggregator<K, V, Long>() {
                    @Override
                    public Long apply(K aggKey, V value, Long aggregate) {
                        return aggregate - 1L;
                    }
                },
                Serdes.Long(), storeName);
    }

}