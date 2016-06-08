/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.AbstractStream;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KStreamReduce;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KGroupedStreamImpl<K, V> extends AbstractStream<K> implements KGroupedStream<K, V> {

    private static final String REDUCE_NAME = "KSTREAM-REDUCE-";

    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public KGroupedStreamImpl(final KStreamBuilder topology,
                              final String name,
                              final Set<String> sourceNodes,
                              final Serde<K> keySerde,
                              final Serde<V> valSerde) {
        super(topology, name, sourceNodes);
        this.keySerde = keySerde;
        this.valSerde = valSerde;
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer,
                               final String name) {
        return doAggregate(
            new KStreamReduce<K,V>(name, reducer),
            valSerde,
            REDUCE_NAME,
            name);
    }

    @Override
    public <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                                      final Aggregator<K, V, T> aggregator,
                                      final Serde<T> aggValueSerde,
                                      final String name) {
        return null;
    }

    @Override
    public KTable<K, Long> count(final String name) {
        return null;
    }

    private <T> KTable<K, T> doAggregate(final ProcessorSupplier<K, T> aggregateSupplier,
                                         final Serde<T> aggValueSerde,
                                         final String functionName,
                                         final String name) {

        final String sinkName = topology.newName(KStreamImpl.SINK_NAME);
        final String sourceName = topology.newName(KStreamImpl.SOURCE_NAME);
        final String aggFunctionName = topology.newName(functionName);
        final String topic = name + KStreamImpl.REPARTITION_TOPIC_SUFFIX;

        final Serializer<K> keySerializer = keySerde == null ? null : keySerde.serializer();
        final Deserializer<K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
        final Serializer<V> valueSerializer = valSerde == null ? null : valSerde.serializer();
        final Deserializer<V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

        final StateStoreSupplier store = Stores.create(name)
            .withKeys(keySerde)
            .withValues(aggValueSerde)
            .persistent()
            .build();

        // create internal topic with new source and sink. Co-partition sources
        topology.addInternalTopic(topic);
        topology.addSink(sinkName, topic, keySerializer, valueSerializer, this.name);
        topology.addSource(sourceName, keyDeserializer, valueDeserializer, topic);

        final Set<String> allSourceNodes = new HashSet<>(sourceNodes);
        allSourceNodes.add(sourceName);
        topology.copartitionSources(allSourceNodes);

        topology.addProcessor(aggFunctionName, aggregateSupplier, sourceName);
        topology.addStateStore(store, aggFunctionName);

        return new KTableImpl<>(topology, aggFunctionName, aggregateSupplier, Collections
            .singleton(sourceName));
    }
}
