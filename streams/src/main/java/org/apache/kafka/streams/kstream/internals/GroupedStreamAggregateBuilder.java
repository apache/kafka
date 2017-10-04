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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;
import java.util.Set;

class GroupedStreamAggregateBuilder<K, V> {
    private final InternalStreamsBuilder builder;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final boolean repartitionRequired;
    private final Set<String> sourceNodes;
    private final String name;

    final Initializer<Long> countInitializer = new Initializer<Long>() {
        @Override
        public Long apply() {
            return 0L;
        }
    };
    final Aggregator<K, V, Long> countAggregator = new Aggregator<K, V, Long>() {
        @Override
        public Long apply(K aggKey, V value, Long aggregate) {
            return aggregate + 1;
        }
    };

    GroupedStreamAggregateBuilder(final InternalStreamsBuilder builder,
                                  final Serde<K> keySerde,
                                  final Serde<V> valueSerde,
                                  final boolean repartitionRequired,
                                  final Set<String> sourceNodes,
                                  final String name) {

        this.builder = builder;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.repartitionRequired = repartitionRequired;
        this.sourceNodes = sourceNodes;
        this.name = name;
    }

    <T> KTable<K, T> build(final KStreamAggProcessorSupplier<K, ?, V, T> aggregateSupplier,
                           final String functionName,
                           final StoreBuilder storeBuilder,
                           final boolean isQueryable) {
        final String aggFunctionName = builder.newProcessorName(functionName);
        final String sourceName = repartitionIfRequired(storeBuilder.name());
        builder.internalTopologyBuilder.addProcessor(aggFunctionName, aggregateSupplier, sourceName);
        builder.internalTopologyBuilder.addStateStore(storeBuilder, aggFunctionName);

        return new KTableImpl<>(
                builder,
                aggFunctionName,
                aggregateSupplier,
                sourceName.equals(this.name) ? sourceNodes : Collections.singleton(sourceName),
                storeBuilder.name(),
                isQueryable);
    }

    /**
     * @return the new sourceName if repartitioned. Otherwise the name of this stream
     */
    private String repartitionIfRequired(final String queryableStoreName) {
        if (!repartitionRequired) {
            return this.name;
        }
        return KStreamImpl.createReparitionedSource(builder, keySerde, valueSerde, queryableStoreName, name);
    }
}
