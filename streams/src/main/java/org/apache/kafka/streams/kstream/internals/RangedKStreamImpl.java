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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Range;
import org.apache.kafka.streams.kstream.RangeAggregator;
import org.apache.kafka.streams.kstream.RangedKStream;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.processor.internals.StoreFactory;

import java.util.Objects;
import java.util.Set;

class RangedKStreamImpl<K, V> extends AbstractStream<K, V> implements RangedKStream<K, V> {

    static final String RANGE_AGGREGATE_NAME = "KSTREAM-RANGE-AGGREGATE-";

    private final Range<? super K, ? super V> range;
    private final StoreFactory storeFactory;

    RangedKStreamImpl(
        final Range<? super K, ? super V> range,
        final StoreFactory storeFactory,
        final String name,
        final InternalStreamsBuilder builder,
        final Set<String> subTopologySourceNodes,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final GraphNode graphNode
    ) {
        super(name, keySerde, valueSerde, subTopologySourceNodes, graphNode, builder);
        this.range = range;
        this.storeFactory = storeFactory;
    }

    @Override
    public <VR> KStream<K, VR> aggregate(final RangeAggregator<K, V, VR> aggregator) {
        Objects.requireNonNull(aggregator, "aggregator can't be null");

        final String aggName = builder.newProcessorName(RANGE_AGGREGATE_NAME);

        final KStreamRangeAggregate<K, V, VR> aggSupplier = new KStreamRangeAggregate<>(storeFactory, range, aggregator);

        final ProcessorGraphNode<K, V> aggNode = new ProcessorGraphNode<>(
            aggName,
            new ProcessorParameters<>(aggSupplier, aggName)
        );

        builder.addGraphNode(graphNode, aggNode);

        return new KStreamImpl<>(aggName, null, null, subTopologySourceNodes, false, aggNode, builder);
    }

    @Override
    public KStream<K, Long> count() {
        return aggregate((anchor, rangeRecords) -> {
            long count = 0L;
            final java.util.Iterator<?> it = rangeRecords.iterator();
            while (it.hasNext()) {
                it.next();
                count++;
            }
            return count;
        });
    }

}
