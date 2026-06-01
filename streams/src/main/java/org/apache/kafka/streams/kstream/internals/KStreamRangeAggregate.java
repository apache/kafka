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

import org.apache.kafka.streams.kstream.CloseableIterator;
import org.apache.kafka.streams.kstream.Range;
import org.apache.kafka.streams.kstream.RangeAggregator;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * Aggregate processor for {@link org.apache.kafka.streams.kstream.RangedKStream}.
 * For each incoming record, fetches the range from the buffer store, applies the aggregator,
 * and forwards the result downstream. Connects to the buffer store by name without owning it.
 */
public class KStreamRangeAggregate<K, V, VR> implements ProcessorSupplier<K, V, K, VR> {

    private final StoreFactory storeFactory;
    private final Range<K, V> range;
    private final RangeAggregator<K, V, VR> aggregator;

    @SuppressWarnings("unchecked")
    public KStreamRangeAggregate(
        final StoreFactory storeFactory,
        final Range<? super K, ? super V> range,
        final RangeAggregator<K, V, VR> aggregator
    ) {
        this.storeFactory = storeFactory;
        this.range = (Range<K, V>) range;
        this.aggregator = aggregator;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        // Return the same builder so the topology connects this processor to the existing store
        // InternalTopologyBuilder deduplicates: if the store already exists it only adds the connection
        return Collections.singleton(new FactoryWrappingStoreBuilder<>(storeFactory));
    }

    @Override
    public Processor<K, V, K, VR> get() {
        return new KStreamRangeAggregateProcessor(storeFactory.storeName());
    }

    private final class KStreamRangeAggregateProcessor extends ContextualProcessor<K, V, K, VR> {

        private final String storeName;
        private ReadOnlyWindowStore<K, V> store;

        KStreamRangeAggregateProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<K, VR> context) {
            super.init(context);
            store = context.getStateStore(storeName);
        }

        @Override
        public void process(final Record<K, V> record) {
            try (final CloseableIterator<Record<K, V>> it = range.fetch(record, store)) {
                final VR result = aggregator.apply(record, new SingleUseIterable<>(it));
                context().forward(record.withValue(result));
            }
        }
    }

    private static final class SingleUseIterable<T> implements Iterable<T> {
        private final Iterator<T> iterator;
        private boolean consumed = false;

        SingleUseIterable(final Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public Iterator<T> iterator() {
            if (consumed) {
                throw new IllegalStateException(
                    "rangeRecords is single-use: iterator() may only be called once per aggregation");
            }
            consumed = true;
            return iterator;
        }
    }
}
