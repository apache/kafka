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

import org.apache.kafka.streams.kstream.AggregatorSupplier;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * NOTE: This is just a demo aggregate supplier that can be implemented by users to add their own built-in aggregates.
 * It is highly in-efficient and is not supposed to be merged in.
 */
public class TopKSupplier<K, V extends Comparable<V>> implements AggregatorSupplier<K, V, Collection<V>> {

    private final int k;

    public TopKSupplier(int k) {
        this.k = k;
    }

    private class TopK implements Aggregator<K, V, Collection<V>> {

        private final Map<K, PriorityQueue<V>> sorted = new HashMap<>();

        @Override
        public Collection<V> initialValue() {
            return Collections.<V>emptySet();
        }

        @Override
        public Collection<V> add(K aggKey, V value, Collection<V> aggregate) {
            PriorityQueue<V> queue = sorted.get(aggKey);
            if (queue == null) {
                queue = new PriorityQueue<>();
                sorted.put(aggKey, queue);
            }

            queue.add(value);

            PriorityQueue<V> copy = new PriorityQueue<>(queue);

            Set<V> ret = new HashSet<>();
            for (int i = 1; i <= k; i++)
                ret.add(copy.poll());

            return ret;
        }

        @Override
        public Collection<V> remove(K aggKey, V value, Collection<V> aggregate) {
            PriorityQueue<V> queue = sorted.get(aggKey);

            if (queue == null)
                throw new IllegalStateException("This should not happen.");

            queue.remove(value);

            PriorityQueue<V> copy = new PriorityQueue<>(queue);

            Set<V> ret = new HashSet<>();
            for (int i = 1; i <= k; i++)
                ret.add(copy.poll());

            return ret;
        }

        @Override
        public Collection<V> merge(Collection<V> aggr1, Collection<V> aggr2) {
            PriorityQueue<V> copy = new PriorityQueue<>(aggr1);
            copy.addAll(aggr2);

            Set<V> ret = new HashSet<>();
            for (int i = 1; i <= k; i++)
                ret.add(copy.poll());

            return ret;
        }
    }

    @Override
    public Aggregator<K, V, Collection<V>> get() {
        return new TopK();
    }
}