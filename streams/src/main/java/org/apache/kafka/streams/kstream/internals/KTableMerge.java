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

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableMerge<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableImpl<K, ?, V>[] parents;

    public KTableMerge(KTableImpl<K, ?, V>... parents) {
        this.parents = parents.clone();
    }

    @Override
    public Processor<K, V> get() {
        return new KTableMergeProcessor<>();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        final KTableValueGetterSupplier<K, V>[] valueGetterSuppliers = new KTableValueGetterSupplier[parents.length];

        for (int i = 0; i < parents.length; i++) {
            valueGetterSuppliers[i] = parents[i].valueGetterSupplier();
        }
        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                KTableValueGetter<K, V>[] valueGetters = new KTableValueGetter[valueGetterSuppliers.length];

                for (int i = 0; i < valueGetters.length; i++) {
                    valueGetters[i] = valueGetterSuppliers[i].get();
                }
                return new KTableMergeValueGetter(valueGetters);
            }

        };
    }

    private class KTableMergeProcessor<K, V> extends AbstractProcessor<K, V> {
        @Override
        public void process(K key, V value) {
            context().forward(key, value);
        }
    }

    private class KTableMergeValueGetter implements KTableValueGetter<K, V> {

        private final KTableValueGetter<K, V>[] valueGetters;

        public KTableMergeValueGetter(KTableValueGetter<K, V>[] valueGetters) {
            this.valueGetters = valueGetters;
        }

        @Override
        public void init(ProcessorContext context) {
            for (KTableValueGetter<K, V> valueGetter : valueGetters) {
                valueGetter.init(context);
            }
        }

        @Override
        public V get(K key) {
            for (KTableValueGetter<K, V> valueGetter : valueGetters) {
                V value = valueGetter.get(key);
                if (value != null)
                    return value;
            }
            return null;
        }

    }

}
