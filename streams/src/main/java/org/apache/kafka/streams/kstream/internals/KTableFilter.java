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

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableFilter<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableImpl<K, ?, V> parent;
    private final Predicate<K, V> predicate;
    private final boolean filterOut;

    public KTableFilter(KTableImpl<K, ?, V> parent, Predicate<K, V> predicate, boolean filterOut) {
        this.parent = parent;
        this.predicate = predicate;
        this.filterOut = filterOut;
    }

    @Override
    public Processor<K, V> get() {
        return new KTableFilterProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTableFilterValueGetter(parentValueGetterSupplier.get());
            }

        };
    }

    private V computeNewValue(K key, V value) {
        V newValue = null;

        if (value != null && (filterOut ^ predicate.test(key, value)))
            newValue = value;

        return newValue;
    }

    private class KTableFilterProcessor extends AbstractProcessor<K, V> {

        @Override
        public void process(K key, V value) {
            context().forward(key, computeNewValue(key, value));
        }

    }

    private class KTableFilterValueGetter implements KTableValueGetter<K, V> {

        private final KTableValueGetter<K, V> parentGetter;

        public KTableFilterValueGetter(KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public V get(K key) {
            return computeNewValue(key, parentGetter.get(key));
        }

    }

}
