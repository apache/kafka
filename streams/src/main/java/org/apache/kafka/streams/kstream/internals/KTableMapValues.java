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

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;


class KTableMapValues<K, V, V1> implements KTableProcessorSupplier<K, V, V1> {

    private final KTableImpl<K, ?, V> parent;
    private final ValueMapper<? super V, ? extends V1> mapper;

    private boolean sendOldValues = false;

    public KTableMapValues(KTableImpl<K, ?, V> parent, ValueMapper<? super V, ? extends V1> mapper) {
        this.parent = parent;
        this.mapper = mapper;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableMapValuesProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V1> view() {
        final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

        return new KTableValueGetterSupplier<K, V1>() {

            public KTableValueGetter<K, V1> get() {
                return new KTableMapValuesValueGetter(parentValueGetterSupplier.get());
            }

            @Override
            public String[] storeNames() {
                return parentValueGetterSupplier.storeNames();
            }
        };
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V1 computeValue(V value) {
        V1 newValue = null;

        if (value != null)
            newValue = mapper.apply(value);

        return newValue;
    }

    private class KTableMapValuesProcessor extends AbstractProcessor<K, Change<V>> {

        @Override
        public void process(K key, Change<V> change) {
            V1 newValue = computeValue(change.newValue);
            V1 oldValue = sendOldValues ? computeValue(change.oldValue) : null;

            context().forward(key, new Change<>(newValue, oldValue));
        }
    }

    private class KTableMapValuesValueGetter implements KTableValueGetter<K, V1> {

        private final KTableValueGetter<K, V> parentGetter;

        public KTableMapValuesValueGetter(KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public V1 get(K key) {
            return computeValue(parentGetter.get(key));
        }

    }

}
