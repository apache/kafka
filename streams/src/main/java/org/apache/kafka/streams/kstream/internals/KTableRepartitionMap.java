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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

/**
 * KTable repartition map functions are not exposed to public APIs, but only used for keyed aggregations.
 * <p>
 * Given the input, it can output at most two records (one mapped from old value and one mapped from new value).
 */
public class KTableRepartitionMap<K, V, K1, V1> implements KTableProcessorSupplier<K, V, KeyValue<K1, V1>> {

    private final KTableImpl<K, ?, V> parent;
    private final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> mapper;

    KTableRepartitionMap(final KTableImpl<K, ?, V> parent, final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> mapper) {
        this.parent = parent;
        this.mapper = mapper;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableMapProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, KeyValue<K1, V1>> view() {
        final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

        return new KTableValueGetterSupplier<K, KeyValue<K1, V1>>() {

            public KTableValueGetter<K, KeyValue<K1, V1>> get() {
                return new KTableMapValueGetter(parentValueGetterSupplier.get());
            }

            @Override
            public String[] storeNames() {
                throw new StreamsException("Underlying state store not accessible due to repartitioning.");
            }
        };
    }

    /**
     * @throws IllegalStateException since this method should never be called
     */
    @Override
    public void enableSendingOldValues() {
        // this should never be called
        throw new IllegalStateException("KTableRepartitionMap should always require sending old values.");
    }

    private class KTableMapProcessor extends AbstractProcessor<K, Change<V>> {

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final K key, final Change<V> change) {
            // the original key should never be null
            if (key == null) {
                throw new StreamsException("Record key for the grouping KTable should not be null.");
            }

            // if the value is null, we do not need to forward its selected key-value further
            final KeyValue<? extends K1, ? extends V1> newPair = change.newValue == null ? null : mapper.apply(key, change.newValue);
            final KeyValue<? extends K1, ? extends V1> oldPair = change.oldValue == null ? null : mapper.apply(key, change.oldValue);

            // if the selected repartition key or value is null, skip
            // forward oldPair first, to be consistent with reduce and aggregate
            if (oldPair != null && oldPair.key != null && oldPair.value != null) {
                context().forward(oldPair.key, new Change<>(null, oldPair.value));
            }

            if (newPair != null && newPair.key != null && newPair.value != null) {
                context().forward(newPair.key, new Change<>(newPair.value, null));
            }

        }
    }

    private class KTableMapValueGetter implements KTableValueGetter<K, KeyValue<K1, V1>> {

        private final KTableValueGetter<K, V> parentGetter;
        private ProcessorContext context;

        KTableMapValueGetter(final KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<KeyValue<K1, V1>> get(final K key) {
            final ValueAndTimestamp<V> valueAndTimestamp = parentGetter.get(key);
            return ValueAndTimestamp.make(
                mapper.apply(key, getValueOrNull(valueAndTimestamp)),
                valueAndTimestamp == null ? context.timestamp() : valueAndTimestamp.timestamp());
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }

}
