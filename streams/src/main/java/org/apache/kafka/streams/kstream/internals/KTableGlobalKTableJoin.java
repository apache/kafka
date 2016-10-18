/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableGlobalKTableJoin<K1, K2, R, V1, V2> implements KTableProcessorSupplier<K1, V1, R> {

    private final KTableValueGetterSupplier<K1, V1> valueGetterSupplier;
    private final KTableValueGetterSupplier<K2, V2> globalTableValueGetterSupplier;
    private final ValueJoiner<V1, V2, R> joiner;
    private final KeyValueMapper<K1, V1, K2> mapper;
    private boolean sendOldValues;

    KTableGlobalKTableJoin(final KTableValueGetterSupplier<K1, V1> tableValueGetterSupplier,
                           final KTableValueGetterSupplier<K2, V2> globalTableValueGetterSupplier,
                           final ValueJoiner<V1, V2, R> joiner,
                           final KeyValueMapper<K1, V1, K2> mapper) {
        this.valueGetterSupplier = tableValueGetterSupplier;
        this.globalTableValueGetterSupplier = globalTableValueGetterSupplier;
        this.joiner = joiner;
        this.mapper = mapper;
    }

    @Override
    public Processor<K1, Change<V1>> get() {
        return new TheJoinProcessor(globalTableValueGetterSupplier.get());
    }

    @Override
    public KTableValueGetterSupplier<K1, R> view() {
        return new TheValueGetterSupplier();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class TheValueGetterSupplier implements KTableValueGetterSupplier<K1, R> {

        @Override
        public KTableValueGetter<K1, R> get() {
            return new KTableKTableJoinValueGetter<>(valueGetterSupplier.get(),
                                                     globalTableValueGetterSupplier.get(),
                                                     joiner,
                                                     mapper);
        }

        @Override
        public String[] storeNames() {
            return valueGetterSupplier.storeNames(); // shouldn't need global?
        }
    }


    private class TheJoinProcessor extends AbstractProcessor<K1, Change<V1>> {

        private final KTableValueGetter<K2, V2> valueGetter;

        TheJoinProcessor(final KTableValueGetter<K2, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final K1 key, final Change<V1> change) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable join operator should not be null.");

            final V2 v2 = change.newValue == null ? null : valueGetter.get(mapper.apply(key, change.newValue));
            if (v2 != null) {
                R oldValue = null;
                if (sendOldValues && change.oldValue != null) {
                    oldValue = joiner.apply(change.oldValue, valueGetter.get(mapper.apply(key, change.oldValue)));
                }
                context().forward(key, new Change<>(joiner.apply(change.newValue, v2), oldValue));
            } else if (sendOldValues && change.oldValue != null) {
                final V2 value2 = valueGetter.get(mapper.apply(key, change.oldValue));
                if (value2 == null) {
                    return;
                }
                final R oldValue = joiner.apply(change.oldValue, value2);
                context().forward(key, new Change<>(null, oldValue));
            }
        }
    }


}
