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

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;


class KTableMapValues<KIn, VIn, VOut> implements KTableProcessorSupplier<KIn, VIn, KIn, VOut> {
    private final KTableImpl<KIn, ?, VIn> parent;
    private final ValueMapperWithKey<? super KIn, ? super VIn, ? extends VOut> mapper;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableMapValues(final KTableImpl<KIn, ?, VIn> parent,
                    final ValueMapperWithKey<? super KIn, ? super VIn, ? extends VOut> mapper,
                    final String queryableName) {
        this.parent = parent;
        this.mapper = mapper;
        this.queryableName = queryableName;
    }

    @Override
    public Processor<KIn, Change<VIn>, KIn, Change<VOut>> get() {
        return new KTableMapValuesProcessor();
    }

    @Override
    public KTableValueGetterSupplier<KIn, VOut> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply map-values on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<KIn, VOut>() {
                final KTableValueGetterSupplier<KIn, VIn> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<KIn, VOut> get() {
                    return new KTableMapValuesValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public String[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        if (queryableName != null) {
            sendOldValues = true;
            return true;
        }

        if (parent.enableSendingOldValues(forceMaterialization)) {
            sendOldValues = true;
        }

        return sendOldValues;
    }

    private VOut computeValue(final KIn key, final VIn value) {
        VOut newValue = null;

        if (value != null) {
            newValue = mapper.apply(key, value);
        }

        return newValue;
    }

    private ValueAndTimestamp<VOut> computeValueAndTimestamp(final KIn key, final ValueAndTimestamp<VIn> valueAndTimestamp) {
        VOut newValue = null;
        long timestamp = 0;

        if (valueAndTimestamp != null) {
            newValue = mapper.apply(key, valueAndTimestamp.value());
            timestamp = valueAndTimestamp.timestamp();
        }

        return ValueAndTimestamp.make(newValue, timestamp);
    }


    private class KTableMapValuesProcessor implements Processor<KIn, Change<VIn>, KIn, Change<VOut>> {
        private ProcessorContext<KIn, Change<VOut>> context;
        private TimestampedKeyValueStore<KIn, VOut> store;
        private TimestampedTupleForwarder<KIn, VOut> tupleForwarder;

        @Override
        public void init(final ProcessorContext<KIn, Change<VOut>> context) {
            this.context = context;
            if (queryableName != null) {
                store = context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<KIn, Change<VIn>> record) {
            final VOut newValue = computeValue(record.key(), record.value().newValue);
            final VOut oldValue = computeOldValue(record.key(), record.value());

            if (queryableName != null) {
                store.put(record.key(), ValueAndTimestamp.make(newValue, record.timestamp()));
                tupleForwarder.maybeForward(record.withValue(new Change<>(newValue, oldValue)));
            } else {
                context.forward(record.withValue(new Change<>(newValue, oldValue)));
            }
        }

        private VOut computeOldValue(final KIn key, final Change<VIn> change) {
            if (!sendOldValues) {
                return null;
            }

            return queryableName != null
                ? getValueOrNull(store.get(key))
                : computeValue(key, change.oldValue);
        }
    }


    private class KTableMapValuesValueGetter implements KTableValueGetter<KIn, VOut> {
        private final KTableValueGetter<KIn, VIn> parentGetter;

        KTableMapValuesValueGetter(final KTableValueGetter<KIn, VIn> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<VOut> get(final KIn key) {
            return computeValueAndTimestamp(key, parentGetter.get(key));
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }
}
