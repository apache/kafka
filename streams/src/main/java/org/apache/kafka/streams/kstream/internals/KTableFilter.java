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

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import java.util.Collections;
import java.util.Set;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

public class KTableFilter<KIn, VIn> implements KTableProcessorSupplier<KIn, VIn, KIn, VIn> {
    private final KTableImpl<KIn, ?, VIn> parent;
    private final Predicate<? super KIn, ? super VIn> predicate;
    private final boolean filterNot;
    private final String queryableName;
    private boolean sendOldValues;
    private boolean useVersionedSemantics = false;
    private final StoreFactory storeFactory;

    KTableFilter(final KTableImpl<KIn, ?, VIn> parent,
                 final Predicate<? super KIn, ? super VIn> predicate,
                 final boolean filterNot,
                 final String queryableName,
                 final StoreFactory storeFactory) {
        this.parent = parent;
        this.predicate = predicate;
        this.filterNot = filterNot;
        this.queryableName = queryableName;
        // If upstream is already materialized, enable sending old values to avoid sending unnecessary tombstones:
        this.sendOldValues = parent.enableSendingOldValues(false);
        this.storeFactory = storeFactory;
    }

    public void setUseVersionedSemantics(final boolean useVersionedSemantics) {
        this.useVersionedSemantics = useVersionedSemantics;
    }

    // VisibleForTesting
    boolean isUseVersionedSemantics() {
        return useVersionedSemantics;
    }

    @Override
    public Processor<KIn, Change<VIn>, KIn, Change<VIn>> get() {
        return new KTableFilterProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        if (storeFactory == null) {
            return null;
        }
        return Collections.singleton(new StoreFactory.FactoryWrappingStoreBuilder<>(storeFactory));
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

    private VIn computeValue(final KIn key, final VIn value) {
        VIn newValue = null;

        if (value != null && (filterNot ^ predicate.test(key, value))) {
            newValue = value;
        }

        return newValue;
    }

    private ValueAndTimestamp<VIn> computeValue(final KIn key, final ValueAndTimestamp<VIn> valueAndTimestamp) {
        ValueAndTimestamp<VIn> newValueAndTimestamp = null;

        if (valueAndTimestamp != null) {
            final VIn value = valueAndTimestamp.value();
            if (filterNot ^ predicate.test(key, value)) {
                newValueAndTimestamp = valueAndTimestamp;
            }
        }

        return newValueAndTimestamp;
    }


    private class KTableFilterProcessor implements Processor<KIn, Change<VIn>, KIn, Change<VIn>> {
        private ProcessorContext<KIn, Change<VIn>> context;
        private KeyValueStoreWrapper<KIn, VIn> store;
        private TimestampedTupleForwarder<KIn, VIn> tupleForwarder;

        @Override
        public void init(final ProcessorContext<KIn, Change<VIn>> context) {
            this.context = context;
            if (queryableName != null) {
                store = new KeyValueStoreWrapper<>(context, queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store.store(),
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<KIn, Change<VIn>> record) {
            final KIn key = record.key();
            final Change<VIn> change = record.value();

            final VIn newValue = computeValue(key, change.newValue);
            final VIn oldValue = computeOldValue(key, change);

            if (!useVersionedSemantics) {
                if (sendOldValues && oldValue == null && newValue == null) {
                    return; // unnecessary to forward here.
                }
            }

            if (queryableName != null) {
                final long putReturnCode = store.put(key, newValue, record.timestamp());
                // if not put to store, do not forward downstream either
                if (putReturnCode != PUT_RETURN_CODE_NOT_PUT) {
                    tupleForwarder.maybeForward(record.withValue(new Change<>(newValue, oldValue, putReturnCode == PUT_RETURN_CODE_IS_LATEST)));
                }
            } else {
                context.forward(record.withValue(new Change<>(newValue, oldValue, record.value().isLatest)));
            }
        }

        private VIn computeOldValue(final KIn key, final Change<VIn> change) {
            if (!sendOldValues) {
                return null;
            }

            return queryableName != null
                ? getValueOrNull(store.get(key))
                : computeValue(key, change.oldValue);
        }
    }

    @Override
    public KTableValueGetterSupplier<KIn, VIn> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply filter on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<KIn, VIn>() {
                final KTableValueGetterSupplier<KIn, VIn> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<KIn, VIn> get() {
                    return new KTableFilterValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public String[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }


    private class KTableFilterValueGetter implements KTableValueGetter<KIn, VIn> {
        private final KTableValueGetter<KIn, VIn> parentGetter;

        KTableFilterValueGetter(final KTableValueGetter<KIn, VIn> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            // This is the old processor context for compatibility with the other KTable processors.
            // Once we migrate them all, we can swap this out.
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<VIn> get(final KIn key) {
            return computeValue(key, parentGetter.get(key));
        }

        @Override
        public ValueAndTimestamp<VIn> get(final KIn key, final long asOfTimestamp) {
            return computeValue(key, parentGetter.get(key, asOfTimestamp));
        }

        @Override
        public boolean isVersioned() {
            return parentGetter.isVersioned();
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }

}
