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

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

public class KTablePassThrough<KIn, VIn> implements KTableProcessorSupplier<KIn, VIn, KIn, VIn> {
    private final Collection<KStreamAggProcessorSupplier> parents;
    private final String storeName;


    KTablePassThrough(final Collection<KStreamAggProcessorSupplier> parents, final String storeName) {
        this.parents = parents;
        this.storeName = storeName;
    }

    @Override
    public Processor<KIn, Change<VIn>, KIn, Change<VIn>> get() {
        return new KTablePassThroughProcessor();
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        // Aggregation requires materialization so we will always enable sending old values
        for (final KStreamAggProcessorSupplier parent : parents) {
            parent.enableSendingOldValues();
        }
        return true;
    }

    @Override
    public KTableValueGetterSupplier<KIn, VIn> view() {

        return new KTableValueGetterSupplier<KIn, VIn>() {

            public KTableValueGetter<KIn, VIn> get() {
                return new KTablePassThroughValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KTablePassThroughProcessor implements Processor<KIn, Change<VIn>, KIn, Change<VIn>> {
        private ProcessorContext<KIn, Change<VIn>> context;

        @Override
        public void init(final ProcessorContext<KIn, Change<VIn>> context) {
            this.context = context;
        }

        @Override
        public void process(final Record<KIn, Change<VIn>> record) {
            context.forward(record);
        }
    }

    private class KTablePassThroughValueGetter implements KTableValueGetter<KIn, VIn> {
        private KeyValueStoreWrapper<KIn, VIn> store;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            store = new KeyValueStoreWrapper<>(context, storeName);
        }

        @Override
        public ValueAndTimestamp<VIn> get(final KIn key) {
            return store.get(key);
        }

        @Override
        public ValueAndTimestamp<VIn> get(final KIn key, final long asOfTimestamp) {
            return store.get(key, asOfTimestamp);
        }

        @Override
        public boolean isVersioned() {
            return store.isVersionedStore();
        }
    }
}
