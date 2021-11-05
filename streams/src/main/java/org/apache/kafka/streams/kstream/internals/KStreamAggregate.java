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

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamAggregate<KIn, VIn, VAgg> implements KStreamAggProcessorSupplier<KIn, VIn, KIn, VAgg> {

    private final String storeName;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;

    private boolean sendOldValues = false;

    KStreamAggregate(final String storeName,
                     final Initializer<VAgg> initializer,
                     final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    @Override
    public Processor<KIn, VIn, KIn, Change<VAgg>> get() {
        return new KStreamAggregateProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamAggregateProcessor extends ContextualProcessor<KIn, VIn, KIn, Change<VAgg>> {
        private TimestampedKeyValueStore<KIn, VAgg> store;
        private TimestampedTupleForwarder<KIn, VAgg> tupleForwarder;

        @Override
        public void init(final ProcessorContext<KIn, Change<VAgg>> context) {
            super.init(context);
            store =  context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            final ValueAndTimestamp<VAgg> oldAggAndTimestamp = store.get(record.key());
            VAgg oldAgg = getValueOrNull(oldAggAndTimestamp);

            final VAgg newAgg;
            final long newTimestamp;

            if (oldAgg == null) {
                oldAgg = initializer.apply();
                newTimestamp = record.timestamp();
            } else {
                oldAgg = oldAggAndTimestamp.value();
                newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
            }

            newAgg = aggregator.apply(record.key(), record.value(), oldAgg);

            store.put(record.key(), ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(
                record.withValue(new Change<>(newAgg, sendOldValues ? oldAgg : null))
                    .withTimestamp(newTimestamp));
        }
    }

    @Override
    public KTableValueGetterSupplier<KIn, VAgg> view() {
        return new KTableValueGetterSupplier<KIn, VAgg>() {

            public KTableValueGetter<KIn, VAgg> get() {
                return new KStreamAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KStreamAggregateValueGetter implements KTableValueGetter<KIn, VAgg> {
        private TimestampedKeyValueStore<KIn, VAgg> store;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<VAgg> get(final KIn key) {
            return store.get(key);
        }
    }
}
