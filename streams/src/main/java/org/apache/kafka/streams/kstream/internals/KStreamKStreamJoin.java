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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class KStreamKStreamJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamJoin.class);

    private final String otherWindowName;
    private final long joinBeforeMs;
    private final long joinAfterMs;

    private final ValueJoiner<? super V1, ? super V2, ? extends R> joiner;
    private final boolean outer;

    KStreamKStreamJoin(final String otherWindowName, final long joinBeforeMs, final long joinAfterMs, final ValueJoiner<? super V1, ? super V2, ? extends R> joiner, final boolean outer) {
        this.otherWindowName = otherWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joiner = joiner;
        this.outer = outer;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKStreamJoinProcessor();
    }

    private class KStreamKStreamJoinProcessor extends AbstractProcessor<K, V1> {

        private WindowStore<K, ValueAndTimestamp<V2>> otherWindow;
        private StreamsMetricsImpl metrics;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();

            StateStore store = context.getStateStore(otherWindowName);
            if (store instanceof WrappedStateStore) {
                store = ((WrappedStateStore) store).wrappedStore();
            }
            otherWindow = ((KStreamImpl.WindowStoreFacade<K, V2>) store).inner;
        }


        @Override
        public void process(final K key, final V1 value) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key == null || value == null) {
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                metrics.skippedRecordsSensor().record();
                return;
            }

            boolean needOuterJoin = outer;

            final long timeFrom = Math.max(0L, context().timestamp() - joinBeforeMs);
            final long timeTo = Math.max(0L, context().timestamp() + joinAfterMs);

            try (final WindowStoreIterator<ValueAndTimestamp<V2>> iter = otherWindow.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    final KeyValue<Long, ValueAndTimestamp<V2>> other = iter.next();
                    final long resultTimestamp = Math.max(context().timestamp(), other.value.timestamp());
                    context().forward(key, joiner.apply(value, other.value.value()), To.all().withTimestamp(resultTimestamp));
                }

                if (needOuterJoin) {
                    context().forward(key, joiner.apply(value, null));
                }
            }
        }
    }
}
