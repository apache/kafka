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

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KTableKTableOuterJoin<K, R, V1, V2> extends KTableKTableAbstractJoin<K, R, V1, V2> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableKTableOuterJoin.class);

    KTableKTableOuterJoin(final KTableImpl<K, ?, V1> table1,
                          final KTableImpl<K, ?, V2> table2,
                          final ValueJoiner<? super V1, ? super V2, ? extends R> joiner) {
        super(table1, table2, joiner);
    }

    @Override
    public Processor<K, Change<V1>> get() {
        return new KTableKTableOuterJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueGetterSupplier<K, R> view() {
        return new KTableKTableOuterJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableOuterJoinValueGetterSupplier extends KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> {

        KTableKTableOuterJoinValueGetterSupplier(final KTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                 final KTableValueGetterSupplier<K, V2> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, R> get() {
            return new KTableKTableOuterJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
        }
    }

    private class KTableKTableOuterJoinProcessor extends AbstractProcessor<K, Change<V1>> {

        private final KTableValueGetter<K, V2> valueGetter;
        private StreamsMetricsImpl metrics;

        KTableKTableOuterJoinProcessor(final KTableValueGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            valueGetter.init(context);
        }

        @Override
        public void process(final K key, final Change<V1> change) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (key == null) {
                LOG.warn(
                    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    change, context().topic(), context().partition(), context().offset()
                );
                metrics.skippedRecordsSensor().record();
                return;
            }

            R newValue = null;
            R oldValue = null;
            long resultTimestamp = context().timestamp();

            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter.get(key);
            final V2 value2;
            if (valueAndTimestamp2 == null) {
                if (change.newValue == null && change.oldValue == null) {
                    return;
                }
                value2 = null;
            } else {
                value2 = valueAndTimestamp2.value();
                resultTimestamp = Math.max(resultTimestamp, valueAndTimestamp2.timestamp());
            }

            if (value2 != null || change.newValue != null) {
                newValue = joiner.apply(change.newValue, value2);
            }

            if (sendOldValues && (value2 != null || change.oldValue != null)) {
                oldValue = joiner.apply(change.oldValue, value2);
            }

            context().forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
        }

        @Override
        public void close() {
            valueGetter.close();
        }
    }

    private class KTableKTableOuterJoinValueGetter implements KTableValueGetter<K, R> {

        private final KTableValueGetter<K, V1> valueGetter1;
        private final KTableValueGetter<K, V2> valueGetter2;

        KTableKTableOuterJoinValueGetter(final KTableValueGetter<K, V1> valueGetter1,
                                         final KTableValueGetter<K, V2> valueGetter2) {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        @Override
        public void init(final ProcessorContext context) {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        @Override
        public ValueAndTimestamp<R> get(final K key) {
            R newValue = null;
            long resultTimestamp = -1;

            final ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1.get(key);
            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2.get(key);

            final V1 value1;
            final V2 value2;
            if (valueAndTimestamp1 != null) {
                value1 = valueAndTimestamp1.value();
                resultTimestamp = valueAndTimestamp1.timestamp();
            } else {
                value1 = null;
            }
            if (valueAndTimestamp2 != null) {
                value2 = valueAndTimestamp2.value();
                resultTimestamp = valueAndTimestamp1 != null ? Math.max(resultTimestamp, valueAndTimestamp2.timestamp()) : valueAndTimestamp2.timestamp();
            } else {
                value2 = null;
            }

            if (value1 != null || value2 != null) {
                newValue = joiner.apply(value1, value2);
            }

            return ValueAndTimestamp.make(newValue, resultTimestamp);
        }

        @Override
        public void close() {
            valueGetter1.close();
            valueGetter2.close();
        }
    }

}
