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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableKTableRightJoin<K, V1, V2, VOut> extends KTableKTableAbstractJoin<K, V1, V2, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableKTableRightJoin.class);

    KTableKTableRightJoin(final KTableImpl<K, ?, V1> table1,
                          final KTableImpl<K, ?, V2> table2,
                          final ValueJoiner<? super V1, ? super V2, ? extends VOut> joiner) {
        super(table1, table2, joiner);
    }

    @Override
    public Processor<K, Change<V1>, K, Change<VOut>> get() {
        return new KTableKTableRightJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueGetterSupplier<K, VOut> view() {
        return new KTableKTableRightJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableRightJoinValueGetterSupplier extends KTableKTableAbstractJoinValueGetterSupplier<K, VOut, V1, V2> {

        KTableKTableRightJoinValueGetterSupplier(final KTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                 final KTableValueGetterSupplier<K, V2> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, VOut> get() {
            return new KTableKTableRightJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
        }
    }

    private class KTableKTableRightJoinProcessor extends ContextualProcessor<K, Change<V1>, K, Change<VOut>> {

        private final KTableValueGetter<K, V2> valueGetter;
        private Sensor droppedRecordsSensor;

        KTableKTableRightJoinProcessor(final KTableValueGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @Override
        public void init(final ProcessorContext<K, Change<VOut>> context) {
            super.init(context);
            droppedRecordsSensor = droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics()
            );
            valueGetter.init(context);
        }

        @Override
        public void process(final Record<K, Change<V1>> record) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (record.key() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            final VOut newValue;
            final long resultTimestamp;
            VOut oldValue = null;

            final ValueAndTimestamp<V2> valueAndTimestampLeft = valueGetter.get(record.key());
            final V2 valueLeft = getValueOrNull(valueAndTimestampLeft);
            if (valueLeft == null) {
                return;
            }

            resultTimestamp = Math.max(record.timestamp(), valueAndTimestampLeft.timestamp());

            // joiner == "reverse joiner"
            newValue = joiner.apply(record.value().newValue, valueLeft);

            if (sendOldValues) {
                // joiner == "reverse joiner"
                oldValue = joiner.apply(record.value().oldValue, valueLeft);
            }

            context().forward(record.withValue(new Change<>(newValue, oldValue)).withTimestamp(resultTimestamp));
        }

        @Override
        public void close() {
            valueGetter.close();
        }
    }

    private class KTableKTableRightJoinValueGetter implements KTableValueGetter<K, VOut> {

        private final KTableValueGetter<K, V1> valueGetter1;
        private final KTableValueGetter<K, V2> valueGetter2;

        KTableKTableRightJoinValueGetter(final KTableValueGetter<K, V1> valueGetter1,
                                         final KTableValueGetter<K, V2> valueGetter2) {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        @Override
        public ValueAndTimestamp<VOut> get(final K key) {
            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2.get(key);
            final V2 value2 = getValueOrNull(valueAndTimestamp2);

            if (value2 != null) {
                final ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1.get(key);
                final V1 value1 = getValueOrNull(valueAndTimestamp1);
                final long resultTimestamp;
                if (valueAndTimestamp1 == null) {
                    resultTimestamp = valueAndTimestamp2.timestamp();
                } else {
                    resultTimestamp = Math.max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp());
                }
                return ValueAndTimestamp.make(joiner.apply(value1, value2), resultTimestamp);
            } else {
                return null;
            }
        }

        @Override
        public void close() {
            valueGetter1.close();
            valueGetter2.close();
        }
    }

}
