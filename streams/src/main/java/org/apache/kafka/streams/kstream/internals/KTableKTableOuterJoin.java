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
import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableKTableOuterJoin<K, V1, V2, VOut> extends KTableKTableAbstractJoin<K, V1, V2, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableKTableOuterJoin.class);

    KTableKTableOuterJoin(final KTableImpl<K, ?, V1> table1,
                          final KTableImpl<K, ?, V2> table2,
                          final ValueJoiner<? super V1, ? super V2, ? extends VOut> joiner) {
        super(table1, table2, joiner);
    }

    @Override
    public Processor<K, Change<V1>, K, Change<VOut>> get() {
        return new KTableKTableOuterJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueGetterSupplier<K, VOut> view() {
        return new KTableKTableOuterJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableOuterJoinValueGetterSupplier extends KTableKTableAbstractJoinValueGetterSupplier<K, VOut, V1, V2> {

        KTableKTableOuterJoinValueGetterSupplier(final KTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                 final KTableValueGetterSupplier<K, V2> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, VOut> get() {
            return new KTableKTableOuterJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
        }
    }

    private class KTableKTableOuterJoinProcessor extends ContextualProcessor<K, Change<V1>, K, Change<VOut>> {

        private final KTableValueGetter<K, V2> valueGetter;
        private Sensor droppedRecordsSensor;

        KTableKTableOuterJoinProcessor(final KTableValueGetter<K, V2> valueGetter) {
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

            // drop out-of-order records from versioned tables (cf. KIP-914)
            if (useVersionedSemantics && !record.value().isLatest) {
                LOG.info("Skipping out-of-order record from versioned table while performing table-table join.");
                droppedRecordsSensor.record();
                return;
            }

            VOut newValue = null;
            final long resultTimestamp;
            VOut oldValue = null;

            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter.get(record.key());
            final V2 value2 = getValueOrNull(valueAndTimestamp2);
            if (value2 == null) {
                if (record.value().newValue == null && record.value().oldValue == null) {
                    return;
                }
                resultTimestamp = record.timestamp();
            } else {
                resultTimestamp = Math.max(record.timestamp(), valueAndTimestamp2.timestamp());
            }

            if (value2 != null || record.value().newValue != null) {
                newValue = joiner.apply(record.value().newValue, value2);
            }

            if (sendOldValues && (value2 != null || record.value().oldValue != null)) {
                oldValue = joiner.apply(record.value().oldValue, value2);
            }

            context().forward(record.withValue(new Change<>(newValue, oldValue, record.value().isLatest)).withTimestamp(resultTimestamp));
        }

        @Override
        public void close() {
            valueGetter.close();
        }
    }

    private class KTableKTableOuterJoinValueGetter implements KTableValueGetter<K, VOut> {

        private final KTableValueGetter<K, V1> valueGetter1;
        private final KTableValueGetter<K, V2> valueGetter2;

        KTableKTableOuterJoinValueGetter(final KTableValueGetter<K, V1> valueGetter1,
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
            VOut newValue = null;

            final ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1.get(key);
            final V1 value1;
            final long timestamp1;
            if (valueAndTimestamp1 == null) {
                value1 = null;
                timestamp1 = UNKNOWN;
            } else {
                value1 = valueAndTimestamp1.value();
                timestamp1 = valueAndTimestamp1.timestamp();
            }

            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2.get(key);
            final V2 value2;
            final long timestamp2;
            if (valueAndTimestamp2 == null) {
                value2 = null;
                timestamp2 = UNKNOWN;
            } else {
                value2 = valueAndTimestamp2.value();
                timestamp2 = valueAndTimestamp2.timestamp();
            }

            if (value1 != null || value2 != null) {
                newValue = joiner.apply(value1, value2);
            }

            return ValueAndTimestamp.make(newValue, Math.max(timestamp1, timestamp2));
        }

        @Override
        public boolean isVersioned() {
            // even though we can derive a proper versioned result (assuming both parent value
            // getters are versioned), we choose not to since the output of a join of two
            // versioned tables today is not considered versioned (cf KIP-914)
            return false;
        }

        @Override
        public void close() {
            valueGetter1.close();
            valueGetter2.close();
        }
    }

}
