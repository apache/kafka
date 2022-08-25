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

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTracker;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KStreamKStreamSelfJoin<K, V1, V2, VOut> implements ProcessorSupplier<K, V1, K, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamSelfJoin.class);

    private final String windowName;
    private final long joinBeforeMs;
    private final long joinAfterMs;
    private final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VOut> joinerThis;
    private final ValueJoinerWithKey<? super K, ? super V2, ? super V1, ? extends VOut> joinerOther;

    private final TimeTracker sharedTimeTracker;

    KStreamKStreamSelfJoin(
        final String windowName,
        final JoinWindowsInternal windows,
        final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VOut> joinerThis,
        final ValueJoinerWithKey<? super K, ? super V2, ? super V1, ? extends VOut> joinerOther,
        final TimeTracker sharedTimeTracker) {

        this.windowName = windowName;
        this.joinBeforeMs = windows.beforeMs;
        this.joinAfterMs = windows.afterMs;
        this.joinerThis = joinerThis;
        this.joinerOther = joinerOther;
        this.sharedTimeTracker = sharedTimeTracker;
    }

    @Override
    public Processor<K, V1, K, VOut> get() {
        return new KStreamKStreamSelfJoinProcessor();
    }

    private class KStreamKStreamSelfJoinProcessor extends ContextualProcessor<K, V1, K, VOut> {
        private WindowStore<K, V2> windowStore;
        private Sensor droppedRecordsSensor;

        @Override
        public void init(final ProcessorContext<K, VOut> context) {
            super.init(context);

            final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            windowStore = context.getStateStore(windowName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(final Record<K, V1> record) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (record.key() == null || record.value() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key or value. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key or value. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            final long inputRecordTimestamp = record.timestamp();
            final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
            final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

            sharedTimeTracker.advanceStreamTime(inputRecordTimestamp);
            System.out.printf("-----> current record: key=%s, value=%s, ts=%d %n",
                              record.key(), record.value(), inputRecordTimestamp);

            // Join current record with other
            try (final WindowStoreIterator<V2> iter = windowStore.fetch(
                record.key(), timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    final KeyValue<Long, V2> otherRecord = iter.next();
                    final long otherRecordTimestamp = otherRecord.key;
                    System.out.printf("-----> other record: value=%s, ts=%d %n",
                                      otherRecord.value, otherRecordTimestamp);

                    // Join this with other
                    context().forward(
                        record.withValue(joinerThis.apply(
                                record.key(), record.value(), otherRecord.value))
                            .withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));

                    System.out.printf("-----> join this with other: key=%s, this=%s, other=%s %n",
                                      record.key(), record.value(), otherRecord.value);

                    // Join other with this
//                    context().forward(
//                        record.withValue(joinerThis.apply(
//                            record.key(), (V1) otherRecord.value, (V2) record.value()))
//                            .withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));
//
//                    System.out.printf("-----> join other with this: key=%s, this=%s, other=%s %n",
//                                      record.key(), otherRecord.value, record.value());
//
                    // Join with self
                }

                // Join other with current record
                try (final WindowStoreIterator<V2> iter2 = windowStore.fetch(
                    record.key(), timeFrom, timeTo)) {
                    while (iter2.hasNext()) {
                        final KeyValue<Long, V2> otherRecord = iter2.next();
                        final long otherRecordTimestamp = otherRecord.key;
                        System.out.printf("-----> other record: value=%s, ts=%d %n",
                                          otherRecord.value, otherRecordTimestamp);
                        context().forward(
                            record.withValue(joinerThis.apply(
                                    record.key(), (V1) otherRecord.value, (V2) record.value()))
                                .withTimestamp(Math.max(inputRecordTimestamp,
                                                        otherRecordTimestamp)));

                        System.out.printf(
                            "-----> join other with this: key=%s, other=%s, this=%s %n",
                            record.key(),
                            otherRecord.value,
                            record.value());

                    }
                }

                // Join current with itself
//                if (windowStore.fetch(record.key(), timeFrom, timeTo).hasNext()) {
                context().forward(
                    record.withValue(joinerThis.apply(
                        record.key(), record.value(), (V2) record.value()))
                          .withTimestamp(inputRecordTimestamp));
//                }
            }
        }
    }
}
