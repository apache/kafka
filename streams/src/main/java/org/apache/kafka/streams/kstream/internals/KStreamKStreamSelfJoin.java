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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KStreamKStreamSelfJoin<K, V1, V2, VOut> implements ProcessorSupplier<K, V1, K, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamSelfJoin.class);

    private final String windowName;
    private final long joinThisBeforeMs;
    private final long joinThisAfterMs;
    private final long joinOtherBeforeMs;
    private final long joinOtherAfterMs;
    private final long retentionPeriod;
    private final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VOut> joinerThis;

    KStreamKStreamSelfJoin(
        final String windowName,
        final JoinWindowsInternal windows,
        final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VOut> joinerThis,
        final long retentionPeriod) {

        this.windowName = windowName;
        this.joinThisBeforeMs = windows.beforeMs;
        this.joinThisAfterMs = windows.afterMs;
        this.joinOtherBeforeMs = windows.afterMs;
        this.joinOtherAfterMs = windows.beforeMs;
        this.joinerThis = joinerThis;
        this.retentionPeriod = retentionPeriod;
    }

    @Override
    public Processor<K, V1, K, VOut> get() {
        return new KStreamKStreamSelfJoinProcessor();
    }

    private class KStreamKStreamSelfJoinProcessor extends ContextualProcessor<K, V1, K, VOut> {
        private final TimeTracker timeTracker = new TimeTracker();
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
            if (StreamStreamJoinUtil.skipRecord(record, LOG, droppedRecordsSensor, context())) {
                return;
            }

            final long inputRecordTimestamp = record.timestamp();
            long timeFrom = Math.max(0L, inputRecordTimestamp - joinThisBeforeMs);
            long timeTo = Math.max(0L, inputRecordTimestamp + joinThisAfterMs);
            boolean emittedJoinWithSelf = false;
            final Record selfRecord = record
                .withValue(joinerThis.apply(record.key(), record.value(), (V2) record.value()))
                .withTimestamp(inputRecordTimestamp);
            timeTracker.advanceStreamTime(inputRecordTimestamp);
            // We emit the self record only if it isn't expired.
            final boolean emitSelfRecord = inputRecordTimestamp > timeTracker.streamTime - retentionPeriod + 1;

            // Join current record with other
            try (final WindowStoreIterator<V2> iter = windowStore.fetch(record.key(), timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    final KeyValue<Long, V2> otherRecord = iter.next();
                    final long otherRecordTimestamp = otherRecord.key;

                    // Join this with other
                    context().forward(
                        record.withValue(joinerThis.apply(
                                record.key(), record.value(), otherRecord.value))
                            .withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));
                }
            }

            // Needs to be in a different loop to ensure correct ordering of records where
            // correct ordering means it matches the output of an inner join.
            timeFrom = Math.max(0L, inputRecordTimestamp - joinOtherBeforeMs);
            timeTo = Math.max(0L, inputRecordTimestamp + joinOtherAfterMs);
            try (final WindowStoreIterator<V2> iter2 = windowStore.fetch(record.key(), timeFrom, timeTo)) {
                while (iter2.hasNext()) {
                    final KeyValue<Long, V2> otherRecord = iter2.next();
                    final long otherRecordTimestamp = otherRecord.key;
                    final long maxRecordTimestamp = Math.max(inputRecordTimestamp, otherRecordTimestamp);

                    // This is needed so that output records follow timestamp order
                    // Join this with self
                    if (inputRecordTimestamp < maxRecordTimestamp && !emittedJoinWithSelf && emitSelfRecord) {
                        emittedJoinWithSelf = true;
                        context().forward(selfRecord);
                    }

                    // Join other with current record
                    context().forward(
                        record
                            .withValue(joinerThis.apply(record.key(), (V1) otherRecord.value, (V2) record.value()))
                            .withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));
                }
            }

            // Join this with self
            if (!emittedJoinWithSelf && emitSelfRecord) {
                context().forward(selfRecord);
            }
        }
    }
}
