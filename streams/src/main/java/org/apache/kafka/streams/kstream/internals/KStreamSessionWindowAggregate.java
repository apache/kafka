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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

public class KStreamSessionWindowAggregate<KIn, VIn, VAgg> implements KStreamAggProcessorSupplier<KIn, VIn, Windowed<KIn>, VAgg> {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamSessionWindowAggregate.class);

    private final String storeName;
    private final SessionWindows windows;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;
    private final Merger<? super KIn, VAgg> sessionMerger;

    private boolean sendOldValues = false;

    public KStreamSessionWindowAggregate(final SessionWindows windows,
        final String storeName,
        final Initializer<VAgg> initializer,
        final Aggregator<? super KIn, ? super VIn, VAgg> aggregator,
        final Merger<? super KIn, VAgg> sessionMerger) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.sessionMerger = sessionMerger;
    }

    @Override
    public Processor<KIn, VIn, Windowed<KIn>, Change<VAgg>> get() {
        return new KStreamSessionWindowAggregateProcessor();
    }

    public SessionWindows windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamSessionWindowAggregateProcessor extends
        ContextualProcessor<KIn, VIn, Windowed<KIn>, Change<VAgg>> {

        private SessionStore<KIn, VAgg> store;
        private SessionTupleForwarder<KIn, VAgg> tupleForwarder;
        private Sensor droppedRecordsSensor;
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

        @Override
        public void init(final ProcessorContext<Windowed<KIn>, Change<VAgg>> context) {
            super.init(context);
            final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
            final String threadId = Thread.currentThread().getName();
            droppedRecordsSensor = droppedRecordsSensor(threadId, context.taskId().toString(),
                metrics);
            store = context.getStateStore(storeName);
            tupleForwarder = new SessionTupleForwarder<>(
                store,
                context,
                new SessionCacheFlushListener<>(context),
                sendOldValues
            );
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
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

            final long timestamp = record.timestamp();
            observedStreamTime = Math.max(observedStreamTime, timestamp);
            final long closeTime = observedStreamTime - windows.gracePeriodMs() - windows.inactivityGap();

            final List<KeyValue<Windowed<KIn>, VAgg>> merged = new ArrayList<>();
            final SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            VAgg agg = initializer.apply();

            try (
                final KeyValueIterator<Windowed<KIn>, VAgg> iterator = store.findSessions(
                    record.key(),
                    timestamp - windows.inactivityGap(),
                    timestamp + windows.inactivityGap()
                )
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<KIn>, VAgg> next = iterator.next();
                    merged.add(next);
                    agg = sessionMerger.apply(record.key(), agg, next.value);
                    mergedWindow = mergeSessionWindow(mergedWindow, (SessionWindow) next.key.window());
                }
            }

            if (mergedWindow.end() < closeTime) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record for expired window. " +
                            "topic=[{}] " +
                            "partition=[{}] " +
                            "offset=[{}] " +
                            "timestamp=[{}] " +
                            "window=[{},{}] " +
                            "expiration=[{}] " +
                            "streamTime=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                        timestamp,
                        mergedWindow.start(), mergedWindow.end(),
                        closeTime,
                        observedStreamTime
                    );
                } else {
                    LOG.warn(
                        "Skipping record for expired window. Topic, partition, and offset not known. " +
                            "timestamp=[{}] " +
                            "window=[{},{}] " +
                            "expiration=[{}] " +
                            "streamTime=[{}]",
                        timestamp,
                        mergedWindow.start(), mergedWindow.end(),
                        closeTime,
                        observedStreamTime
                    );
                }
                droppedRecordsSensor.record();
            } else {
                if (!mergedWindow.equals(newSessionWindow)) {
                    for (final KeyValue<Windowed<KIn>, VAgg> session : merged) {
                        store.remove(session.key);
                        tupleForwarder.maybeForward(
                            record.withKey(session.key)
                                .withValue(new Change<>(null, session.value)));
                    }
                }

                agg = aggregator.apply(record.key(), record.value(), agg);
                final Windowed<KIn> sessionKey = new Windowed<>(record.key(), mergedWindow);
                store.put(sessionKey, agg);
                tupleForwarder.maybeForward(
                    record.withKey(sessionKey)
                        .withValue(new Change<>(agg, null)));
            }
        }
    }

    private SessionWindow mergeSessionWindow(final SessionWindow one, final SessionWindow two) {
        final long start = Math.min(one.start(), two.start());
        final long end = Math.max(one.end(), two.end());
        return new SessionWindow(start, end);
    }

    @Override
    public KTableValueGetterSupplier<Windowed<KIn>, VAgg> view() {
        return new KTableValueGetterSupplier<Windowed<KIn>, VAgg>() {
            @Override
            public KTableValueGetter<Windowed<KIn>, VAgg> get() {
                return new KTableSessionWindowValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KTableSessionWindowValueGetter implements KTableValueGetter<Windowed<KIn>, VAgg> {

        private SessionStore<KIn, VAgg> store;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<VAgg> get(final Windowed<KIn> key) {
            return ValueAndTimestamp.make(
                store.fetchSession(key.key(), key.window().start(), key.window().end()),
                key.window().end());
        }
    }

}
