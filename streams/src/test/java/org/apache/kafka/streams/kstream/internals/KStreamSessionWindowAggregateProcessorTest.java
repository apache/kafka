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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogCaptureAppender.Event;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDbTimeOrderedSessionBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KStreamSessionWindowAggregateProcessorTest {

    private static final long GAP_MS = 5 * 60 * 1000L;
    private static final String STORE_NAME = "session-store";

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics();
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test", time);
    private final String threadId = Thread.currentThread().getName();
    private final Initializer<Long> initializer = () -> 0L;
    private final Aggregator<String, String, Long> aggregator = (aggKey, value, aggregate) -> aggregate + 1;
    private final Merger<String, Long> sessionMerger = (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;
    private final List<KeyValueTimestamp<Windowed<String>, Change<Long>>> results = new ArrayList<>();

    private InternalMockProcessorContext<Windowed<String>, Change<Long>> context;
    private KStreamSessionWindowAggregate<String, String, Long> sessionAggregator;
    private Processor<String, String, Windowed<String>, Change<Long>> processor;
    private SessionStore<String, Long> sessionStore;
    
    public EmitStrategy.StrategyType type;

    private EmitStrategy emitStrategy;
    private boolean emitFinal;

    private void setup(final EmitStrategy.StrategyType inputType, final boolean enableCaching) {
        type = inputType;
        // Always process
        final Properties prop = StreamsTestUtils.getStreamsConfig();
        prop.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0);
        final StreamsConfig config = new StreamsConfig(prop);

        context = new InternalMockProcessorContext<Windowed<String>, Change<Long>>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            streamsMetrics,
            config,
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 100000, streamsMetrics),
            time
        ) {
            @Override
            public <K extends Windowed<String>, V extends Change<Long>> void forward(final Record<K, V> record) {
                results.add(new KeyValueTimestamp<>(record.key(), record.value(), record.timestamp()));
            }
        };


        emitFinal = type.equals(EmitStrategy.StrategyType.ON_WINDOW_CLOSE);
        emitStrategy = EmitStrategy.StrategyType.forType(type);

        sessionAggregator = new KStreamSessionWindowAggregate<>(
            SessionWindows.ofInactivityGapWithNoGrace(ofMillis(GAP_MS)),
            STORE_NAME,
            emitStrategy,
            initializer,
            aggregator,
            sessionMerger);

        if (processor != null) {
            processor.close();
        }
        processor = sessionAggregator.get();

        // Set initial timestamp for CachingSessionStore to prepare entry from as default
        // InternalMockProcessorContext#timestamp returns -1.
        context.setTime(0L);
        TaskMetrics.droppedRecordsSensor(threadId, context.taskId().toString(), streamsMetrics);

        initStore(enableCaching);
        processor.init(context);
    }

    private void initStore(final boolean enableCaching) {
        final SessionBytesStoreSupplier supplier = emitStrategy.type() == EmitStrategy.StrategyType.ON_WINDOW_CLOSE ?
            new RocksDbTimeOrderedSessionBytesStoreSupplier(STORE_NAME, GAP_MS * 3, true) :
            Stores.persistentSessionStore(STORE_NAME, ofMillis(GAP_MS * 3));

        final StoreBuilder<SessionStore<String, Long>> storeBuilder = Stores.sessionStoreBuilder(supplier, Serdes.String(), Serdes.Long())
            .withLoggingDisabled();

        if (enableCaching && emitStrategy.type() != EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
            storeBuilder.withCachingEnabled();
        }

        if (sessionStore != null) {
            sessionStore.close();
        }
        sessionStore = storeBuilder.build();
        sessionStore.init((StateStoreContext) context, sessionStore);
    }

    @AfterEach
    public void closeStore() {
        sessionStore.close();
        processor.close();
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldCreateSingleSessionWhenWithinGap(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        processor.process(new Record<>("john", "first", 0L));
        processor.process(new Record<>("john", "second", 500L));

        try (final KeyValueIterator<Windowed<String>, Long> values =
                 sessionStore.findSessions("john", 0, 2000)) {
            assertTrue(values.hasNext());
            assertEquals(Long.valueOf(2), values.next().value);
        }
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldMergeSessions(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        final String sessionId = "mel";
        processor.process(new Record<>(sessionId, "first", 0L));
        assertTrue(sessionStore.findSessions(sessionId, 0, 0).hasNext());

        // move time beyond gap
        processor.process(new Record<>(sessionId, "second", GAP_MS + 1));
        assertTrue(sessionStore.findSessions(sessionId, GAP_MS + 1, GAP_MS + 1).hasNext());
        // should still exist as not within gap
        assertTrue(sessionStore.findSessions(sessionId, 0, 0).hasNext());
        // move time back
        processor.process(new Record<>(sessionId, "third", GAP_MS / 2));

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions(sessionId, 0, GAP_MS + 1)) {
            final KeyValue<Windowed<String>, Long> kv = iterator.next();

            assertEquals(Long.valueOf(3), kv.value);
            assertFalse(iterator.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldUpdateSessionIfTheSameTime(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        processor.process(new Record<>("mel", "first", 0L));
        processor.process(new Record<>("mel", "second", 0L));
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions("mel", 0, 0)) {
            assertEquals(Long.valueOf(2L), iterator.next().value);
            assertFalse(iterator.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldHaveMultipleSessionsForSameIdWhenTimestampApartBySessionGap(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        final String sessionId = "mel";
        long now = 0;
        processor.process(new Record<>(sessionId, "first", now));
        now += GAP_MS + 1;
        processor.process(new Record<>(sessionId, "second", now));
        processor.process(new Record<>(sessionId, "second", now));
        now += GAP_MS + 1;
        processor.process(new Record<>(sessionId, "third", now));
        processor.process(new Record<>(sessionId, "third", now));
        processor.process(new Record<>(sessionId, "third", now));

        sessionStore.flush();

        if (emitFinal) {
            assertEquals(
                Arrays.asList(
                    new KeyValueTimestamp<>(
                        new Windowed<>(sessionId, new SessionWindow(0, 0)),
                        new Change<>(1L, null),
                        0L),
                    new KeyValueTimestamp<>(
                        new Windowed<>(sessionId, new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
                        new Change<>(2L, null),
                        GAP_MS + 1)
                ),
                results
            );
        } else {
            assertEquals(
                Arrays.asList(
                    new KeyValueTimestamp<>(
                        new Windowed<>(sessionId, new SessionWindow(0, 0)),
                        new Change<>(1L, null),
                        0L),
                    new KeyValueTimestamp<>(
                        new Windowed<>(sessionId, new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
                        new Change<>(2L, null),
                        GAP_MS + 1),
                    new KeyValueTimestamp<>(
                        new Windowed<>(sessionId, new SessionWindow(now, now)),
                        new Change<>(3L, null),
                        now)
                ),
                results
            );
        }
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldRemoveMergedSessionsFromStateStore(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        processor.process(new Record<>("a", "1", 0L));

        // first ensure it is in the store
        try (final KeyValueIterator<Windowed<String>, Long> a1 =
                 sessionStore.findSessions("a", 0, 0)) {
            assertEquals(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L), a1.next());
        }


        processor.process(new Record<>("a", "2", 100L));
        // a1 from above should have been removed
        // should have merged session in store
        try (final KeyValueIterator<Windowed<String>, Long> a2 =
                 sessionStore.findSessions("a", 0, 100)) {
            assertEquals(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 100)), 2L), a2.next());
            assertFalse(a2.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldHandleMultipleSessionsAndMerging(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        processor.process(new Record<>("a", "1", 0L));
        processor.process(new Record<>("b", "1", 0L));
        processor.process(new Record<>("c", "1", 0L));
        processor.process(new Record<>("d", "1", 0L));
        processor.process(new Record<>("d", "2", GAP_MS / 2));
        processor.process(new Record<>("a", "2", GAP_MS + 1));
        processor.process(new Record<>("b", "2", GAP_MS + 1));
        processor.process(new Record<>("a", "3", GAP_MS + 1 + GAP_MS / 2));
        processor.process(new Record<>("c", "3", GAP_MS + 1 + GAP_MS / 2));

        sessionStore.flush();

        if (emitFinal) {
            assertEquals(Arrays.asList(
                new KeyValueTimestamp<>(
                    new Windowed<>("a", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("b", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("c", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("d", new SessionWindow(0, GAP_MS / 2)),
                    new Change<>(2L, null),
                    GAP_MS / 2)
                ),
                results);
        } else {
            assertEquals(
                Arrays.asList(
                    new KeyValueTimestamp<>(
                        new Windowed<>("a", new SessionWindow(0, 0)),
                        new Change<>(1L, null),
                        0L),
                    new KeyValueTimestamp<>(
                        new Windowed<>("b", new SessionWindow(0, 0)),
                        new Change<>(1L, null),
                        0L),
                    new KeyValueTimestamp<>(
                        new Windowed<>("c", new SessionWindow(0, 0)),
                        new Change<>(1L, null),
                       0L),
                    new KeyValueTimestamp<>(
                        new Windowed<>("d", new SessionWindow(0, GAP_MS / 2)),
                        new Change<>(2L, null),
                        GAP_MS / 2),
                    new KeyValueTimestamp<>(
                        new Windowed<>("b", new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
                        new Change<>(1L, null),
                        GAP_MS + 1),
                    new KeyValueTimestamp<>(
                        new Windowed<>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1 + GAP_MS / 2)),
                        new Change<>(2L, null),
                        GAP_MS + 1 + GAP_MS / 2),
                    new KeyValueTimestamp<>(new Windowed<>(
                        "c",
                        new SessionWindow(GAP_MS + 1 + GAP_MS / 2, GAP_MS + 1 + GAP_MS / 2)), new Change<>(1L, null),
                        GAP_MS + 1 + GAP_MS / 2)
                    ),
                    results
            );
        }
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldGetAggregatedValuesFromValueGetter(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        final KTableValueGetter<Windowed<String>, Long> getter = sessionAggregator.view().get();
        getter.init(context);
        processor.process(new Record<>("a", "1", 0L));
        processor.process(new Record<>("a", "1", GAP_MS + 1));
        processor.process(new Record<>("a", "2", GAP_MS + 1));
        final long t0 = getter.get(new Windowed<>("a", new SessionWindow(0, 0))).value();
        final long t1 = getter.get(new Windowed<>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1))).value();
        assertEquals(1L, t0);
        assertEquals(2L, t1);
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldImmediatelyForwardNewSessionWhenNonCachedStore(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        if (emitFinal)
            return;

        initStore(false);
        processor.init(context);

        processor.process(new Record<>("a", "1", 0L));
        processor.process(new Record<>("b", "1", 0L));
        processor.process(new Record<>("c", "1", 0L));

        assertEquals(
            Arrays.asList(
                new KeyValueTimestamp<>(
                    new Windowed<>("a", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("b", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("c", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L)
            ),
            results
        );
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldImmediatelyForwardRemovedSessionsWhenMerging(final EmitStrategy.StrategyType inputType) {
        setup(inputType, true);
        if (emitFinal)
            return;

        initStore(false);
        processor.init(context);

        processor.process(new Record<>("a", "1", 0L));
        processor.process(new Record<>("a", "1", 5L));
        assertEquals(
            Arrays.asList(
                new KeyValueTimestamp<>(
                    new Windowed<>("a", new SessionWindow(0, 0)),
                    new Change<>(1L, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("a", new SessionWindow(0, 0)),
                    new Change<>(null, null),
                    0L),
                new KeyValueTimestamp<>(
                    new Windowed<>("a", new SessionWindow(0, 5)),
                    new Change<>(2L, null),
                    5L)
            ),
            results
        );
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldLogAndMeterWhenSkippingNullKeyWithBuiltInMetrics(final EmitStrategy.StrategyType inputType) {
        setup(inputType, false);
        context.setRecordContext(
            new ProcessorRecordContext(-1, -2, -3, "topic", new RecordHeaders())
        );

        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(KStreamSessionWindowAggregate.class)) {

            processor.process(new Record<>(null, "1", 0L));

            assertThat(
                appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals("WARN"))
                    .map(Event::getMessage)
                    .collect(Collectors.toList()),
                hasItem("Skipping record due to null key. topic=[topic] partition=[-3] offset=[-2]")
            );
        }

        assertEquals(
            1.0,
            getMetricByName(context.metrics().metrics(), "dropped-records-total", "stream-task-metrics").metricValue()
        );
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldLogAndMeterWhenSkippingLateRecordWithZeroGrace(final EmitStrategy.StrategyType inputType) {
        setup(inputType, false);
        final Processor<String, String, Windowed<String>, Change<Long>> processor = new KStreamSessionWindowAggregate<>(
            SessionWindows.ofInactivityGapAndGrace(ofMillis(10L), ofMillis(0L)),
            STORE_NAME,
            EmitStrategy.onWindowUpdate(),
            initializer,
            aggregator,
            sessionMerger
        ).get();
        processor.init(context);

        // dummy record to establish stream time = 0
        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
        processor.process(new Record<>("dummy", "dummy", 0L));

        // record arrives on time, should not be skipped
        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
        processor.process(new Record<>("OnTime1", "1", 0L));

        // dummy record to advance stream time = 11, 10 for gap time plus 1 to place outside window
        context.setRecordContext(new ProcessorRecordContext(11, -2, -3, "topic", new RecordHeaders()));
        processor.process(new Record<>("dummy", "dummy", 11L));

        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(KStreamSessionWindowAggregate.class)) {

            // record is late
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("Late1", "1", 0L));

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record for expired window." +
                    " topic=[topic] partition=[-3] offset=[-2] timestamp=[0] window=[0,0] expiration=[1] streamTime=[11]")
            );
        }

        final MetricName dropTotal;
        final MetricName dropRate;
        dropTotal = new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
        dropRate = new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "The average number of dropped records per second",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
        assertThat(metrics.metrics().get(dropTotal).metricValue(), is(1.0));
        assertThat(
            (Double) metrics.metrics().get(dropRate).metricValue(),
            greaterThan(0.0)
        );
    }

    @ParameterizedTest
    @EnumSource(EmitStrategy.StrategyType.class)
    public void shouldLogAndMeterWhenSkippingLateRecordWithNonzeroGrace(final EmitStrategy.StrategyType inputType) {
        setup(inputType, false);
        final Processor<String, String, Windowed<String>, Change<Long>> processor = new KStreamSessionWindowAggregate<>(
            SessionWindows.ofInactivityGapAndGrace(ofMillis(10L), ofMillis(1L)),
            STORE_NAME,
            EmitStrategy.onWindowUpdate(),
            initializer,
            aggregator,
            sessionMerger
        ).get();
        processor.init(context);

        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(KStreamSessionWindowAggregate.class)) {

            // dummy record to establish stream time = 0
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("dummy", "dummy", 0L));

            // record arrives on time, should not be skipped
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("OnTime1", "1", 0L));

            // dummy record to advance stream time = 11, 10 for gap time plus 1 to place at edge of window
            context.setRecordContext(new ProcessorRecordContext(11, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("dummy", "dummy", 11L));

            // delayed record arrives on time, should not be skipped
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("OnTime2", "1", 0L));

            // dummy record to advance stream time = 12, 10 for gap time plus 2 to place outside window
            context.setRecordContext(new ProcessorRecordContext(12, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("dummy", "dummy", 12L));

            // delayed record arrives late
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", new RecordHeaders()));
            processor.process(new Record<>("Late1", "1", 0L));

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record for expired window." +
                    " topic=[topic] partition=[-3] offset=[-2] timestamp=[0] window=[0,0] expiration=[1] streamTime=[12]")
            );
        }

        final MetricName dropTotal;
        final MetricName dropRate;
        dropTotal = new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
        dropRate = new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "The average number of dropped records per second",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );

        assertThat(metrics.metrics().get(dropTotal).metricValue(), is(1.0));
        assertThat(
            (Double) metrics.metrics().get(dropRate).metricValue(),
            greaterThan(0.0));
    }
}
