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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KStreamSessionWindowAggregateProcessorTest {

    private static final long GAP_MS = 5 * 60 * 1000L;
    private static final String STORE_NAME = "session-store";

    private final Initializer<Long> initializer = new Initializer<Long>() {
        @Override
        public Long apply() {
            return 0L;
        }
    };
    private final Aggregator<String, String, Long> aggregator = new Aggregator<String, String, Long>() {
        @Override
        public Long apply(final String aggKey, final String value, final Long aggregate) {
            return aggregate + 1;
        }
    };
    private final Merger<String, Long> sessionMerger = new Merger<String, Long>() {
        @Override
        public Long apply(final String aggKey, final Long aggOne, final Long aggTwo) {
            return aggOne + aggTwo;
        }
    };
    private final KStreamSessionWindowAggregate<String, String, Long> sessionAggregator =
        new KStreamSessionWindowAggregate<>(
            SessionWindows.with(GAP_MS).until(3 * GAP_MS),
            STORE_NAME,
            initializer,
            aggregator,
            sessionMerger);

    private final List<KeyValue> results = new ArrayList<>();
    private final Processor<String, String> processor = sessionAggregator.get();
    private SessionStore<String, Long> sessionStore;
    private InternalMockProcessorContext context;


    @Before
    public void initializeStore() {
        final File stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(stateDir,
            Serdes.String(), Serdes.String(), new NoOpRecordCollector(), new ThreadCache(new LogContext("testCache "), 100000, new MockStreamsMetrics(new Metrics()))) {
            @Override
            public <K, V> void forward(final K key, final V value) {
                results.add(KeyValue.pair(key, value));
            }
        };

        initStore(true);
        processor.init(context);
    }

    private void initStore(final boolean enableCaching) {
        final StoreBuilder<SessionStore<String, Long>> storeBuilder = Stores.sessionStoreBuilder(Stores.persistentSessionStore(STORE_NAME, GAP_MS * 3),
                Serdes.String(),
                Serdes.Long())
                .withLoggingDisabled();

        if (enableCaching) {
            storeBuilder.withCachingEnabled();
        }

        sessionStore = storeBuilder.build();
        sessionStore.init(context, sessionStore);
    }

    @After
    public void closeStore() {
        sessionStore.close();
    }

    @Test
    public void shouldCreateSingleSessionWhenWithinGap() {
        context.setTime(0);
        processor.process("john", "first");
        context.setTime(500);
        processor.process("john", "second");

        final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions("john", 0, 2000);
        assertTrue(values.hasNext());
        assertEquals(Long.valueOf(2), values.next().value);
    }


    @Test
    public void shouldMergeSessions() {
        context.setTime(0);
        final String sessionId = "mel";
        processor.process(sessionId, "first");
        assertTrue(sessionStore.findSessions(sessionId, 0, 0).hasNext());

        // move time beyond gap
        context.setTime(GAP_MS + 1);
        processor.process(sessionId, "second");
        assertTrue(sessionStore.findSessions(sessionId, GAP_MS + 1, GAP_MS + 1).hasNext());
        // should still exist as not within gap
        assertTrue(sessionStore.findSessions(sessionId, 0, 0).hasNext());
        // move time back
        context.setTime(GAP_MS / 2);
        processor.process(sessionId, "third");

        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions(sessionId, 0, GAP_MS + 1);
        final KeyValue<Windowed<String>, Long> kv = iterator.next();

        assertEquals(Long.valueOf(3), kv.value);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldUpdateSessionIfTheSameTime() {
        context.setTime(0);
        processor.process("mel", "first");
        processor.process("mel", "second");
        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions("mel", 0, 0);
        assertEquals(Long.valueOf(2L), iterator.next().value);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldHaveMultipleSessionsForSameIdWhenTimestampApartBySessionGap() {
        final String sessionId = "mel";
        long time = 0;
        context.setTime(time);
        processor.process(sessionId, "first");
        context.setTime(time += GAP_MS + 1);
        processor.process(sessionId, "second");
        processor.process(sessionId, "second");
        context.setTime(time += GAP_MS + 1);
        processor.process(sessionId, "third");
        processor.process(sessionId, "third");
        processor.process(sessionId, "third");

        sessionStore.flush();
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(sessionId, new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>(sessionId, new SessionWindow(GAP_MS + 1, GAP_MS + 1)), new Change<>(2L, null)),
                KeyValue.pair(new Windowed<>(sessionId, new SessionWindow(time, time)), new Change<>(3L, null))
            ),
            results
        );

    }


    @Test
    public void shouldRemoveMergedSessionsFromStateStore() {
        context.setTime(0);
        processor.process("a", "1");

        // first ensure it is in the store
        final KeyValueIterator<Windowed<String>, Long> a1 = sessionStore.findSessions("a", 0, 0);
        assertEquals(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L), a1.next());

        context.setTime(100);
        processor.process("a", "2");
        // a1 from above should have been removed
        // should have merged session in store
        final KeyValueIterator<Windowed<String>, Long> a2 = sessionStore.findSessions("a", 0, 100);
        assertEquals(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 100)), 2L), a2.next());
        assertFalse(a2.hasNext());
    }

    @Test
    public void shouldHandleMultipleSessionsAndMerging() {
        context.setTime(0);
        processor.process("a", "1");
        processor.process("b", "1");
        processor.process("c", "1");
        processor.process("d", "1");
        context.setTime(GAP_MS / 2);
        processor.process("d", "2");
        context.setTime(GAP_MS + 1);
        processor.process("a", "2");
        processor.process("b", "2");
        context.setTime(GAP_MS + 1 + GAP_MS / 2);
        processor.process("a", "3");
        processor.process("c", "3");

        sessionStore.flush();

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("b", new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("c", new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("d", new SessionWindow(0, GAP_MS / 2)), new Change<>(2L, null)),
                KeyValue.pair(new Windowed<>("b", new SessionWindow(GAP_MS + 1, GAP_MS + 1)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1 + GAP_MS / 2)), new Change<>(2L, null)),
                KeyValue.pair(new Windowed<>("c", new SessionWindow(GAP_MS + 1 + GAP_MS / 2, GAP_MS + 1 + GAP_MS / 2)), new Change<>(1L, null))
            ),
            results
        );
    }


    @Test
    public void shouldGetAggregatedValuesFromValueGetter() {
        final KTableValueGetter<Windowed<String>, Long> getter = sessionAggregator.view().get();
        getter.init(context);
        context.setTime(0);
        processor.process("a", "1");
        context.setTime(GAP_MS + 1);
        processor.process("a", "1");
        processor.process("a", "2");
        final long t0 = getter.get(new Windowed<>("a", new SessionWindow(0, 0)));
        final long t1 = getter.get(new Windowed<>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1)));
        assertEquals(1L, t0);
        assertEquals(2L, t1);
    }

    @Test
    public void shouldImmediatelyForwardNewSessionWhenNonCachedStore() {
        initStore(false);
        processor.init(context);

        context.setTime(0);
        processor.process("a", "1");
        processor.process("b", "1");
        processor.process("c", "1");

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("b", new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("c", new SessionWindow(0, 0)), new Change<>(1L, null))
            ),
            results
        );
    }

    @Test
    public void shouldImmediatelyForwardRemovedSessionsWhenMerging() {
        initStore(false);
        processor.init(context);

        context.setTime(0);
        processor.process("a", "1");
        context.setTime(5);
        processor.process("a", "1");
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), new Change<>(1L, null)),
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), new Change<>(null, null)),
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 5)), new Change<>(2L, null))
            ),
            results
        );

    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullKey() {
        initStore(false);
        processor.init(context);
        context.setRecordContext(new ProcessorRecordContext(-1, -2, -3, "topic"));
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        processor.process(null, "1");
        LogCaptureAppender.unregister(appender);

        assertEquals(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key. value=[1] topic=[topic] partition=[-3] offset=[-2]"));
    }
}