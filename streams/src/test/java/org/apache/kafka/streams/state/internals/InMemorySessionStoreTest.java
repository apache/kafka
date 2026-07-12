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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.valuesToSet;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class InMemorySessionStoreTest extends AbstractSessionBytesStoreTest {

    @Override
    StoreType storeType() {
        return StoreType.InMemoryStore;
    }

    @Test
    public void shouldCountNumEntries() {
        final InMemorySessionStore store = new InMemorySessionStore("test", RETENTION_PERIOD, "scope");
        store.init(context, store);

        assertEquals(0L, store.numEntries());

        store.put(
                new Windowed<>(
                        Bytes.wrap("a".getBytes()),
                        new SessionWindow(0, 0)
                ),
                "1".getBytes()
        );
        assertEquals(1L, store.numEntries());

        store.put(
                new Windowed<>(
                        Bytes.wrap("b".getBytes()),
                        new SessionWindow(0, 10)
                ),
                "2".getBytes()
        );
        assertEquals(2L, store.numEntries());

        store.put(
                new Windowed<>(
                        Bytes.wrap("a".getBytes()),
                        new SessionWindow(5, 15)
                ),
                "3".getBytes()
        );
        assertEquals(3L, store.numEntries());

        // remove one entry
        store.remove(
                new Windowed<>(
                        Bytes.wrap("a".getBytes()),
                        new SessionWindow(0, 0)
                )
        );
        assertEquals(2L, store.numEntries());

        store.close();
    }

    @Test
    public void shouldNotExpireFromOpenIterator() {

        sessionStore.put(
                new Windowed<>("a", new SessionWindow(0, 0)),
                1L
        );
        sessionStore.put(
                new Windowed<>("aa", new SessionWindow(0, 10)),
                2L
        );
        sessionStore.put(
                new Windowed<>("a", new SessionWindow(10, 20)),
                3L
        );

        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions("a", "b", 0L, RETENTION_PERIOD);

        // Advance stream time to expire the first three record
        sessionStore.put(
                new Windowed<>(
                        "aa",
                        new SessionWindow(100, 2 * RETENTION_PERIOD)
                ),
                4L
        );

        assertEquals(Set.of(1L, 2L, 3L, 4L), valuesToSet(iterator));
        assertFalse(iterator.hasNext());

        iterator.close();

        try (final KeyValueIterator<Windowed<String>, Long> it =
             sessionStore.findSessions("a", "b", 0L, 20L)) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void readOnlyCommittedShouldHideStagedSessions() {
        final InMemorySessionStore store = openTransactionalSessionStore();
        try {
            final Bytes k = Bytes.wrap("k".getBytes());
            store.put(new Windowed<>(k, new SessionWindow(0, 10)), "v1".getBytes());
            store.commit(Map.of());

            store.put(new Windowed<>(k, new SessionWindow(20, 30)), "v2".getBytes());
            store.remove(new Windowed<>(k, new SessionWindow(0, 10)));

            final ReadOnlySessionStore<Bytes, byte[]> uncommitted = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
            final ReadOnlySessionStore<Bytes, byte[]> committed = store.readOnly(IsolationLevel.READ_COMMITTED);

            assertNull(uncommitted.fetchSession(k, 0, 10));
            assertArrayEquals("v2".getBytes(), uncommitted.fetchSession(k, 20, 30));
            assertArrayEquals("v1".getBytes(), committed.fetchSession(k, 0, 10));
            assertNull(committed.fetchSession(k, 20, 30));
        } finally {
            store.close();
        }
    }

    @Test
    public void readOnlyFindSessionsShouldRespectIsolationLevel() {
        final InMemorySessionStore store = openTransactionalSessionStore();
        try {
            final Bytes k = Bytes.wrap("k".getBytes());
            store.put(new Windowed<>(k, new SessionWindow(0, 10)), "v1".getBytes());
            store.commit(Map.of());
            store.put(new Windowed<>(k, new SessionWindow(20, 30)), "v2".getBytes());

            try (KeyValueIterator<Windowed<Bytes>, byte[]> it =
                     store.readOnly(IsolationLevel.READ_UNCOMMITTED).findSessions(k, 0L, 100L)) {
                int count = 0;
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                assertEquals(2, count);
            }
            try (KeyValueIterator<Windowed<Bytes>, byte[]> it =
                     store.readOnly(IsolationLevel.READ_COMMITTED).findSessions(k, 0L, 100L)) {
                int count = 0;
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                assertEquals(1, count);
            }
        } finally {
            store.close();
        }
    }

    @Test
    public void readOnlyCommittedFindSessionsShouldNotRaceConcurrentCommit() throws InterruptedException {
        // A READ_COMMITTED scan from a non-owner (IQ) thread must observe a point-in-time snapshot of
        // the committed sessions, taken under the buffer's snapshot read-lock. Without it the view
        // iterated the live end-time map while commit()'s write-lock-guarded flushToBase() mutated it,
        // exposing a half-applied commit. Each commit flips every session to a single generation, so a
        // correct snapshot scan must see one uniform generation across the full session set.
        final InMemorySessionStore store = openTransactionalSessionStore();
        final Bytes k = Bytes.wrap("k".getBytes());
        final int numSessions = 50;
        final int rounds = 300;
        final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);

        try {
            for (int i = 0; i < numSessions; i++) {
                store.put(new Windowed<>(k, new SessionWindow(i * 100L, i * 100L + 10L)), "gen0".getBytes());
            }
            store.commit(Map.of());

            final Thread reader = new Thread(() -> {
                final ReadOnlySessionStore<Bytes, byte[]> committed = store.readOnly(IsolationLevel.READ_COMMITTED);
                try {
                    while (!stop.get()) {
                        try (KeyValueIterator<Windowed<Bytes>, byte[]> it =
                                 committed.findSessions(k, 0L, numSessions * 100L)) {
                            String generation = null;
                            int count = 0;
                            while (it.hasNext()) {
                                final String value = new String(it.next().value);
                                if (generation == null) {
                                    generation = value;
                                } else {
                                    assertEquals(generation, value, "scan observed a partial commit");
                                }
                                count++;
                            }
                            assertEquals(numSessions, count, "scan observed a partial commit");
                        }
                    }
                } catch (final Throwable t) {
                    readerFailure.set(t);
                }
            }, "iq-reader");
            reader.start();

            for (int round = 1; round <= rounds && readerFailure.get() == null; round++) {
                final String generation = "gen" + round;
                for (int i = 0; i < numSessions; i++) {
                    store.put(new Windowed<>(k, new SessionWindow(i * 100L, i * 100L + 10L)), generation.getBytes());
                }
                store.commit(Map.of());
            }
            stop.set(true);
            reader.join(TimeUnit.SECONDS.toMillis(30));

            if (readerFailure.get() != null) {
                throw new AssertionError("READ_COMMITTED findSessions raced a concurrent commit", readerFailure.get());
            }
        } finally {
            stop.set(true);
            store.close();
        }
    }

    @Test
    public void shouldReportUncommittedPositionForTransactionalStore() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.setProperty(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, "true");
        final InternalMockProcessorContext<Bytes, byte[]> ctx = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            new Serdes.BytesSerde(),
            new Serdes.ByteArraySerde(),
            new StreamsConfig(props)
        );
        final InMemorySessionStore store = new InMemorySessionStore(
            "txn-pos-session-store", RETENTION_PERIOD, "scope");
        store.init(ctx, store);
        try {
            ctx.setRecordContext(new ProcessorRecordContext(0, 1, 0, "topic", new RecordHeaders()));
            store.put(new Windowed<>(Bytes.wrap("k".getBytes()), new SessionWindow(0, 10)), "v".getBytes());

            final Position expected = Position.fromMap(mkMap(mkEntry("topic", mkMap(mkEntry(0, 1L)))));

            // READ_UNCOMMITTED query should expose the staged position before commit
            final QueryResult<?> uncommitted = store.query(
                WindowRangeQuery.withKey(Bytes.wrap("k".getBytes())),
                PositionBound.unbounded(),
                new QueryConfig(false));
            assertEquals(expected, uncommitted.getPosition(), "READ_UNCOMMITTED query position");

            // getPosition() reports the uncommitted (committed + staged) position, mirroring
            // RocksDBStore, so the changelog consistency vector reflects the staged write.
            assertEquals(expected, store.getPosition(), "getPosition before commit (uncommitted)");

            // after commit, committed position populated
            store.commit(Map.of());
            assertEquals(expected, store.getPosition(), "getPosition after commit");
        } finally {
            store.close();
        }
    }

    private InMemorySessionStore openTransactionalSessionStore() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.setProperty(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, "true");
        final InternalMockProcessorContext<Bytes, byte[]> ctx = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            new Serdes.BytesSerde(),
            new Serdes.ByteArraySerde(),
            new StreamsConfig(props)
        );
        final InMemorySessionStore store = new InMemorySessionStore(
            "txn-in-memory-session-store", RETENTION_PERIOD, "scope");
        store.init(ctx, store);
        return store;
    }

}