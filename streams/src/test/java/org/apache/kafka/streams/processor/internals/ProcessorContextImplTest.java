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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProcessorContextImplTest {
    private ProcessorContextImpl context;

    private static final String KEY = "key";
    private static final long VALUE = 42L;
    private static final ValueAndTimestamp<Long> VALUE_AND_TIMESTAMP = ValueAndTimestamp.make(42L, 21L);
    private static final String STORE_NAME = "underlying-store";

    private boolean flushExecuted;
    private boolean putExecuted;
    private boolean putWithTimestampExecuted;
    private boolean putIfAbsentExecuted;
    private boolean putAllExecuted;
    private boolean deleteExecuted;
    private boolean removeExecuted;

    private KeyValueIterator<String, Long> rangeIter;
    private KeyValueIterator<String, ValueAndTimestamp<Long>> timestampedRangeIter;
    private KeyValueIterator<String, Long> allIter;
    private KeyValueIterator<String, ValueAndTimestamp<Long>> timestampedAllIter;

    private final List<KeyValueIterator<Windowed<String>, Long>> iters = new ArrayList<>(7);
    private final List<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> timestampedIters = new ArrayList<>(7);
    private WindowStoreIterator windowStoreIter;

    @Before
    public void setup() {
        flushExecuted = false;
        putExecuted = false;
        putIfAbsentExecuted = false;
        putAllExecuted = false;
        deleteExecuted = false;
        removeExecuted = false;

        rangeIter = mock(KeyValueIterator.class);
        timestampedRangeIter = mock(KeyValueIterator.class);
        allIter = mock(KeyValueIterator.class);
        timestampedAllIter = mock(KeyValueIterator.class);
        windowStoreIter = mock(WindowStoreIterator.class);

        for (int i = 0; i < 7; i++) {
            iters.add(i, mock(KeyValueIterator.class));
            timestampedIters.add(i, mock(KeyValueIterator.class));
        }

        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        expect(stateManager.getGlobalStore("GlobalKeyValueStore")).andReturn(keyValueStoreMock());
        expect(stateManager.getGlobalStore("GlobalTimestampedKeyValueStore")).andReturn(timestampedKeyValueStoreMock());
        expect(stateManager.getGlobalStore("GlobalWindowStore")).andReturn(windowStoreMock());
        expect(stateManager.getGlobalStore("GlobalTimestampedWindowStore")).andReturn(timestampedWindowStoreMock());
        expect(stateManager.getGlobalStore("GlobalSessionStore")).andReturn(sessionStoreMock());
        expect(stateManager.getGlobalStore(anyString())).andReturn(null);

        expect(stateManager.getStore("LocalKeyValueStore")).andReturn(keyValueStoreMock());
        expect(stateManager.getStore("LocalTimestampedKeyValueStore")).andReturn(timestampedKeyValueStoreMock());
        expect(stateManager.getStore("LocalWindowStore")).andReturn(windowStoreMock());
        expect(stateManager.getStore("LocalTimestampedWindowStore")).andReturn(timestampedWindowStoreMock());
        expect(stateManager.getStore("LocalSessionStore")).andReturn(sessionStoreMock());

        replay(stateManager);

        context = new ProcessorContextImpl(
            mock(TaskId.class),
            mock(StreamTask.class),
            streamsConfig,
            mock(RecordCollector.class),
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );

        context.setCurrentNode(new ProcessorNode<String, Long>("fake", null,
            new HashSet<>(asList(
                "LocalKeyValueStore",
                "LocalTimestampedKeyValueStore",
                "LocalWindowStore",
                "LocalTimestampedWindowStore",
                "LocalSessionStore"))));
    }

    @Test
    public void globalKeyValueStoreShouldBeReadOnly() {
        doTest("GlobalKeyValueStore", (Consumer<KeyValueStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.put("1", 1L), "put()");
            checkThrowsUnsupportedOperation(() -> store.putIfAbsent("1", 1L), "putIfAbsent()");
            checkThrowsUnsupportedOperation(() -> store.putAll(Collections.emptyList()), "putAll()");
            checkThrowsUnsupportedOperation(() -> store.delete("1"), "delete()");

            assertEquals((Long) VALUE, store.get(KEY));
            assertEquals(rangeIter, store.range("one", "two"));
            assertEquals(allIter, store.all());
            assertEquals(VALUE, store.approximateNumEntries());
        });
    }

    @Test
    public void globalTimestampedKeyValueStoreShouldBeReadOnly() {
        doTest("GlobalTimestampedKeyValueStore", (Consumer<TimestampedKeyValueStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.put("1", ValueAndTimestamp.make(1L, 2L)), "put()");
            checkThrowsUnsupportedOperation(() -> store.putIfAbsent("1", ValueAndTimestamp.make(1L, 2L)), "putIfAbsent()");
            checkThrowsUnsupportedOperation(() -> store.putAll(Collections.emptyList()), "putAll()");
            checkThrowsUnsupportedOperation(() -> store.delete("1"), "delete()");

            assertEquals(VALUE_AND_TIMESTAMP, store.get(KEY));
            assertEquals(timestampedRangeIter, store.range("one", "two"));
            assertEquals(timestampedAllIter, store.all());
            assertEquals(VALUE, store.approximateNumEntries());
        });
    }

    @Test
    public void globalWindowStoreShouldBeReadOnly() {
        doTest("GlobalWindowStore", (Consumer<WindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.put("1", 1L, 1L), "put()");
            checkThrowsUnsupportedOperation(() -> store.put("1", 1L), "put()");

            assertEquals(iters.get(0), store.fetchAll(0L, 0L));
            assertEquals(windowStoreIter, store.fetch(KEY, 0L, 1L));
            assertEquals(iters.get(1), store.fetch(KEY, KEY, 0L, 1L));
            assertEquals((Long) VALUE, store.fetch(KEY, 1L));
            assertEquals(iters.get(2), store.all());
        });
    }

    @Test
    public void globalTimestampedWindowStoreShouldBeReadOnly() {
        doTest("GlobalTimestampedWindowStore", (Consumer<TimestampedWindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.put("1", ValueAndTimestamp.make(1L, 1L), 1L), "put() [with timestamp]");
            checkThrowsUnsupportedOperation(() -> store.put("1", ValueAndTimestamp.make(1L, 1L)), "put() [no timestamp]");

            assertEquals(timestampedIters.get(0), store.fetchAll(0L, 0L));
            assertEquals(windowStoreIter, store.fetch(KEY, 0L, 1L));
            assertEquals(timestampedIters.get(1), store.fetch(KEY, KEY, 0L, 1L));
            assertEquals(VALUE_AND_TIMESTAMP, store.fetch(KEY, 1L));
            assertEquals(timestampedIters.get(2), store.all());
        });
    }

    @Test
    public void globalSessionStoreShouldBeReadOnly() {
        doTest("GlobalSessionStore", (Consumer<SessionStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.remove(null), "remove()");
            checkThrowsUnsupportedOperation(() -> store.put(null, null), "put()");

            assertEquals(iters.get(3), store.findSessions(KEY, 1L, 2L));
            assertEquals(iters.get(4), store.findSessions(KEY, KEY, 1L, 2L));
            assertEquals(iters.get(5), store.fetch(KEY));
            assertEquals(iters.get(6), store.fetch(KEY, KEY));
        });
    }

    @Test
    public void localKeyValueStoreShouldNotAllowInitOrClose() {
        doTest("LocalKeyValueStore", (Consumer<KeyValueStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.put("1", 1L);
            assertTrue(putExecuted);

            store.putIfAbsent("1", 1L);
            assertTrue(putIfAbsentExecuted);

            store.putAll(Collections.emptyList());
            assertTrue(putAllExecuted);

            store.delete("1");
            assertTrue(deleteExecuted);

            assertEquals((Long) VALUE, store.get(KEY));
            assertEquals(rangeIter, store.range("one", "two"));
            assertEquals(allIter, store.all());
            assertEquals(VALUE, store.approximateNumEntries());
        });
    }

    @Test
    public void localTimestampedKeyValueStoreShouldNotAllowInitOrClose() {
        doTest("LocalTimestampedKeyValueStore", (Consumer<TimestampedKeyValueStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.put("1", ValueAndTimestamp.make(1L, 2L));
            assertTrue(putExecuted);

            store.putIfAbsent("1", ValueAndTimestamp.make(1L, 2L));
            assertTrue(putIfAbsentExecuted);

            store.putAll(Collections.emptyList());
            assertTrue(putAllExecuted);

            store.delete("1");
            assertTrue(deleteExecuted);

            assertEquals(VALUE_AND_TIMESTAMP, store.get(KEY));
            assertEquals(timestampedRangeIter, store.range("one", "two"));
            assertEquals(timestampedAllIter, store.all());
            assertEquals(VALUE, store.approximateNumEntries());
        });
    }

    @Test
    public void localWindowStoreShouldNotAllowInitOrClose() {
        doTest("LocalWindowStore", (Consumer<WindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.put("1", 1L);
            assertTrue(putExecuted);

            assertEquals(iters.get(0), store.fetchAll(0L, 0L));
            assertEquals(windowStoreIter, store.fetch(KEY, 0L, 1L));
            assertEquals(iters.get(1), store.fetch(KEY, KEY, 0L, 1L));
            assertEquals((Long) VALUE, store.fetch(KEY, 1L));
            assertEquals(iters.get(2), store.all());
        });
    }

    @Test
    public void localTimestampedWindowStoreShouldNotAllowInitOrClose() {
        doTest("LocalTimestampedWindowStore", (Consumer<TimestampedWindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.put("1", ValueAndTimestamp.make(1L, 1L));
            assertTrue(putExecuted);

            store.put("1", ValueAndTimestamp.make(1L, 1L), 1L);
            assertTrue(putWithTimestampExecuted);

            assertEquals(timestampedIters.get(0), store.fetchAll(0L, 0L));
            assertEquals(windowStoreIter, store.fetch(KEY, 0L, 1L));
            assertEquals(timestampedIters.get(1), store.fetch(KEY, KEY, 0L, 1L));
            assertEquals(VALUE_AND_TIMESTAMP, store.fetch(KEY, 1L));
            assertEquals(timestampedIters.get(2), store.all());
        });
    }

    @Test
    public void localSessionStoreShouldNotAllowInitOrClose() {
        doTest("LocalSessionStore", (Consumer<SessionStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.remove(null);
            assertTrue(removeExecuted);

            store.put(null, null);
            assertTrue(putExecuted);

            assertEquals(iters.get(3), store.findSessions(KEY, 1L, 2L));
            assertEquals(iters.get(4), store.findSessions(KEY, KEY, 1L, 2L));
            assertEquals(iters.get(5), store.fetch(KEY));
            assertEquals(iters.get(6), store.fetch(KEY, KEY));
        });
    }

    @SuppressWarnings("unchecked")
    private KeyValueStore<String, Long> keyValueStoreMock() {
        final KeyValueStore<String, Long> keyValueStoreMock = mock(KeyValueStore.class);

        initStateStoreMock(keyValueStoreMock);

        expect(keyValueStoreMock.get(KEY)).andReturn(VALUE);
        expect(keyValueStoreMock.approximateNumEntries()).andReturn(VALUE);

        expect(keyValueStoreMock.range("one", "two")).andReturn(rangeIter);
        expect(keyValueStoreMock.all()).andReturn(allIter);


        keyValueStoreMock.put(anyString(), anyLong());
        expectLastCall().andAnswer(() -> {
            putExecuted = true;
            return null;
        });

        keyValueStoreMock.putIfAbsent(anyString(), anyLong());
        expectLastCall().andAnswer(() -> {
            putIfAbsentExecuted = true;
            return null;
        });

        keyValueStoreMock.putAll(anyObject(List.class));
        expectLastCall().andAnswer(() -> {
            putAllExecuted = true;
            return null;
        });

        keyValueStoreMock.delete(anyString());
        expectLastCall().andAnswer(() -> {
            deleteExecuted = true;
            return null;
        });

        replay(keyValueStoreMock);

        return keyValueStoreMock;
    }

    @SuppressWarnings("unchecked")
    private TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock() {
        final TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock = mock(TimestampedKeyValueStore.class);

        initStateStoreMock(timestampedKeyValueStoreMock);

        expect(timestampedKeyValueStoreMock.get(KEY)).andReturn(VALUE_AND_TIMESTAMP);
        expect(timestampedKeyValueStoreMock.approximateNumEntries()).andReturn(VALUE);

        expect(timestampedKeyValueStoreMock.range("one", "two")).andReturn(timestampedRangeIter);
        expect(timestampedKeyValueStoreMock.all()).andReturn(timestampedAllIter);


        timestampedKeyValueStoreMock.put(anyString(), anyObject(ValueAndTimestamp.class));
        expectLastCall().andAnswer(() -> {
            putExecuted = true;
            return null;
        });

        timestampedKeyValueStoreMock.putIfAbsent(anyString(), anyObject(ValueAndTimestamp.class));
        expectLastCall().andAnswer(() -> {
            putIfAbsentExecuted = true;
            return null;
        });

        timestampedKeyValueStoreMock.putAll(anyObject(List.class));
        expectLastCall().andAnswer(() -> {
            putAllExecuted = true;
            return null;
        });

        timestampedKeyValueStoreMock.delete(anyString());
        expectLastCall().andAnswer(() -> {
            deleteExecuted = true;
            return null;
        });

        replay(timestampedKeyValueStoreMock);

        return timestampedKeyValueStoreMock;
    }

    @SuppressWarnings("unchecked")
    private WindowStore<String, Long> windowStoreMock() {
        final WindowStore<String, Long> windowStore = mock(WindowStore.class);

        initStateStoreMock(windowStore);

        expect(windowStore.fetchAll(anyLong(), anyLong())).andReturn(iters.get(0));
        expect(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).andReturn(iters.get(1));
        expect(windowStore.fetch(anyString(), anyLong(), anyLong())).andReturn(windowStoreIter);
        expect(windowStore.fetch(anyString(), anyLong())).andReturn(VALUE);
        expect(windowStore.all()).andReturn(iters.get(2));

        windowStore.put(anyString(), anyLong());
        expectLastCall().andAnswer(() -> {
            putExecuted = true;
            return null;
        });

        replay(windowStore);

        return windowStore;
    }

    @SuppressWarnings("unchecked")
    private TimestampedWindowStore<String, Long> timestampedWindowStoreMock() {
        final TimestampedWindowStore<String, Long> windowStore = mock(TimestampedWindowStore.class);

        initStateStoreMock(windowStore);

        expect(windowStore.fetchAll(anyLong(), anyLong())).andReturn(timestampedIters.get(0));
        expect(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).andReturn(timestampedIters.get(1));
        expect(windowStore.fetch(anyString(), anyLong(), anyLong())).andReturn(windowStoreIter);
        expect(windowStore.fetch(anyString(), anyLong())).andReturn(VALUE_AND_TIMESTAMP);
        expect(windowStore.all()).andReturn(timestampedIters.get(2));

        windowStore.put(anyString(), anyObject(ValueAndTimestamp.class));
        expectLastCall().andAnswer(() -> {
            putExecuted = true;
            return null;
        });

        windowStore.put(anyString(), anyObject(ValueAndTimestamp.class), anyLong());
        expectLastCall().andAnswer(() -> {
            putWithTimestampExecuted = true;
            return null;
        });

        replay(windowStore);

        return windowStore;
    }

    @SuppressWarnings("unchecked")
    private SessionStore<String, Long> sessionStoreMock() {
        final SessionStore<String, Long> sessionStore = mock(SessionStore.class);

        initStateStoreMock(sessionStore);

        expect(sessionStore.findSessions(anyString(), anyLong(), anyLong())).andReturn(iters.get(3));
        expect(sessionStore.findSessions(anyString(), anyString(), anyLong(), anyLong())).andReturn(iters.get(4));
        expect(sessionStore.fetch(anyString())).andReturn(iters.get(5));
        expect(sessionStore.fetch(anyString(), anyString())).andReturn(iters.get(6));

        sessionStore.put(anyObject(Windowed.class), anyLong());
        expectLastCall().andAnswer(() -> {
            putExecuted = true;
            return null;
        });

        sessionStore.remove(anyObject(Windowed.class));
        expectLastCall().andAnswer(() -> {
            removeExecuted = true;
            return null;
        });

        replay(sessionStore);

        return sessionStore;
    }

    private void initStateStoreMock(final StateStore stateStore) {
        expect(stateStore.name()).andReturn(STORE_NAME);
        expect(stateStore.persistent()).andReturn(true);
        expect(stateStore.isOpen()).andReturn(true);

        stateStore.flush();
        expectLastCall().andAnswer(() -> {
            flushExecuted = true;
            return null;
        });
    }

    private <T extends StateStore> void doTest(final String name, final Consumer<T> checker) {
        final Processor processor = new Processor<String, Long>() {
            @Override
            @SuppressWarnings("unchecked")
            public void init(final ProcessorContext context) {
                final T store = (T) context.getStateStore(name);
                checker.accept(store);
            }

            @Override
            public void process(final String k, final Long v) {
                //No-op.
            }

            @Override
            public void close() {
                //No-op.
            }
        };

        processor.init(context);
    }

    private void verifyStoreCannotBeInitializedOrClosed(final StateStore store) {
        assertEquals(STORE_NAME, store.name());
        assertTrue(store.persistent());
        assertTrue(store.isOpen());

        checkThrowsUnsupportedOperation(() -> store.init(null, null), "init()");
        checkThrowsUnsupportedOperation(store::close, "close()");
    }

    private void checkThrowsUnsupportedOperation(final Runnable check, final String name) {
        try {
            check.run();
            fail(name + " should throw exception");
        } catch (final UnsupportedOperationException e) {
            //ignore.
        }
    }
}
