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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.PositionSerde;
import org.apache.kafka.streams.state.internals.ThreadCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.ProcessorContextImpl.BYTEARRAY_VALUE_SERIALIZER;
import static org.apache.kafka.streams.processor.internals.ProcessorContextImpl.BYTES_KEY_SERIALIZER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ProcessorContextImplTest {
    private ProcessorContextImpl context;

    private final StreamsConfig streamsConfig = streamsConfigMock();

    @Mock
    private RecordCollector recordCollector;
    @Mock
    private ProcessorStateManager stateManager;

    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final long VALUE = 42L;
    private static final byte[] VALUE_BYTES = String.valueOf(VALUE).getBytes();
    private static final long TIMESTAMP = 21L;
    private static final long STREAM_TIME = 50L;
    private static final ValueAndTimestamp<Long> VALUE_AND_TIMESTAMP = ValueAndTimestamp.make(42L, 21L);
    private static final String STORE_NAME = "underlying-store";
    private static final String REGISTERED_STORE_NAME = "registered-store";
    private static final TopicPartition CHANGELOG_PARTITION = new TopicPartition("store-changelog", 1);

    private boolean flushExecuted = false;
    private boolean putExecuted = false;
    private boolean putWithTimestampExecuted;
    private boolean putIfAbsentExecuted = false;
    private boolean putAllExecuted = false;
    private boolean deleteExecuted = false;
    private boolean removeExecuted = false;

    @Mock
    private KeyValueIterator<String, Long> rangeIter;
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<Long>> timestampedRangeIter;
    @Mock
    private KeyValueIterator<String, Long> allIter;
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<Long>> timestampedAllIter;
    @SuppressWarnings("rawtypes")
    @Mock
    private WindowStoreIterator windowStoreIter;

    private final List<KeyValueIterator<Windowed<String>, Long>> iters = new ArrayList<>(7);
    private final List<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> timestampedIters = new ArrayList<>(7);

    private void foreachSetUp() {
        for (int i = 0; i < 7; i++) {
            iters.add(i, mock(KeyValueIterator.class));
            timestampedIters.add(i, mock(KeyValueIterator.class));
        }
    }

    private ProcessorContextImpl getStandbyContext() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        when(stateManager.taskType()).thenReturn(TaskType.STANDBY);
        return buildProcessorContextImpl(streamsConfig, stateManager);
    }

    @Test
    public void globalKeyValueStoreShouldBeReadOnly() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final KeyValueStore<String, Long> keyValueStoreMock = mock(KeyValueStore.class);
        when(stateManager.globalStore("GlobalKeyValueStore")).thenAnswer(answer -> keyValueStoreMock(keyValueStoreMock));

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();
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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock = mock(TimestampedKeyValueStore.class);
        when(stateManager.globalStore("GlobalTimestampedKeyValueStore")).thenAnswer(answer -> timestampedKeyValueStoreMock(timestampedKeyValueStoreMock));

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final WindowStore<String, Long> windowStore = mock(WindowStore.class);
        when(stateManager.globalStore("GlobalWindowStore")).thenAnswer(answer -> windowStoreMock(windowStore));

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        doTest("GlobalWindowStore", (Consumer<WindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.put("1", 1L, 1L), "put()");

            assertEquals(iters.get(0), store.fetchAll(0L, 0L));
            assertEquals(windowStoreIter, store.fetch(KEY, 0L, 1L));
            assertEquals(iters.get(1), store.fetch(KEY, KEY, 0L, 1L));
            assertEquals((Long) VALUE, store.fetch(KEY, 1L));
            assertEquals(iters.get(2), store.all());
        });
    }

    @Test
    public void globalTimestampedWindowStoreShouldBeReadOnly() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final TimestampedWindowStore<String, Long> windowStore = mock(TimestampedWindowStore.class);
        when(stateManager.globalStore("GlobalTimestampedWindowStore")).thenAnswer(answer -> timestampedWindowStoreMock(windowStore));

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        doTest("GlobalTimestampedWindowStore", (Consumer<TimestampedWindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::flush, "flush()");
            checkThrowsUnsupportedOperation(() -> store.put("1", ValueAndTimestamp.make(1L, 1L), 1L), "put() [with timestamp]");

            assertEquals(timestampedIters.get(0), store.fetchAll(0L, 0L));
            assertEquals(windowStoreIter, store.fetch(KEY, 0L, 1L));
            assertEquals(timestampedIters.get(1), store.fetch(KEY, KEY, 0L, 1L));
            assertEquals(VALUE_AND_TIMESTAMP, store.fetch(KEY, 1L));
            assertEquals(timestampedIters.get(2), store.all());
        });
    }

    @Test
    public void globalSessionStoreShouldBeReadOnly() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final SessionStore<String, Long> sessionStore = mock(SessionStore.class);
        when(stateManager.globalStore("GlobalSessionStore")).thenAnswer(answer -> sessionStoreMock(sessionStore));

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final KeyValueStore<String, Long> keyValueStoreMock = mock(KeyValueStore.class);
        when(stateManager.store("LocalKeyValueStore")).thenAnswer(answer -> keyValueStoreMock(keyValueStoreMock));
        mockStateStoreFlush(keyValueStoreMock);
        mockKeyValueStoreOperation(keyValueStoreMock);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock = mock(TimestampedKeyValueStore.class);
        when(stateManager.store("LocalTimestampedKeyValueStore"))
            .thenAnswer(answer -> timestampedKeyValueStoreMock(timestampedKeyValueStoreMock));
        mockTimestampedKeyValueOperation(timestampedKeyValueStoreMock);
        mockStateStoreFlush(timestampedKeyValueStoreMock);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final WindowStore<String, Long> windowStore = mock(WindowStore.class);
        when(stateManager.store("LocalWindowStore")).thenAnswer(answer -> windowStoreMock(windowStore));
        mockStateStoreFlush(windowStore);

        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).when(windowStore).put(anyString(), anyLong(), anyLong());

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        doTest("LocalWindowStore", (Consumer<WindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.put("1", 1L, 1L);
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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final TimestampedWindowStore<String, Long> windowStore = mock(TimestampedWindowStore.class);
        when(stateManager.store("LocalTimestampedWindowStore")).thenAnswer(answer -> timestampedWindowStoreMock(windowStore));
        mockStateStoreFlush(windowStore);

        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).doAnswer(answer -> {
            putWithTimestampExecuted = true;
            return null;
        }).when(windowStore).put(anyString(), any(ValueAndTimestamp.class), anyLong());

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        doTest("LocalTimestampedWindowStore", (Consumer<TimestampedWindowStore<String, Long>>) store -> {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.flush();
            assertTrue(flushExecuted);

            store.put("1", ValueAndTimestamp.make(1L, 1L), 1L);
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
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.globalStore(anyString())).thenReturn(null);

        final SessionStore<String, Long> sessionStore = mock(SessionStore.class);
        when(stateManager.store("LocalSessionStore")).thenAnswer(answer -> sessionStoreMock(sessionStore));
        mockStateStoreFlush(sessionStore);

        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).when(sessionStore).put(any(), any());

        doAnswer(answer -> {
            removeExecuted = true;
            return null;
        }).when(sessionStore).remove(any());

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

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

    @Test
    public void shouldNotSendRecordHeadersToChangelogTopic() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.registeredChangelogPartitionFor(REGISTERED_STORE_NAME)).thenReturn(CHANGELOG_PARTITION);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        final StreamTask task1 = mock(StreamTask.class);

        context.transitionToActive(task1, recordCollector, null);
        context.logChange(REGISTERED_STORE_NAME, KEY_BYTES, VALUE_BYTES, TIMESTAMP, Position.emptyPosition());

        verify(recordCollector).send(
            CHANGELOG_PARTITION.topic(),
            KEY_BYTES,
            VALUE_BYTES,
            null,
            CHANGELOG_PARTITION.partition(),
            TIMESTAMP,
            BYTES_KEY_SERIALIZER,
            BYTEARRAY_VALUE_SERIALIZER,
            null,
            null);
    }

    @Test
    public void shouldSendRecordHeadersToChangelogTopicWhenConsistencyEnabled() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.registeredChangelogPartitionFor(REGISTERED_STORE_NAME)).thenReturn(CHANGELOG_PARTITION);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        final Position position = Position.emptyPosition();
        final Headers headers = new RecordHeaders();
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
            PositionSerde.serialize(position).array()));

        final StreamTask task1 = mock(StreamTask.class);

        context = buildProcessorContextImpl(streamsConfigWithConsistencyMock(), stateManager);

        context.transitionToActive(task1, recordCollector, null);
        context.logChange(REGISTERED_STORE_NAME, KEY_BYTES, VALUE_BYTES, TIMESTAMP, position);

        verify(recordCollector).send(
            CHANGELOG_PARTITION.topic(),
            KEY_BYTES,
            VALUE_BYTES,
            headers,
            CHANGELOG_PARTITION.partition(),
            TIMESTAMP,
            BYTES_KEY_SERIALIZER,
            BYTEARRAY_VALUE_SERIALIZER,
            null,
            null);
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnLogChange() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.logChange("Store", Bytes.wrap("k".getBytes()), null, 0L, Position.emptyPosition())
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnGetStateStore() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.getStateStore("store")
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnForward() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.forward("key", "value")
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnForwardWithTo() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.forward("key", "value", To.child("child-name"))
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnCommit() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.commit()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnSchedule() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, t -> { })
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnTopic() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.topic()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnPartition() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.partition()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnOffset() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.offset()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnTimestamp() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.recordContext.timestamp()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnCurrentNode() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.currentNode()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnSetRecordContext() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.setRecordContext(mock(ProcessorRecordContext.class))
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnRecordContext() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.recordContext()
        );
    }

    @Test
    public void shouldMatchStreamTime() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        when(task.streamTime()).thenReturn(STREAM_TIME);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        assertEquals(STREAM_TIME, context.currentStreamTimeMs());
    }

    @Test
    public void shouldAddAndGetProcessorKeyValue() {
        foreachSetUp();

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        final StreamTask task = mock(StreamTask.class);
        context.transitionToActive(task, null, null);

        mockProcessorNodeWithLocalKeyValueStore();

        context.addProcessorMetadataKeyValue("key1", 100L);
        final Long value = context.processorMetadataForKey("key1");
        assertEquals(100L, value.longValue());

        final Long noValue = context.processorMetadataForKey("nokey");
        assertNull(noValue);
    }

    @Test
    public void shouldSetAndGetProcessorMetaData() {
        foreachSetUp();

        context = buildProcessorContextImpl(streamsConfig, stateManager);

        mockProcessorNodeWithLocalKeyValueStore();

        final ProcessorMetadata emptyMetadata = new ProcessorMetadata();
        context.setProcessorMetadata(emptyMetadata);
        assertEquals(emptyMetadata, context.processorMetadata());

        final ProcessorMetadata metadata = new ProcessorMetadata(
            mkMap(
                mkEntry("key1", 10L),
                mkEntry("key2", 100L)
            )
        );

        context.setProcessorMetadata(metadata);
        assertEquals(10L, context.processorMetadataForKey("key1").longValue());
        assertEquals(100L, context.processorMetadataForKey("key2").longValue());

        assertThrows(NullPointerException.class, () -> context.setProcessorMetadata(null));
    }

    private void mockProcessorNodeWithLocalKeyValueStore() {
        context.setCurrentNode(
            new ProcessorNode<>(
                "fake",
                (org.apache.kafka.streams.processor.api.Processor<String, Long, Object, Object>) null,
                new HashSet<>(
                    asList(
                        "LocalKeyValueStore",
                        "LocalTimestampedKeyValueStore",
                        "LocalWindowStore",
                        "LocalTimestampedWindowStore",
                        "LocalSessionStore"
                    )
                )
            )
        );
    }

    private ProcessorContextImpl buildProcessorContextImpl(final StreamsConfig streamsConfig, final ProcessorStateManager stateManager) {
        return new ProcessorContextImpl(
            mock(TaskId.class),
            streamsConfig,
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );
    }

    private KeyValueStore<String, Long> keyValueStoreMock(final KeyValueStore<String, Long> keyValueStoreMock) {
        initStateStoreMock(keyValueStoreMock);

        when(keyValueStoreMock.get(KEY)).thenReturn(VALUE);
        when(keyValueStoreMock.approximateNumEntries()).thenReturn(VALUE);

        when(keyValueStoreMock.range("one", "two")).thenReturn(rangeIter);
        when(keyValueStoreMock.all()).thenReturn(allIter);

        return keyValueStoreMock;
    }

    private void mockKeyValueStoreOperation(final KeyValueStore<String, Long> keyValueStoreMock) {
        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).when(keyValueStoreMock).put(anyString(), anyLong());

        doAnswer(answer -> {
            putIfAbsentExecuted = true;
            return null;
        }).when(keyValueStoreMock).putIfAbsent(anyString(), anyLong());

        doAnswer(answer -> {
            putAllExecuted = true;
            return null;
        }).when(keyValueStoreMock).putAll(any(List.class));

        doAnswer(answer -> {
            deleteExecuted = true;
            return null;
        }).when(keyValueStoreMock).delete(anyString());
    }

    private TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock(final TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock) {
        initStateStoreMock(timestampedKeyValueStoreMock);

        when(timestampedKeyValueStoreMock.get(KEY)).thenReturn(VALUE_AND_TIMESTAMP);
        when(timestampedKeyValueStoreMock.approximateNumEntries()).thenReturn(VALUE);

        when(timestampedKeyValueStoreMock.range("one", "two")).thenReturn(timestampedRangeIter);
        when(timestampedKeyValueStoreMock.all()).thenReturn(timestampedAllIter);

        return timestampedKeyValueStoreMock;
    }

    private void mockTimestampedKeyValueOperation(final TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock) {
        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).when(timestampedKeyValueStoreMock).put(anyString(), any(ValueAndTimestamp.class));

        doAnswer(answer -> {
            putIfAbsentExecuted = true;
            return null;
        }).when(timestampedKeyValueStoreMock).putIfAbsent(anyString(), any(ValueAndTimestamp.class));

        doAnswer(answer -> {
            putAllExecuted = true;
            return null;
        }).when(timestampedKeyValueStoreMock).putAll(any(List.class));

        doAnswer(answer -> {
            deleteExecuted = true;
            return null;
        }).when(timestampedKeyValueStoreMock).delete(anyString());
    }

    private WindowStore<String, Long> windowStoreMock(final WindowStore<String, Long> windowStore) {
        initStateStoreMock(windowStore);

        when(windowStore.fetchAll(anyLong(), anyLong())).thenReturn(iters.get(0));
        when(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).thenReturn(iters.get(1));
        when(windowStore.fetch(anyString(), anyLong(), anyLong())).thenReturn(windowStoreIter);
        when(windowStore.fetch(anyString(), anyLong())).thenReturn(VALUE);
        when(windowStore.all()).thenReturn(iters.get(2));

        return windowStore;
    }

    private TimestampedWindowStore<String, Long> timestampedWindowStoreMock(final TimestampedWindowStore<String, Long> windowStore) {
        initStateStoreMock(windowStore);

        when(windowStore.fetchAll(anyLong(), anyLong())).thenReturn(timestampedIters.get(0));
        when(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).thenReturn(timestampedIters.get(1));
        when(windowStore.fetch(anyString(), anyLong(), anyLong())).thenReturn(windowStoreIter);
        when(windowStore.fetch(anyString(), anyLong())).thenReturn(VALUE_AND_TIMESTAMP);
        when(windowStore.all()).thenReturn(timestampedIters.get(2));

        return windowStore;
    }

    private SessionStore<String, Long> sessionStoreMock(final SessionStore<String, Long> sessionStore) {
        initStateStoreMock(sessionStore);

        when(sessionStore.findSessions(anyString(), anyLong(), anyLong())).thenReturn(iters.get(3));
        when(sessionStore.findSessions(anyString(), anyString(), anyLong(), anyLong())).thenReturn(iters.get(4));
        when(sessionStore.fetch(anyString())).thenReturn(iters.get(5));
        when(sessionStore.fetch(anyString(), anyString())).thenReturn(iters.get(6));

        return sessionStore;
    }

    private StreamsConfig streamsConfigMock() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        when(streamsConfig.originals()).thenReturn(Collections.emptyMap());
        when(streamsConfig.values()).thenReturn(Collections.emptyMap());
        when(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("add-id");
        return streamsConfig;
    }

    private StreamsConfig streamsConfigWithConsistencyMock() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);

        final Map<String, Object> myValues = new HashMap<>();
        myValues.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        when(streamsConfig.originals()).thenReturn(myValues);
        when(streamsConfig.values()).thenReturn(Collections.emptyMap());
        when(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("add-id");
        return streamsConfig;
    }

    private void initStateStoreMock(final StateStore stateStore) {
        when(stateStore.name()).thenReturn(STORE_NAME);
        when(stateStore.persistent()).thenReturn(true);
        when(stateStore.isOpen()).thenReturn(true);
    }

    private void mockStateStoreFlush(final StateStore stateStore) {
        doAnswer(answer -> {
            flushExecuted = true;
            return null;
        }).when(stateStore).flush();
    }

    @SuppressWarnings("rawtypes")
    private <T extends StateStore> void doTest(final String name, final Consumer<T> checker) {
        final Processor<String, Long, String, Long> processor = new Processor<>() {
            @Override
            public void init(final ProcessorContext<String, Long> context) {
                final T store = context.getStateStore(name);
                checker.accept(store);
            }

            @Override
            public void process(final Record<String, Long> record) {
                //No-op.
            }

            @Override
            public void close() {
                //No-op.
            }
        };

        processor.init((ProcessorContext) context);
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
