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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ErrorHandlerContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogAndContinueOnInvalidProcessor;
import org.apache.kafka.streams.processor.internals.testutil.LogAndContinueOnInvalidPunctuate;
import org.apache.kafka.streams.processor.internals.testutil.LogAndFailOnInvalidProcessor;
import org.apache.kafka.streams.processor.internals.testutil.LogAndFailOnInvalidPunctuate;
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
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.ProcessorContextImpl.BYTEARRAY_VALUE_SERIALIZER;
import static org.apache.kafka.streams.processor.internals.ProcessorContextImpl.BYTES_KEY_SERIALIZER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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

    private boolean flushExecuted;
    private boolean putExecuted;
    private boolean putWithTimestampExecuted;
    private boolean putIfAbsentExecuted;
    private boolean putAllExecuted;
    private boolean deleteExecuted;
    private boolean removeExecuted;

    @Mock
    private KeyValueIterator<String, Long> rangeIter;
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<Long>> timestampedRangeIter;
    @Mock
    private KeyValueIterator<String, Long> allIter;
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<Long>> timestampedAllIter;
    @Mock
    private WindowStoreIterator windowStoreIter;

    private final List<KeyValueIterator<Windowed<String>, Long>> iters = new ArrayList<>(7);
    private final List<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> timestampedIters = new ArrayList<>(7);


    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        flushExecuted = false;
        putExecuted = false;
        putIfAbsentExecuted = false;
        putAllExecuted = false;
        deleteExecuted = false;
        removeExecuted = false;

        for (int i = 0; i < 7; i++) {
            iters.add(i, mock(KeyValueIterator.class));
            timestampedIters.add(i, mock(KeyValueIterator.class));
        }

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);

        when(stateManager.getGlobalStore(anyString())).thenReturn(null);
        when(stateManager.getGlobalStore("GlobalKeyValueStore")).thenAnswer(answer -> keyValueStoreMock());
        when(stateManager.getGlobalStore("GlobalTimestampedKeyValueStore")).thenAnswer(answer -> timestampedKeyValueStoreMock());
        when(stateManager.getGlobalStore("GlobalWindowStore")).thenAnswer(answer -> windowStoreMock());
        when(stateManager.getGlobalStore("GlobalTimestampedWindowStore")).thenAnswer(answer -> timestampedWindowStoreMock());
        when(stateManager.getGlobalStore("GlobalSessionStore")).thenAnswer(answer -> sessionStoreMock());
        when(stateManager.getStore("LocalKeyValueStore")).thenAnswer(answer -> keyValueStoreMock());
        when(stateManager.getStore("LocalTimestampedKeyValueStore")).thenAnswer(answer -> timestampedKeyValueStoreMock());
        when(stateManager.getStore("LocalWindowStore")).thenAnswer(answer -> windowStoreMock());
        when(stateManager.getStore("LocalTimestampedWindowStore")).thenAnswer(answer -> timestampedWindowStoreMock());
        when(stateManager.getStore("LocalSessionStore")).thenAnswer(answer -> sessionStoreMock());
        when(stateManager.registeredChangelogPartitionFor(REGISTERED_STORE_NAME)).thenReturn(CHANGELOG_PARTITION);

        context = new ProcessorContextImpl(
            mock(TaskId.class),
            streamsConfig,
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );

        final StreamTask task = mock(StreamTask.class);
        when(task.streamTime()).thenReturn(STREAM_TIME);
        context.transitionToActive(task, null, null);

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

    private ProcessorContextImpl getStandbyContext() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        when(stateManager.taskType()).thenReturn(TaskType.STANDBY);
        return new ProcessorContextImpl(
            mock(TaskId.class),
            streamsConfig,
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );
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
        final StreamTask task = mock(StreamTask.class);

        context.transitionToActive(task, recordCollector, null);
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
        final Position position = Position.emptyPosition();
        final Headers headers = new RecordHeaders();
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position).array()));

        final StreamTask task = mock(StreamTask.class);

        context = new ProcessorContextImpl(
                mock(TaskId.class),
                streamsConfigWithConsistencyMock(),
                stateManager,
                mock(StreamsMetricsImpl.class),
                mock(ThreadCache.class)
        );

        context.transitionToActive(task, recordCollector, null);
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
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.logChange("Store", Bytes.wrap("k".getBytes()), null, 0L, Position.emptyPosition())
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnGetStateStore() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.getStateStore("store")
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnForward() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.forward("key", "value")
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnForwardWithTo() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.forward("key", "value", To.child("child-name"))
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnCommit() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.commit()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnSchedule() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, t -> { })
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnTopic() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.topic()
        );
    }
    @Test
    public void shouldThrowUnsupportedOperationExceptionOnPartition() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.partition()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnOffset() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.offset()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnTimestamp() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.timestamp()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnCurrentNode() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.currentNode()
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnSetRecordContext() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.setRecordContext(mock(ProcessorRecordContext.class))
        );
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnRecordContext() {
        context = getStandbyContext();
        assertThrows(
            UnsupportedOperationException.class,
            () -> context.recordContext()
        );
    }

    @Test
    public void shouldMatchStreamTime() {
        assertEquals(STREAM_TIME, context.currentStreamTimeMs());
    }

    @Test
    public void shouldAddAndGetProcessorKeyValue() {
        context.addProcessorMetadataKeyValue("key1", 100L);
        final Long value = context.processorMetadataForKey("key1");
        assertEquals(100L, value.longValue());

        final Long noValue = context.processorMetadataForKey("nokey");
        assertNull(noValue);
    }

    @Test
    public void shouldSetAndGetProcessorMetaData() {
        final ProcessorMetadata emptyMetadata = new ProcessorMetadata();
        context.setProcessorMetadata(emptyMetadata);
        assertEquals(emptyMetadata, context.getProcessorMetadata());

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

    @Test
    public void shouldContinueOnProcessingExceptions() {
        when(streamsConfig.processingExceptionHandler()).thenReturn(new ProcessingExceptionHandler() {
            @Override
            public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
                assertArrayEquals(new byte[] {107, 101, 121}, context.sourceRawKey());
                assertArrayEquals(new byte[] {118, 97, 108, 117, 101}, context.sourceRawValue());
                assertEquals("fakeChild", context.processorNodeId());
                assertEquals("key", record.key());
                assertEquals("value", record.value());
                assertEquals("Exception should be handled by processing exception handler", exception.getMessage());

                return ProcessingHandlerResponse.CONTINUE;
            }

            @Override
            public void configure(final Map<String, ?> configs) {
                // No-op
            }
        });

        context = new ProcessorContextImpl(
            mock(TaskId.class),
            streamsConfig,
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );

        final StreamTask task = mock(StreamTask.class);
        when(task.rawRecord()).thenReturn(new ConsumerRecord<>("topic", 0, 0, "key".getBytes(), "value".getBytes()));
        context.transitionToActive(task, null, null);

        final ProcessorNode<String, Long, Object, Object> processorNode = new ProcessorNode<>(
            "fake",
            (org.apache.kafka.streams.processor.api.Processor<String, Long, Object, Object>) null,
            Collections.emptySet()
        );

        final ProcessorNode<Object, Object, Object, Object> childProcessorNode = new ProcessorNode<>(
            "fakeChild",
            (Processor<Object, Object, Object, Object>) record -> {
                throw new RuntimeException("Exception should be handled by processing exception handler");
            },
            Collections.emptySet()
        );

        processorNode.init(context);
        childProcessorNode.init(context);
        processorNode.addChild(childProcessorNode);

        context.setCurrentNode(processorNode);

        context.forward("key", "value");
    }

    @Test
    public void shouldFailOnProcessingExceptions() {
        when(streamsConfig.processingExceptionHandler()).thenReturn(new ProcessingExceptionHandler() {
            @Override
            public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
                assertArrayEquals(new byte[] {107, 101, 121}, context.sourceRawKey());
                assertArrayEquals(new byte[] {118, 97, 108, 117, 101}, context.sourceRawValue());
                assertEquals("fakeChild", context.processorNodeId());
                assertEquals("key", record.key());
                assertEquals("value", record.value());
                assertEquals("Exception should be handled by processing exception handler", exception.getMessage());

                return ProcessingHandlerResponse.FAIL;
            }

            @Override
            public void configure(final Map<String, ?> configs) {
                // No-op
            }
        });

        context = new ProcessorContextImpl(
            mock(TaskId.class),
            streamsConfig,
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );

        final StreamTask task = mock(StreamTask.class);
        when(task.rawRecord()).thenReturn(new ConsumerRecord<>("topic", 0, 0, "key".getBytes(), "value".getBytes()));
        context.transitionToActive(task, null, null);

        final ProcessorNode<String, Long, Object, Object> processorNode = new ProcessorNode<>(
            "fake",
            (org.apache.kafka.streams.processor.api.Processor<String, Long, Object, Object>) null,
            Collections.emptySet()
        );

        final ProcessorNode<Object, Object, Object, Object> childProcessorNode = new ProcessorNode<>(
            "fakeChild",
            (Processor<Object, Object, Object, Object>) record -> {
                throw new RuntimeException("Exception should be handled by processing exception handler");
            },
            Collections.emptySet()
        );

        processorNode.init(context);
        childProcessorNode.init(context);
        processorNode.addChild(childProcessorNode);

        context.setCurrentNode(processorNode);

        final StreamsException exception = assertThrows(StreamsException.class, () -> context.forward("key", "value"));
        assertEquals("Processing exception handler is set to fail upon a processing error. "
            + "If you would rather have the streaming pipeline continue after a processing error, "
            + "please set the processing.exception.handler appropriately.", exception.getMessage());
    }

    @Test
    public void shouldContinueInProcessorOnProcessingRecordAtBeginningExceptions() {

        final int[] expectedKeys = {0, 1, 2, 10};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, String, String> processorSupplier =
                () -> new Processor<Number, Number, String, String>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        int value = 100 / (10 * record.value().intValue());
                        context.forward(new Record<>(record.key().toString(), Integer.toString(value), record.timestamp()));
                    }

                    @Override
                    public void close() {
                    }
                };

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndContinueOnInvalidProcessor.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey, 0);
            }

            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>("1", "10", 0),
                    new KeyValueTimestamp<>("2", "5", 0),
                    new KeyValueTimestamp<>("10", "1", 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @Test
    public void shouldContinueInProcessorOnProcessingRecordInMiddleExceptions() {

        final int[] expectedKeys = {1, 0, 2, 10};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, String, String> processorSupplier =
                () -> new Processor<Number, Number, String, String>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        int value = 100 / (10 * record.value().intValue());
                        context.forward(new Record<>(record.key().toString(), Integer.toString(value), record.timestamp()));
                    }

                    @Override
                    public void close() {
                    }
                };

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndContinueOnInvalidProcessor.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey, 0);
            }

            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>("1", "10", 0),
                    new KeyValueTimestamp<>("2", "5", 0),
                    new KeyValueTimestamp<>("10", "1", 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @Test
    public void shouldContinueInProcessorOnProcessingRecordAtEndExceptions() {

        final int[] expectedKeys = {1, 2, 10, 0};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, String, String> processorSupplier =
                () -> new Processor<Number, Number, String, String>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        int value = 100 / (10 * record.value().intValue());
                        context.forward(new Record<>(record.key().toString(), Integer.toString(value), record.timestamp()));
                    }

                    @Override
                    public void close() {
                    }
                };


        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndContinueOnInvalidProcessor.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey, 0);
            }

            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>("1", "10", 0),
                    new KeyValueTimestamp<>("2", "5", 0),
                    new KeyValueTimestamp<>("10", "1", 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @Test
    public void shouldFailInProcessorOnProcessingRecordAtBeginningExceptions() {

        final int[] expectedKeys = {0, 1, 2, 10};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, String, String> processorSupplier =
                () -> new Processor<Number, Number, String, String>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        int value = 100 / (10 * record.value().intValue());
                        context.forward(new Record<>(record.key().toString(), Integer.toString(value), record.timestamp()));
                    }

                    @Override
                    public void close() {
                    }
                };

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndFailOnInvalidProcessor.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            try {
                for (final int expectedKey : expectedKeys) {
                    inputTopic.pipeInput(expectedKey, expectedKey, 0);
                }
            } catch (Exception exception) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                exception.printStackTrace(pw);
                assertTrue(sw.toString().contains("java.lang.ArithmeticException: / by zero"));
            }
            final KeyValueTimestamp[] expected = {};
            assertEquals(0, processor.theCapturedProcessor().processed().size());
        }
    }

    @Test
    public void shouldFailInProcessorOnProcessingRecordInMiddleExceptions() {

        final int[] expectedKeys = {1, 0, 2, 10};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, String, String> processorSupplier =
                () -> new Processor<Number, Number, String, String>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        int value = 100 / (10 * record.value().intValue());
                        context.forward(new Record<>(record.key().toString(), Integer.toString(value), record.timestamp()));
                    }

                    @Override
                    public void close() {
                    }
                };

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndFailOnInvalidProcessor.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            try {
                for (final int expectedKey : expectedKeys) {
                    inputTopic.pipeInput(expectedKey, expectedKey, 0);
                }
            } catch (Exception exception) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                exception.printStackTrace(pw);
                assertTrue(sw.toString().contains("java.lang.ArithmeticException: / by zero"));
            }
            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>("1", "10", 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @Test
    public void shouldFailInProcessorOnProcessingRecordAtEndExceptions() {

        final int[] expectedKeys = {1, 2, 10, 0};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, String, String> processorSupplier =
                () -> new Processor<Number, Number, String, String>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        int value = 100 / (10 * record.value().intValue());
                        context.forward(new Record<>(record.key().toString(), Integer.toString(value), record.timestamp()));
                    }

                    @Override
                    public void close() {
                    }
                };


        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndFailOnInvalidProcessor.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            try {
                for (final int expectedKey : expectedKeys) {
                    inputTopic.pipeInput(expectedKey, expectedKey, 0);
                }
            } catch (Exception exception) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                exception.printStackTrace(pw);
                assertTrue(sw.toString().contains("java.lang.ArithmeticException: / by zero"));
            }
            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>("1", "10", 0),
                    new KeyValueTimestamp<>("2", "5", 0),
                    new KeyValueTimestamp<>("10", "1", 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @Test
    public void shouldContinueOnPunctuateExceptions() {

        final int[] expectedKeys = {1, 2, 10};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, Number, Number> processorSupplier =
                () -> new Processor<Number, Number, Number, Number>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<Number, Number> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Number, Number> context) {
                        this.context = context;
                        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ts -> {
                            String zero = "0";
                            int i = 1 / Integer.parseInt(zero);
                        });
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        context.forward(record);
                    }

                    @Override
                    public void close() {
                    }
                };

        final MockProcessorSupplier<Number, Number, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndContinueOnInvalidPunctuate.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());
            for (final int expectedKey : expectedKeys) {
                driver.advanceWallClockTime(Duration.ofSeconds(2));
                inputTopic.pipeInput(expectedKey, expectedKey, 0);
            }

            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>(1, 1, 0),
                    new KeyValueTimestamp<>(2, 2, 0),
                    new KeyValueTimestamp<>(10, 10, 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @Test
    public void shouldFailOnPunctuateExceptions() {

        final int[] expectedKeys = {0, 1, 2, 10};

        final StreamsBuilder builder = new StreamsBuilder();

        final ProcessorSupplier<Number, Number, Number, Number> processorSupplier =
                () -> new Processor<Number, Number, Number, Number>() {

                    org.apache.kafka.streams.processor.api.ProcessorContext<Number, Number> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Number, Number> context) {
                        this.context = context;
                        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ts -> {
                            String zero = "0";
                            int i = 1 / Integer.parseInt(zero);
                        });
                    }

                    @Override
                    public void process(Record<Number, Number> record) {
                        context.forward(record);
                    }

                    @Override
                    public void close() {
                    }
                };

        final MockProcessorSupplier<Number, Number, Void, Void> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream("TOPIC_NAME", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.process(processorSupplier).process(processor);

        Properties properties = new Properties();
        properties.put("processing.exception.handler", LogAndFailOnInvalidPunctuate.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), properties,
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic("TOPIC_NAME", new IntegerSerializer(), new IntegerSerializer());

            try {
                inputTopic.pipeInput(expectedKeys[0], expectedKeys[0], 0);
                inputTopic.pipeInput(expectedKeys[1], expectedKeys[1], 0);
                driver.advanceWallClockTime(Duration.ofSeconds(2));
                inputTopic.pipeInput(expectedKeys[2], expectedKeys[2], 0);

            } catch (Exception exception) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                exception.printStackTrace(pw);
                assertTrue(sw.toString().contains("java.lang.ArithmeticException: / by zero"));
            }

            final KeyValueTimestamp[] expected = {
                    new KeyValueTimestamp<>(0, 0, 0),
                    new KeyValueTimestamp<>(1, 1, 0),
            };
            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }
    @SuppressWarnings("unchecked")
    private KeyValueStore<String, Long> keyValueStoreMock() {
        final KeyValueStore<String, Long> keyValueStoreMock = mock(KeyValueStore.class);

        initStateStoreMock(keyValueStoreMock);

        when(keyValueStoreMock.get(KEY)).thenReturn(VALUE);
        when(keyValueStoreMock.approximateNumEntries()).thenReturn(VALUE);

        when(keyValueStoreMock.range("one", "two")).thenReturn(rangeIter);
        when(keyValueStoreMock.all()).thenReturn(allIter);

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

        return keyValueStoreMock;
    }

    @SuppressWarnings("unchecked")
    private TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock() {
        final TimestampedKeyValueStore<String, Long> timestampedKeyValueStoreMock = mock(TimestampedKeyValueStore.class);

        initStateStoreMock(timestampedKeyValueStoreMock);

        when(timestampedKeyValueStoreMock.get(KEY)).thenReturn(VALUE_AND_TIMESTAMP);
        when(timestampedKeyValueStoreMock.approximateNumEntries()).thenReturn(VALUE);

        when(timestampedKeyValueStoreMock.range("one", "two")).thenReturn(timestampedRangeIter);
        when(timestampedKeyValueStoreMock.all()).thenReturn(timestampedAllIter);

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

        return timestampedKeyValueStoreMock;
    }

    @SuppressWarnings("unchecked")
    private WindowStore<String, Long> windowStoreMock() {
        final WindowStore<String, Long> windowStore = mock(WindowStore.class);

        initStateStoreMock(windowStore);

        when(windowStore.fetchAll(anyLong(), anyLong())).thenReturn(iters.get(0));
        when(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).thenReturn(iters.get(1));
        when(windowStore.fetch(anyString(), anyLong(), anyLong())).thenReturn(windowStoreIter);
        when(windowStore.fetch(anyString(), anyLong())).thenReturn(VALUE);
        when(windowStore.all()).thenReturn(iters.get(2));

        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).when(windowStore).put(anyString(), anyLong(), anyLong());

        return windowStore;
    }

    @SuppressWarnings("unchecked")
    private TimestampedWindowStore<String, Long> timestampedWindowStoreMock() {
        final TimestampedWindowStore<String, Long> windowStore = mock(TimestampedWindowStore.class);

        initStateStoreMock(windowStore);

        when(windowStore.fetchAll(anyLong(), anyLong())).thenReturn(timestampedIters.get(0));
        when(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).thenReturn(timestampedIters.get(1));
        when(windowStore.fetch(anyString(), anyLong(), anyLong())).thenReturn(windowStoreIter);
        when(windowStore.fetch(anyString(), anyLong())).thenReturn(VALUE_AND_TIMESTAMP);
        when(windowStore.all()).thenReturn(timestampedIters.get(2));

        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).doAnswer(answer -> {
            putWithTimestampExecuted = true;
            return null;
        }).when(windowStore).put(anyString(), any(ValueAndTimestamp.class), anyLong());

        return windowStore;
    }

    @SuppressWarnings("unchecked")
    private SessionStore<String, Long> sessionStoreMock() {
        final SessionStore<String, Long> sessionStore = mock(SessionStore.class);

        initStateStoreMock(sessionStore);

        when(sessionStore.findSessions(anyString(), anyLong(), anyLong())).thenReturn(iters.get(3));
        when(sessionStore.findSessions(anyString(), anyString(), anyLong(), anyLong())).thenReturn(iters.get(4));
        when(sessionStore.fetch(anyString())).thenReturn(iters.get(5));
        when(sessionStore.fetch(anyString(), anyString())).thenReturn(iters.get(6));

        doAnswer(answer -> {
            putExecuted = true;
            return null;
        }).when(sessionStore).put(any(), any());

        doAnswer(answer -> {
            removeExecuted = true;
            return null;
        }).when(sessionStore).remove(any());

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

        doAnswer(answer -> {
            flushExecuted = true;
            return null;
        }).when(stateStore).flush();
    }

    private <T extends StateStore> void doTest(final String name, final Consumer<T> checker) {
        @SuppressWarnings("deprecation") final org.apache.kafka.streams.processor.Processor<String, Long> processor = new org.apache.kafka.streams.processor.Processor<String, Long>() {
            @Override
            public void init(final ProcessorContext context) {
                final T store = context.getStateStore(name);
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

        checkThrowsUnsupportedOperation(() -> store.init((StateStoreContext) null, null), "init()");
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
