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

import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRule;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.StoreChangelogReader.ChangelogReaderState.ACTIVE_RESTORING;
import static org.apache.kafka.streams.processor.internals.StoreChangelogReader.ChangelogReaderState.STANDBY_UPDATING;
import static org.apache.kafka.streams.processor.internals.Task.TaskType.ACTIVE;
import static org.apache.kafka.streams.processor.internals.Task.TaskType.STANDBY;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.resetToDefault;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class StoreChangelogReaderTest extends EasyMockSupport {

    @Rule
    public EasyMockRule rule = new EasyMockRule(this);

    @Mock(type = MockType.NICE)
    private ProcessorStateManager stateManager;
    @Mock(type = MockType.NICE)
    private ProcessorStateManager activeStateManager;
    @Mock(type = MockType.NICE)
    private ProcessorStateManager standbyStateManager;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadata;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadataOne;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadataTwo;
    @Mock(type = MockType.NICE)
    private StateStore store;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Object[] {STANDBY, ACTIVE};
    }

    @Parameterized.Parameter
    public Task.TaskType type;

    private final String storeName = "store";
    private final String topicName = "topic";
    private final LogContext logContext = new LogContext("test-reader ");
    private final TopicPartition tp = new TopicPartition(topicName, 0);
    private final TopicPartition tp1 = new TopicPartition("one", 0);
    private final TopicPartition tp2 = new TopicPartition("two", 0);
    private final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader"));
    private final MockTime time = new MockTime();
    private final MockStateRestoreListener callback = new MockStateRestoreListener();
    private final KafkaException kaboom = new KafkaException("KABOOM!");
    private final MockStateRestoreListener exceptionCallback = new MockStateRestoreListener() {
        @Override
        public void onRestoreStart(final TopicPartition tp, final String store, final long stOffset, final long edOffset) {
            throw kaboom;
        }

        @Override
        public void onBatchRestored(final TopicPartition tp, final String store, final long bedOffset, final long numRestored) {
            throw kaboom;
        }

        @Override
        public void onRestoreEnd(final TopicPartition tp, final String store, final long totalRestored) {
            throw kaboom;
        }
    };

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockAdminClient adminClient = new MockAdminClient();
    private final StoreChangelogReader changelogReader =
        new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

    @Before
    public void setUp() {
        EasyMock.expect(stateManager.storeMetadata(tp)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(stateManager.taskType()).andReturn(type).anyTimes();
        EasyMock.expect(activeStateManager.storeMetadata(tp)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(activeStateManager.taskType()).andReturn(ACTIVE).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(tp)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(standbyStateManager.taskType()).andReturn(STANDBY).anyTimes();

        EasyMock.expect(storeMetadata.changelogPartition()).andReturn(tp).anyTimes();
        EasyMock.expect(storeMetadata.store()).andReturn(store).anyTimes();
        EasyMock.expect(store.name()).andReturn(storeName).anyTimes();
    }

    @After
    public void tearDown() {
        EasyMock.reset(
            stateManager,
            activeStateManager,
            standbyStateManager,
            storeMetadata,
            storeMetadataOne,
            storeMetadataTwo,
            store
        );
    }

    @Test
    public void shouldNotRegisterSameStoreMultipleTimes() {
        EasyMock.replay(stateManager, storeMetadata);

        changelogReader.register(tp, stateManager);

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        assertThrows(IllegalStateException.class, () -> changelogReader.register(tp, stateManager));
    }

    @Test
    public void shouldNotRegisterStoreWithoutMetadata() {
        EasyMock.replay(stateManager, storeMetadata);

        assertThrows(IllegalStateException.class,
            () -> changelogReader.register(new TopicPartition("ChangelogWithoutStoreMetadata", 0), stateManager));
    }

    @Test
    public void shouldInitializeChangelogAndCheckForCompletion() {
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(9L).anyTimes();
        EasyMock.replay(mockTasks, stateManager, storeMetadata, store);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);
        changelogReader.restore(mockTasks);

        assertEquals(
            type == ACTIVE ?
                StoreChangelogReader.ChangelogState.COMPLETED :
                StoreChangelogReader.ChangelogState.RESTORING,
            changelogReader.changelogMetadata(tp).state()
        );
        assertEquals(type == ACTIVE ? 10L : null, changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(
            type == ACTIVE ? Collections.singleton(tp) : Collections.emptySet(),
            changelogReader.completedChangelogs()
        );
        assertEquals(10L, consumer.position(tp));
        assertEquals(Collections.singleton(tp), consumer.paused());

        if (type == ACTIVE) {
            assertEquals(tp, callback.restoreTopicPartition);
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
            assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
        }
    }

    @Test
    public void shouldTriggerRestoreListenerWithOffsetZeroIfPositionThrowsTimeoutException() {
        // restore listener is only triggered for active tasks
        if (type == ACTIVE) {
            final Map<TaskId, Task> mockTasks = mock(Map.class);
            EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
            EasyMock.expect(stateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
            EasyMock.replay(mockTasks, stateManager, storeMetadata, store);

            adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

            final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
                @Override
                public long position(final TopicPartition partition) {
                    throw new TimeoutException("KABOOM!");
                }
            };
            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));

            final StoreChangelogReader changelogReader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

            changelogReader.register(tp, stateManager);
            changelogReader.restore(mockTasks);

            assertThat(callback.restoreStartOffset, equalTo(0L));
        }
    }

    @Test
    public void shouldPollWithRightTimeout() {
        final TaskId taskId = new TaskId(0, 0);

        EasyMock.expect(storeMetadata.offset()).andReturn(null).andReturn(9L).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(stateManager.taskId()).andReturn(taskId);
        EasyMock.replay(stateManager, storeMetadata, store);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 11L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        if (type == ACTIVE) {
            assertEquals(Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)), consumer.lastPollTimeout());
        } else {
            assertEquals(Duration.ZERO, consumer.lastPollTimeout());
        }
    }

    @Test
    public void shouldPollWithRightTimeoutWithStateUpdater() {
        final TaskId taskId = new TaskId(0, 0);

        EasyMock.expect(storeMetadata.offset()).andReturn(null).andReturn(9L).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(stateManager.taskId()).andReturn(taskId);
        EasyMock.replay(stateManager, storeMetadata, store);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 11L));

        final Properties properties = new Properties();
        properties.put(InternalConfig.STATE_UPDATER_ENABLED, true);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader", properties));

        final StoreChangelogReader changelogReader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));
        assertEquals(Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)), consumer.lastPollTimeout());
    }

    @Test
    public void shouldRestoreFromPositionAndCheckForCompletion() {
        final TaskId taskId = new TaskId(0, 0);

        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(stateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(stateManager, storeMetadata, store);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertTrue(changelogReader.completedChangelogs().isEmpty());
        assertEquals(6L, consumer.position(tp));
        assertEquals(Collections.emptySet(), consumer.paused());

        if (type == ACTIVE) {
            assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());

            assertEquals(tp, callback.restoreTopicPartition);
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
            assertNull(callback.storeNameCalledStates.get(RESTORE_END));
            assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
        } else {
            assertNull(changelogReader.changelogMetadata(tp).endOffset());
        }

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        // beyond end records should be skipped even when there's gap at the end offset
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(12L, consumer.position(tp));

        if (type == ACTIVE) {
            assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
            assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());
            assertEquals(1, changelogReader.changelogMetadata(tp).bufferedRecords().size());
            assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
            assertEquals(Collections.singleton(tp), consumer.paused());

            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_BATCH));
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
        } else {
            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
            assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
            assertEquals(0, changelogReader.changelogMetadata(tp).bufferedRecords().size());
            assertEquals(Collections.emptySet(), changelogReader.completedChangelogs());
            assertEquals(Collections.emptySet(), consumer.paused());
        }
    }

    @Test
    public void shouldRestoreFromBeginningAndCheckCompletion() {
        final TaskId taskId = new TaskId(0, 0);

        EasyMock.expect(storeMetadata.offset()).andReturn(null).andReturn(9L).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(stateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(stateManager, storeMetadata, store);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 11L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(5L, consumer.position(tp));
        assertEquals(Collections.emptySet(), consumer.paused());

        if (type == ACTIVE) {
            assertEquals(11L, (long) changelogReader.changelogMetadata(tp).endOffset());

            assertEquals(tp, callback.restoreTopicPartition);
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
            assertNull(callback.storeNameCalledStates.get(RESTORE_END));
            assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
        } else {
            assertNull(changelogReader.changelogMetadata(tp).endOffset());
        }

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        // consumer position bypassing the gap in the next poll
        consumer.seek(tp, 11L);

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(11L, consumer.position(tp));
        assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());

        if (type == ACTIVE) {
            assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
            assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());
            assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
            assertEquals(Collections.singleton(tp), consumer.paused());

            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_BATCH));
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
        } else {
            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
            assertEquals(Collections.emptySet(), changelogReader.completedChangelogs());
            assertEquals(Collections.emptySet(), consumer.paused());
        }
    }

    @Test
    public void shouldCheckCompletionIfPositionLargerThanEndOffset() {
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.replay(mockTasks, activeStateManager, storeMetadata, store);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 0L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore(mockTasks);

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(0L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertEquals(6L, consumer.position(tp));
        assertEquals(Collections.singleton(tp), consumer.paused());
        assertEquals(tp, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @Test
    public void shouldRequestPositionAndHandleTimeoutException() {
        final TaskId taskId = new TaskId(0, 0);

        final Task mockTask = mock(Task.class);
        mockTask.clearTaskTimeout();
        mockTask.maybeInitTaskTimeoutOrThrow(anyLong(), anyObject());
        EasyMock.expectLastCall();
        EasyMock.expect(storeMetadata.offset()).andReturn(10L).anyTimes();
        EasyMock.expect(activeStateManager.changelogOffsets()).andReturn(singletonMap(tp, 10L));
        EasyMock.expect(activeStateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(mockTask, activeStateManager, storeMetadata, store);

        final AtomicBoolean clearException = new AtomicBoolean(false);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public long position(final TopicPartition partition) {
                if (clearException.get()) {
                    return 10L;
                } else {
                    throw new TimeoutException("KABOOM!");
                }
            }
        };

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertTrue(changelogReader.completedChangelogs().isEmpty());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        verify(mockTask);

        clearException.set(true);
        resetToDefault(mockTask);
        mockTask.clearTaskTimeout();
        EasyMock.expectLastCall();
        EasyMock.replay(mockTask);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertEquals(10L, consumer.position(tp));
        verify(mockTask);
    }

    @Test
    public void shouldThrowIfPositionFail() {
        final TaskId taskId = new TaskId(0, 0);
        EasyMock.expect(activeStateManager.taskId()).andReturn(taskId);
        EasyMock.expect(storeMetadata.offset()).andReturn(10L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public long position(final TopicPartition partition) {
                throw kaboom;
            }
        };

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, activeStateManager);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldRequestEndOffsetsAndHandleTimeoutException() {
        final TaskId taskId = new TaskId(0, 0);

        final Task mockTask = mock(Task.class);
        mockTask.maybeInitTaskTimeoutOrThrow(anyLong(), anyObject());
        EasyMock.expectLastCall();

        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(activeStateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(activeStateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(mockTask, activeStateManager, storeMetadata, store);

        final AtomicBoolean functionCalled = new AtomicBoolean(false);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public ListOffsetsResult listOffsets(final Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                                 final ListOffsetsOptions options) {
                if (functionCalled.get()) {
                    return super.listOffsets(topicPartitionOffsets, options);
                } else {
                    functionCalled.set(true);
                    throw new TimeoutException("KABOOM!");
                }
            }
        };
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new AssertionError("Should not trigger this function");
            }
        };

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertTrue(functionCalled.get());
        verify(mockTask);

        EasyMock.resetToDefault(mockTask);
        mockTask.clearTaskTimeout();
        EasyMock.replay(mockTask);

        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(6L, consumer.position(tp));
        verify(mockTask);
    }

    @Test
    public void shouldThrowIfEndOffsetsFail() {
        EasyMock.expect(storeMetadata.offset()).andReturn(10L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public ListOffsetsResult listOffsets(final Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                                 final ListOffsetsOptions options) {
                throw kaboom;
            }
        };
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 0L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, activeStateManager);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.emptyMap())
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldRequestCommittedOffsetsAndHandleTimeoutException() {
        final TaskId taskId = new TaskId(0, 0);

        final Task mockTask = mock(Task.class);
        if (type == ACTIVE) {
            mockTask.clearTaskTimeout();
        }
        mockTask.maybeInitTaskTimeoutOrThrow(anyLong(), anyObject());
        EasyMock.expectLastCall();

        EasyMock.expect(stateManager.changelogAsSource(tp)).andReturn(true).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(stateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(mockTask, stateManager, storeMetadata, store);

        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public synchronized ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, final ListConsumerGroupOffsetsOptions options) {
                if (functionCalled.get()) {
                    return super.listConsumerGroupOffsets(groupSpecs, options);
                } else {
                    functionCalled.set(true);
                    return AdminClientTestUtils.listConsumerGroupOffsetsResult(groupSpecs.keySet().iterator().next(), new TimeoutException("KABOOM!"));
                }
            }
        };

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 20L));
        adminClient.updateConsumerGroupOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(
            type == ACTIVE ?
                StoreChangelogReader.ChangelogState.REGISTERED :
                StoreChangelogReader.ChangelogState.RESTORING,
            changelogReader.changelogMetadata(tp).state()
        );
        if (type == ACTIVE) {
            assertNull(changelogReader.changelogMetadata(tp).endOffset());
        } else {
            assertEquals(0L, (long) changelogReader.changelogMetadata(tp).endOffset());
        }
        assertTrue(functionCalled.get());
        verify(mockTask);

        resetToDefault(mockTask);
        if (type == ACTIVE) {
            mockTask.clearTaskTimeout();
            mockTask.clearTaskTimeout();
            expectLastCall();
        }
        replay(mockTask);

        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(type == ACTIVE ? 10L : 0L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(6L, consumer.position(tp));
        verify(mockTask);
    }

    @Test
    public void shouldThrowIfCommittedOffsetsFail() {
        final TaskId taskId = new TaskId(0, 0);

        EasyMock.expect(stateManager.taskId()).andReturn(taskId);
        EasyMock.expect(stateManager.changelogAsSource(tp)).andReturn(true).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(10L).anyTimes();
        EasyMock.replay(stateManager, storeMetadata, store);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public synchronized ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, final ListConsumerGroupOffsetsOptions options) {
                throw kaboom;
            }
        };
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        changelogReader.register(tp, stateManager);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldThrowIfUnsubscribeFail() {
        EasyMock.replay(stateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public void unsubscribe() {
                throw kaboom;
            }
        };
        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);

        final StreamsException thrown = assertThrows(StreamsException.class, changelogReader::clear);
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldOnlyRestoreStandbyChangelogInUpdateStandbyState() {
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
        EasyMock.replay(mockTasks, standbyStateManager, storeMetadata, store);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        changelogReader.register(tp, standbyStateManager);
        changelogReader.restore(mockTasks);

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore(mockTasks);
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertTrue(changelogReader.changelogMetadata(tp).bufferedRecords().isEmpty());

        assertEquals(Collections.singleton(tp), consumer.paused());

        changelogReader.transitToUpdateStandby();
        changelogReader.restore(mockTasks);
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(5L, changelogReader.changelogMetadata(tp).totalRestored());
        assertTrue(changelogReader.changelogMetadata(tp).bufferedRecords().isEmpty());
    }

    @Test
    public void shouldNotUpdateLimitForNonSourceStandbyChangelog() {
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
        EasyMock.expect(standbyStateManager.changelogAsSource(tp)).andReturn(false).anyTimes();
        EasyMock.replay(mockTasks, standbyStateManager, storeMetadata, store);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public synchronized ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, final ListConsumerGroupOffsetsOptions options) {
                throw new AssertionError("Should not try to fetch committed offsets");
            }
        };

        final Properties properties = new Properties();
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader", properties));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);
        changelogReader.transitToUpdateStandby();

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        changelogReader.register(tp, standbyStateManager);
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        // if there's no records fetchable, nothings gets restored
        changelogReader.restore(mockTasks);
        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 5L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        // we should be able to restore to the log end offsets since there's no limit
        changelogReader.restore(mockTasks);
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(6L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @Test
    public void shouldRestoreToLimitInStandbyState() {
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
        EasyMock.expect(standbyStateManager.changelogAsSource(tp)).andReturn(true).anyTimes();
        EasyMock.replay(mockTasks, standbyStateManager, storeMetadata, store);

        final long now = time.milliseconds();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader", properties));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);
        changelogReader.transitToUpdateStandby();

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        adminClient.updateConsumerGroupOffsets(Collections.singletonMap(tp, 7L));
        changelogReader.register(tp, standbyStateManager);
        assertEquals(0L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        changelogReader.restore(mockTasks);

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(7L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 5L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore(mockTasks);
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(7L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(2L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));

        adminClient.updateConsumerGroupOffsets(Collections.singletonMap(tp, 10L));
        // should not try to read committed offsets if interval has not reached
        time.setCurrentTimeMs(now + 100L);
        changelogReader.restore(mockTasks);
        assertEquals(7L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(2L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        time.setCurrentTimeMs(now + 101L);
        // the first restore would only update the limit, same below
        changelogReader.restore(mockTasks);
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(2L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        changelogReader.restore(mockTasks);
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        adminClient.updateConsumerGroupOffsets(Collections.singletonMap(tp, 15L));
        // after we've updated once, the timer should be reset and we should not try again until next interval elapsed
        time.setCurrentTimeMs(now + 201L);
        changelogReader.restore(mockTasks);
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        // once we are in update active mode, we should not try to update limit offset
        time.setCurrentTimeMs(now + 202L);
        changelogReader.enforceRestoreActive();
        changelogReader.restore(mockTasks);
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        changelogReader.transitToUpdateStandby();
        changelogReader.restore(mockTasks);
        assertEquals(15L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        changelogReader.restore(mockTasks);
        assertEquals(15L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(6L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 12L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 13L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 14L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 15L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore(mockTasks);
        assertEquals(15L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(9L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(1, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
    }

    @Test
    public void shouldRestoreMultipleChangelogs() {
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        EasyMock.expect(mockTasks.get(null)).andReturn(mock(Task.class)).anyTimes();
        EasyMock.expect(storeMetadataOne.changelogPartition()).andReturn(tp1).anyTimes();
        EasyMock.expect(storeMetadataOne.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadataTwo.changelogPartition()).andReturn(tp2).anyTimes();
        EasyMock.expect(storeMetadataTwo.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(0L).anyTimes();
        EasyMock.expect(storeMetadataOne.offset()).andReturn(0L).anyTimes();
        EasyMock.expect(storeMetadataTwo.offset()).andReturn(0L).anyTimes();
        EasyMock.expect(activeStateManager.storeMetadata(tp1)).andReturn(storeMetadataOne).anyTimes();
        EasyMock.expect(activeStateManager.storeMetadata(tp2)).andReturn(storeMetadataTwo).anyTimes();
        EasyMock.expect(activeStateManager.changelogOffsets()).andReturn(mkMap(
            mkEntry(tp, 5L),
            mkEntry(tp1, 5L),
            mkEntry(tp2, 5L)
        )).anyTimes();
        EasyMock.replay(mockTasks, activeStateManager, storeMetadata, store, storeMetadataOne, storeMetadataTwo);

        setupConsumer(10, tp);
        setupConsumer(5, tp1);
        setupConsumer(3, tp2);

        changelogReader.register(tp, activeStateManager);
        changelogReader.register(tp1, activeStateManager);
        changelogReader.register(tp2, activeStateManager);

        changelogReader.restore(mockTasks);

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());

        // should support removing and clearing changelogs
        changelogReader.unregister(Collections.singletonList(tp));
        assertNull(changelogReader.changelogMetadata(tp));
        assertFalse(changelogReader.isEmpty());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());

        changelogReader.clear();
        assertTrue(changelogReader.isEmpty());
        assertNull(changelogReader.changelogMetadata(tp1));
        assertNull(changelogReader.changelogMetadata(tp2));
        assertEquals(changelogReader.state(), ACTIVE_RESTORING);
    }

    @Test
    public void shouldTransitState() {
        final TaskId taskId = new TaskId(0, 0);
        EasyMock.expect(storeMetadataOne.changelogPartition()).andReturn(tp1).anyTimes();
        EasyMock.expect(storeMetadataOne.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadataTwo.changelogPartition()).andReturn(tp2).anyTimes();
        EasyMock.expect(storeMetadataTwo.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(storeMetadataOne.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(storeMetadataTwo.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(tp1)).andReturn(storeMetadataOne).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(tp2)).andReturn(storeMetadataTwo).anyTimes();
        EasyMock.expect(activeStateManager.changelogOffsets()).andReturn(singletonMap(tp, 5L));
        EasyMock.expect(activeStateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(activeStateManager, standbyStateManager, storeMetadata, store, storeMetadataOne, storeMetadataTwo);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp1, 10L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp2, 10L));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);
        assertEquals(ACTIVE_RESTORING, changelogReader.state());

        changelogReader.register(tp, activeStateManager);
        changelogReader.register(tp1, standbyStateManager);
        changelogReader.register(tp2, standbyStateManager);
        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp2).state());

        assertEquals(Collections.emptySet(), consumer.assignment());

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(mkSet(tp1, tp2), consumer.paused());
        assertEquals(ACTIVE_RESTORING, changelogReader.state());

        // transition to restore active is idempotent
        changelogReader.enforceRestoreActive();
        assertEquals(ACTIVE_RESTORING, changelogReader.state());

        changelogReader.transitToUpdateStandby();
        assertEquals(STANDBY_UPDATING, changelogReader.state());

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(Collections.emptySet(), consumer.paused());

        // transition to update standby is NOT idempotent
        assertThrows(IllegalStateException.class, changelogReader::transitToUpdateStandby);

        changelogReader.unregister(Collections.singletonList(tp));
        changelogReader.register(tp, activeStateManager);

        // if a new active is registered, we should immediately transit to standby updating
        assertThrows(
            IllegalStateException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(STANDBY_UPDATING, changelogReader.state());

        changelogReader.enforceRestoreActive();
        assertEquals(ACTIVE_RESTORING, changelogReader.state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(mkSet(tp1, tp2), consumer.paused());
    }

    @Test
    public void shouldTransitStateBackToActiveRestoringAfterRemovingLastTask() {
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback);
        EasyMock.expect(standbyStateManager.storeMetadata(tp1)).andReturn(storeMetadataOne).anyTimes();
        EasyMock.expect(storeMetadataOne.changelogPartition()).andReturn(tp1).anyTimes();
        EasyMock.expect(storeMetadataOne.store()).andReturn(store).anyTimes();
        EasyMock.replay(standbyStateManager, store, storeMetadataOne);
        changelogReader.register(tp1, standbyStateManager);
        changelogReader.transitToUpdateStandby();

        changelogReader.unregister(mkSet(tp1));
        assertTrue(changelogReader.isEmpty());
        assertEquals(ACTIVE_RESTORING, changelogReader.state());
    }

    @Test
    public void shouldThrowIfRestoreCallbackThrows() {
        final TaskId taskId = new TaskId(0, 0);

        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(activeStateManager.taskId()).andReturn(taskId).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, exceptionCallback);

        changelogReader.register(tp, activeStateManager);

        StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));

        thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());

        consumer.seek(tp, 10L);

        thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldNotThrowOnUnknownRevokedPartition() {
        LogCaptureAppender.setClassLoggerToDebug(StoreChangelogReader.class);
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StoreChangelogReader.class)) {
            changelogReader.unregister(Collections.singletonList(new TopicPartition("unknown", 0)));

            assertThat(
                appender.getMessages(),
                hasItem("test-reader Changelog partition unknown-0 could not be found," +
                    " it could be already cleaned up during the handling of task corruption and never restore again")
            );
        }
    }

    private void setupConsumer(final long messages, final TopicPartition topicPartition) {
        assignPartition(messages, topicPartition);
        addRecords(messages, topicPartition);
        consumer.assign(Collections.emptyList());
    }

    private void addRecords(final long messages, final TopicPartition topicPartition) {
        for (int i = 0; i < messages; i++) {
            consumer.addRecord(new ConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                i,
                new byte[0],
                new byte[0]));
        }
    }

    private void assignPartition(final long messages,
                                 final TopicPartition topicPartition) {
        consumer.updatePartitions(
            topicPartition.topic(),
            Collections.singletonList(new PartitionInfo(
                topicPartition.topic(),
                topicPartition.partition(),
                null,
                null,
                null)));
        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, Math.max(0, messages) + 1));
        adminClient.updateEndOffsets(Collections.singletonMap(topicPartition, Math.max(0, messages) + 1));
        consumer.assign(Collections.singletonList(topicPartition));
    }
}
