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
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.test.MockStandbyUpdateListener;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.StoreChangelogReader.ChangelogReaderState.ACTIVE_RESTORING;
import static org.apache.kafka.streams.processor.internals.StoreChangelogReader.ChangelogReaderState.STANDBY_UPDATING;
import static org.apache.kafka.streams.processor.internals.Task.TaskType.ACTIVE;
import static org.apache.kafka.streams.processor.internals.Task.TaskType.STANDBY;
import static org.apache.kafka.test.MockStandbyUpdateListener.UPDATE_BATCH;
import static org.apache.kafka.test.MockStandbyUpdateListener.UPDATE_START;
import static org.apache.kafka.test.MockStandbyUpdateListener.UPDATE_SUSPENDED;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_SUSPENDED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StoreChangelogReaderTest {

    @Mock
    private ProcessorStateManager stateManager;
    @Mock
    private ProcessorStateManager activeStateManager;
    @Mock
    private ProcessorStateManager standbyStateManager;
    @Mock
    private StateStoreMetadata storeMetadata;
    @Mock
    private StateStoreMetadata storeMetadataOne;
    @Mock
    private StateStoreMetadata storeMetadataTwo;
    @Mock
    private StateStore store;

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

    private final MockStandbyUpdateListener standbyListener = new MockStandbyUpdateListener();
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

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
    private final MockAdminClient adminClient = new MockAdminClient();
    private final StoreChangelogReader changelogReader =
        new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

    private void setupStateManagerMock(final Task.TaskType type) {
        when(stateManager.storeMetadata(tp)).thenReturn(storeMetadata);
        when(stateManager.taskType()).thenReturn(type);
    }

    private void setupActiveStateManager() {
        when(activeStateManager.storeMetadata(tp)).thenReturn(storeMetadata);
        when(activeStateManager.taskType()).thenReturn(ACTIVE);
    }

    private void setupStandbyStateManager() {
        when(standbyStateManager.storeMetadata(tp)).thenReturn(storeMetadata);
        when(standbyStateManager.taskType()).thenReturn(STANDBY);
    }

    private void setupStoreMetadata() {
        when(storeMetadata.changelogPartition()).thenReturn(tp);
        when(storeMetadata.store()).thenReturn(store);
    }

    private void setupStore() {
        when(store.name()).thenReturn(storeName);
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldNotRegisterSameStoreMultipleTimes(final Task.TaskType type) {
        setupStateManagerMock(type);

        changelogReader.register(tp, stateManager);

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        assertThrows(IllegalStateException.class, () -> changelogReader.register(tp, stateManager));
    }

    @Test
    public void shouldNotRegisterStoreWithoutMetadata() {
        assertThrows(IllegalStateException.class,
            () -> changelogReader.register(new TopicPartition("ChangelogWithoutStoreMetadata", 0), stateManager));
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldSupportUnregisterChangelogBeforeInitialization(final Task.TaskType type) {
        setupStateManagerMock(type);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 100L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.unregister(Collections.singleton(tp));

        assertEquals(Collections.emptySet(), consumer.assignment());

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertNull(callback.storeNameCalledStates.get(RESTORE_SUSPENDED));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
        assertNull(standbyListener.capturedStore(UPDATE_SUSPENDED));
        assertNull(standbyListener.capturedStore(UPDATE_START));
        assertNull(standbyListener.capturedStore(UPDATE_START));
        assertNull(standbyListener.capturedStore(UPDATE_BATCH));
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldSupportUnregisterChangelogBeforeCompletion(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(9L);
        if (type == STANDBY) {
            when(storeMetadata.endOffset()).thenReturn(10L);
            when(stateManager.changelogAsSource(tp)).thenReturn(true);
        }

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 100L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(mockTasks);

        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(Collections.emptySet(), changelogReader.completedChangelogs());
        assertEquals(10L, consumer.position(tp));
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(Collections.singleton(tp), consumer.assignment());

        changelogReader.unregister(Collections.singleton(tp));

        assertEquals(Collections.emptySet(), consumer.assignment());

        if (type == ACTIVE) {
            assertEquals(tp, callback.restoreTopicPartition);
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_SUSPENDED));
        } else {
            assertNull(callback.restoreTopicPartition);
            assertNull(callback.storeNameCalledStates.get(RESTORE_START));
            assertNull(callback.storeNameCalledStates.get(RESTORE_SUSPENDED));
            assertEquals(storeName, standbyListener.capturedStore(UPDATE_START));
            assertEquals(tp, standbyListener.updatePartition);
        }
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldSupportUnregisterChangelogAfterCompletion(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(9L);
        if (type == STANDBY) {
            when(storeMetadata.endOffset()).thenReturn(10L);
            when(stateManager.changelogAsSource(tp)).thenReturn(true);
        }

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(mockTasks);

        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(10L, consumer.position(tp));

        assertEquals(Collections.singleton(tp), consumer.assignment());
        if (type == ACTIVE) {
            assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
            assertEquals(Collections.singleton(tp), consumer.paused());
        } else {
            assertEquals(Collections.emptySet(), changelogReader.completedChangelogs());
            assertEquals(Collections.emptySet(), consumer.paused());
        }

        changelogReader.unregister(Collections.singleton(tp));

        assertEquals(Collections.emptySet(), consumer.assignment());

        if (type == ACTIVE) {
            assertEquals(tp, callback.restoreTopicPartition);
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
            assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
            assertNull(callback.storeNameCalledStates.get(RESTORE_SUSPENDED));
            assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
        } else {
            assertNull(callback.storeNameCalledStates.get(UPDATE_SUSPENDED));
            assertNull(callback.storeNameCalledStates.get(UPDATE_BATCH));
            assertEquals(storeName, standbyListener.capturedStore(UPDATE_START));
            assertEquals(tp, standbyListener.updatePartition);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldInitializeChangelogAndCheckForCompletion(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(9L);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

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

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldTriggerRestoreListenerWithOffsetZeroIfPositionThrowsTimeoutException(final Task.TaskType type) {
        // restore listener is only triggered for active tasks
        if (type == ACTIVE) {
            setupStateManagerMock(type);
            setupStoreMetadata();
            @SuppressWarnings("unchecked")
            final Map<TaskId, Task> mockTasks = mock(Map.class);
            when(mockTasks.get(null)).thenReturn(mock(Task.class));
            when(mockTasks.containsKey(null)).thenReturn(true);
            when(stateManager.changelogOffsets()).thenReturn(singletonMap(tp, 5L));

            adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

            final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public long position(final TopicPartition partition) {
                    throw new TimeoutException("KABOOM!");
                }
            };
            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));

            final StoreChangelogReader changelogReader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

            changelogReader.register(tp, stateManager);
            changelogReader.restore(mockTasks);

            assertThat(callback.restoreStartOffset, equalTo(0L));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldPollWithRightTimeoutWithStateUpdater(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        shouldPollWithRightTimeout(true, type);
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldPollWithRightTimeoutWithoutStateUpdater(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        shouldPollWithRightTimeout(false, type);
    }

    private void shouldPollWithRightTimeout(final boolean stateUpdaterEnabled, final Task.TaskType type) {
        final Properties properties = new Properties();
        properties.put(InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled);
        shouldPollWithRightTimeout(properties, type);
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldPollWithRightTimeoutWithStateUpdaterDefault(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        final Properties properties = new Properties();
        shouldPollWithRightTimeout(properties, type);
    }

    private void shouldPollWithRightTimeout(final Properties properties, final Task.TaskType type) {
        final TaskId taskId = new TaskId(0, 0);

        when(storeMetadata.offset()).thenReturn(null).thenReturn(9L);
        when(stateManager.taskId()).thenReturn(taskId);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 11L));

        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader", properties));

        final StoreChangelogReader changelogReader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, stateManager);

        if (type == STANDBY) {
            changelogReader.transitToUpdateStandby();
        }

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));
        if (type == ACTIVE) {
            assertEquals(Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)), consumer.lastPollTimeout());
        } else {
            if (!properties.containsKey(InternalConfig.STATE_UPDATER_ENABLED)
                    || (boolean) properties.get(InternalConfig.STATE_UPDATER_ENABLED)) {
                assertEquals(Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)), consumer.lastPollTimeout());
            } else {
                assertEquals(Duration.ZERO, consumer.lastPollTimeout());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldRestoreFromPositionAndCheckForCompletion(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);

        when(storeMetadata.offset()).thenReturn(5L);
        if (type == STANDBY) {
            when(storeMetadata.endOffset()).thenReturn(10L);
        }
        when(stateManager.taskId()).thenReturn(taskId);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

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

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldRestoreFromBeginningAndCheckCompletion(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);

        if (type == STANDBY && logContext.logger(StoreChangelogReader.class).isDebugEnabled()) {
            when(storeMetadata.offset()).thenReturn(null).thenReturn(null).thenReturn(9L);
            when(storeMetadata.endOffset()).thenReturn(10L);
        } else {
            when(storeMetadata.offset()).thenReturn(null).thenReturn(9L);
        }
        when(stateManager.taskId()).thenReturn(taskId);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 11L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

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
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(5L);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 0L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

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
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);

        final Task mockTask = mock(Task.class);
        when(storeMetadata.offset()).thenReturn(10L);
        when(activeStateManager.changelogOffsets()).thenReturn(singletonMap(tp, 10L));
        when(activeStateManager.taskId()).thenReturn(taskId);

        final AtomicBoolean clearException = new AtomicBoolean(false);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
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
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertTrue(changelogReader.completedChangelogs().isEmpty());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        Mockito.verify(mockTask).clearTaskTimeout();
        Mockito.verify(mockTask).maybeInitTaskTimeoutOrThrow(anyLong(), any());
        Mockito.verify(mockTask).recordRestoration(any(), anyLong(), anyBoolean());

        clearException.set(true);
        Mockito.reset(mockTask);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertEquals(10L, consumer.position(tp));
        Mockito.verify(mockTask).clearTaskTimeout();
    }

    @Test
    public void shouldThrowIfPositionFail() {
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);
        when(activeStateManager.taskId()).thenReturn(taskId);
        when(storeMetadata.offset()).thenReturn(10L);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public long position(final TopicPartition partition) {
                throw kaboom;
            }
        };

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, activeStateManager);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldRequestEndOffsetsAndHandleTimeoutException() {
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);

        final Task mockTask = mock(Task.class);

        when(storeMetadata.offset()).thenReturn(5L);
        when(activeStateManager.changelogOffsets()).thenReturn(singletonMap(tp, 5L));
        when(activeStateManager.taskId()).thenReturn(taskId);

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

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new AssertionError("Should not trigger this function");
            }
        };

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertTrue(functionCalled.get());
        Mockito.verify(mockTask).maybeInitTaskTimeoutOrThrow(anyLong(), any());

        Mockito.reset(mockTask);

        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(6L, consumer.position(tp));
        Mockito.verify(mockTask).clearTaskTimeout();
        Mockito.verify(mockTask).recordRestoration(any(), anyLong(), anyBoolean());
    }

    @Test
    public void shouldThrowIfEndOffsetsFail() {
        setupActiveStateManager();
        when(storeMetadata.changelogPartition()).thenReturn(tp);
        final TaskId taskId = new TaskId(0, 0);

        when(activeStateManager.taskId()).thenReturn(taskId);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public ListOffsetsResult listOffsets(final Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                                 final ListOffsetsOptions options) {
                throw kaboom;
            }
        };
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 0L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, activeStateManager);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldRequestCommittedOffsetsAndHandleTimeoutException(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();

        final TaskId taskId = new TaskId(0, 0);

        final Task mockTask = mock(Task.class);
        if (type == ACTIVE) {
            mockTask.clearTaskTimeout();
        }

        when(stateManager.changelogAsSource(tp)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(5L);
        when(stateManager.taskId()).thenReturn(taskId);

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
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

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
        Mockito.verify(mockTask).maybeInitTaskTimeoutOrThrow(anyLong(), any());

        Mockito.reset(mockTask);

        changelogReader.restore(Collections.singletonMap(taskId, mockTask));

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(type == ACTIVE ? 10L : 0L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(6L, consumer.position(tp));
        if (type == ACTIVE) {
            Mockito.verify(mockTask, times(2)).clearTaskTimeout();
            Mockito.verify(mockTask).recordRestoration(any(), anyLong(), anyBoolean());
        }
    }

    @ParameterizedTest
    @EnumSource(Task.TaskType.class)
    public void shouldThrowIfCommittedOffsetsFail(final Task.TaskType type) {
        setupStateManagerMock(type);
        when(storeMetadata.changelogPartition()).thenReturn(tp);

        final TaskId taskId = new TaskId(0, 0);

        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.changelogAsSource(tp)).thenReturn(true);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public synchronized ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, final ListConsumerGroupOffsetsOptions options) {
                throw kaboom;
            }
        };
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, stateManager);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)))
        );
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldThrowIfUnsubscribeFail() {
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public void unsubscribe() {
                throw kaboom;
            }
        };
        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        final StreamsException thrown = assertThrows(StreamsException.class, changelogReader::clear);
        assertEquals(kaboom, thrown.getCause());
    }

    @Test
    public void shouldOnlyRestoreStandbyChangelogInUpdateStandbyState() {
        setupStandbyStateManager();
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(3L);
        when(storeMetadata.endOffset()).thenReturn(20L);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
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
        setupStandbyStateManager();
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(3L);
        when(storeMetadata.endOffset()).thenReturn(20L);
        when(standbyStateManager.changelogAsSource(tp)).thenReturn(false);

        final MockAdminClient adminClient = new MockAdminClient() {
            @Override
            public synchronized ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, final ListConsumerGroupOffsetsOptions options) {
                throw new AssertionError("Should not try to fetch committed offsets");
            }
        };

        final Properties properties = new Properties();
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader", properties));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
        changelogReader.transitToUpdateStandby();

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
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
        setupStandbyStateManager();
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(standbyStateManager.changelogAsSource(tp)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(3L);
        when(storeMetadata.endOffset()).thenReturn(20L);

        final long now = time.milliseconds();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader", properties));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
        changelogReader.transitToUpdateStandby();

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
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
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadataOne.changelogPartition()).thenReturn(tp1);
        when(storeMetadataOne.store()).thenReturn(store);
        when(storeMetadataTwo.changelogPartition()).thenReturn(tp2);
        when(storeMetadataTwo.store()).thenReturn(store);
        when(storeMetadata.offset()).thenReturn(0L);
        when(storeMetadataOne.offset()).thenReturn(0L);
        when(storeMetadataTwo.offset()).thenReturn(0L);
        when(activeStateManager.storeMetadata(tp1)).thenReturn(storeMetadataOne);
        when(activeStateManager.storeMetadata(tp2)).thenReturn(storeMetadataTwo);
        when(activeStateManager.changelogOffsets()).thenReturn(mkMap(
            mkEntry(tp, 5L),
            mkEntry(tp1, 5L),
            mkEntry(tp2, 5L)
        ));

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
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        when(standbyStateManager.taskType()).thenReturn(STANDBY);
        final TaskId taskId = new TaskId(0, 0);
        when(storeMetadataOne.changelogPartition()).thenReturn(tp1);
        when(storeMetadataOne.store()).thenReturn(store);
        when(storeMetadataTwo.changelogPartition()).thenReturn(tp2);
        when(storeMetadataTwo.store()).thenReturn(store);
        when(storeMetadata.offset()).thenReturn(5L);
        when(storeMetadataOne.offset()).thenReturn(5L);
        when(storeMetadataTwo.offset()).thenReturn(5L);
        when(standbyStateManager.storeMetadata(tp1)).thenReturn(storeMetadataOne);
        when(standbyStateManager.storeMetadata(tp2)).thenReturn(storeMetadataTwo);
        when(activeStateManager.changelogOffsets()).thenReturn(singletonMap(tp, 5L));
        when(activeStateManager.taskId()).thenReturn(taskId);
        when(standbyStateManager.taskId()).thenReturn(taskId);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp1, 10L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp2, 10L));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
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
        assertEquals(Set.of(tp, tp1, tp2), consumer.assignment());
        assertEquals(Set.of(tp1, tp2), consumer.paused());
        assertEquals(ACTIVE_RESTORING, changelogReader.state());

        // transition to restore active is idempotent
        changelogReader.enforceRestoreActive();
        assertEquals(ACTIVE_RESTORING, changelogReader.state());

        changelogReader.transitToUpdateStandby();
        assertEquals(STANDBY_UPDATING, changelogReader.state());

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(Set.of(tp, tp1, tp2), consumer.assignment());
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
        assertEquals(Set.of(tp, tp1, tp2), consumer.assignment());
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(STANDBY_UPDATING, changelogReader.state());

        changelogReader.enforceRestoreActive();
        assertEquals(ACTIVE_RESTORING, changelogReader.state());
        assertEquals(Set.of(tp, tp1, tp2), consumer.assignment());
        assertEquals(Set.of(tp1, tp2), consumer.paused());
    }

    @Test
    public void shouldTransitStateBackToActiveRestoringAfterRemovingLastTask() {
        when(standbyStateManager.taskType()).thenReturn(STANDBY);
        final StoreChangelogReader changelogReader = new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
        when(standbyStateManager.storeMetadata(tp1)).thenReturn(storeMetadataOne);
        changelogReader.register(tp1, standbyStateManager);
        changelogReader.transitToUpdateStandby();

        changelogReader.unregister(Set.of(tp1));
        assertTrue(changelogReader.isEmpty());
        assertEquals(ACTIVE_RESTORING, changelogReader.state());
    }

    @Test
    public void shouldThrowIfRestoreCallbackThrows() {
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);

        when(storeMetadata.offset()).thenReturn(5L);
        when(activeStateManager.taskId()).thenReturn(taskId);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, exceptionCallback, standbyListener);

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
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StoreChangelogReader.class)) {
            appender.setClassLogger(StoreChangelogReader.class, Level.DEBUG);
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
