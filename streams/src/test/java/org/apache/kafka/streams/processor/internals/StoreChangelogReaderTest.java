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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StandbyUpdateListener.SuspendReason;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockStandbyUpdateListener;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
            assertEquals(storeName, standbyListener.capturedStore(UPDATE_SUSPENDED));
            assertEquals(SuspendReason.MIGRATED, standbyListener.updateSuspendedReason);
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
            assertEquals(storeName, standbyListener.capturedStore(UPDATE_SUSPENDED));
            assertEquals(SuspendReason.MIGRATED, standbyListener.updateSuspendedReason);
        }
    }

    @Test
    public void shouldPassSuspendReasonToStandbyListener() {
        setupStateManagerMock(STANDBY);
        setupStoreMetadata();
        setupStore();
        @SuppressWarnings("unchecked")
        final Map<TaskId, Task> mockTasks = mock(Map.class);
        when(mockTasks.get(null)).thenReturn(mock(Task.class));
        when(mockTasks.containsKey(null)).thenReturn(true);
        when(storeMetadata.offset()).thenReturn(9L);
        when(storeMetadata.endOffset()).thenReturn(10L);
        when(stateManager.changelogAsSource(tp)).thenReturn(true);

        adminClient.updateEndOffsets(Collections.singletonMap(tp, 100L));

        final StoreChangelogReader changelogReader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        changelogReader.register(tp, stateManager);
        changelogReader.transitToUpdateStandby();
        changelogReader.restore(mockTasks);

        changelogReader.unregister(Collections.singleton(tp), SuspendReason.PROMOTED);

        assertEquals(storeName, standbyListener.capturedStore(UPDATE_SUSPENDED));
        assertEquals(SuspendReason.PROMOTED, standbyListener.updateSuspendedReason);
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

            final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
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
    public void shouldPollWithRightTimeout(final Task.TaskType type) {
        setupStateManagerMock(type);
        setupStoreMetadata();
        setupStore();

        final TaskId taskId = new TaskId(0, 0);

        when(storeMetadata.offset()).thenReturn(null).thenReturn(9L);
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

        assertEquals(Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)), consumer.lastPollTimeout());
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

    /**
     * KAFKA-14302: an empty restore of a high-LEO compacted changelog must still advertise the log
     * end offset (next-fetch), not 0, so taskOffsetSums match the assignor's endOffsetSum.
     */
    @ParameterizedTest
    @EnumSource(value = Task.TaskType.class, names = {"ACTIVE", "STANDBY"})
    public void shouldReportCaughtUpOffsetAfterRestoringEmptyChangelogWithHighEndOffset(final Task.TaskType type) throws IOException {
        final long changelogEndOffset = 20_000L;
        final TaskId taskId = new TaskId(0, 0);
        final File stateDir = TestUtils.tempDirectory();
        final StateDirectory stateDirectory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, "test-reader");
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time,
            true,
            false
        );
        final ProcessorStateManager realStateManager = new ProcessorStateManager(
            taskId,
            type,
            false,
            false,
            logContext,
            stateDirectory,
            time,
            mkMap(mkEntry(storeName, topicName)),
            Collections.emptySet()
        );
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);

        try {
            realStateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            // Empty local state directory: no checkpoint, store offset stays null (pod restart without a PV).
            realStateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, changelogEndOffset));
            consumer.updateEndOffsets(Collections.singletonMap(tp, changelogEndOffset));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, changelogEndOffset));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, realStateManager);
            if (type == STANDBY) {
                reader.transitToUpdateStandby();
            }
            reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

            if (type == ACTIVE) {
                assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, reader.changelogMetadata(tp).state());
            } else {
                // Standbys never transit to COMPLETED; they keep updating. The advertised offset
                // is what TaskManager publishes on the next subscription (no LATEST_OFFSET overlay).
                assertEquals(StoreChangelogReader.ChangelogState.RESTORING, reader.changelogMetadata(tp).state());
            }
            assertEquals(0L, reader.changelogMetadata(tp).totalRestored());
            assertTrue(kvStore.keys.isEmpty());
            assertEquals(
                changelogEndOffset,
                consumer.position(tp),
                "restore consumer must sit at the compacted log end"
            );
            assertEquals(changelogEndOffset - 1L, realStateManager.storeMetadata(tp).offset());
            // Next offset to fetch must be the log end offset so taskOffsetSums equal the assignor's endOffsetSum.
            assertEquals(
                Collections.singletonMap(tp, changelogEndOffset),
                realStateManager.changelogOffsets(),
                "empty restore of a high-end changelog must advertise LEO, not 0"
            );
        } finally {
            realStateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldDecrementRemainingRecordsToZeroWithOffsetGaps() {
        // end offset is 10 but the changelog holds only 8 data records (offsets 0..7); offsets 8 and 9
        // are transaction markers the restore consumer never returns, so remaining-records is initialized
        // to 10 offset slots and must still decrement to exactly zero once restoration completes
        setupActiveStateManager();
        setupStoreMetadata();
        setupStore();
        final TaskId taskId = new TaskId(0, 0);

        // null while preparing (seek-to-beginning, so startOffset == 0) and before the first batch;
        // once a batch is applied it reflects the last restored record's offset (7)
        when(storeMetadata.offset()).thenReturn(null).thenReturn(null).thenReturn(7L);
        when(activeStateManager.taskId()).thenReturn(taskId);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

        final Task mockTask = mock(Task.class);

        changelogReader.register(tp, activeStateManager);

        // first restore initializes the changelog and records the initial remaining (= 10 slots)
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));
        assertEquals(0L, consumer.position(tp));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());

        for (int offset = 0; offset < 8; offset++) {
            consumer.addRecord(new ConsumerRecord<>(topicName, 0, offset, "key".getBytes(), "value".getBytes()));
        }

        // second restore applies the 8 data records
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));
        assertEquals(8L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());

        // skip the trailing transaction-marker offsets (8, 9) so the consumer reaches the end
        consumer.seek(tp, 10L);

        // third restore observes position >= endOffset and completes restoration
        changelogReader.restore(Collections.singletonMap(taskId, mockTask));
        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(8L, changelogReader.changelogMetadata(tp).totalRestored());

        // remaining-records is driven by numOffsets (init sets it, later calls decrement it); restore-total
        // is driven by numRecords, a distinct quantity when the changelog has offset gaps
        final ArgumentCaptor<Long> numRecordsCaptor = ArgumentCaptor.forClass(Long.class);
        final ArgumentCaptor<Long> numOffsetsCaptor = ArgumentCaptor.forClass(Long.class);
        final ArgumentCaptor<Boolean> initCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(mockTask, atLeastOnce())
            .recordRestoration(any(), numRecordsCaptor.capture(), numOffsetsCaptor.capture(), initCaptor.capture());

        long initialRemaining = 0L;
        long decrementedRemaining = 0L;
        long totalRecords = 0L;
        for (int i = 0; i < initCaptor.getAllValues().size(); i++) {
            if (initCaptor.getAllValues().get(i)) {
                initialRemaining += numOffsetsCaptor.getAllValues().get(i);
            } else {
                decrementedRemaining += numOffsetsCaptor.getAllValues().get(i);
                totalRecords += numRecordsCaptor.getAllValues().get(i);
            }
        }

        assertEquals(10L, initialRemaining, "remaining-records metric should be initialized from the offset range");
        assertEquals(initialRemaining, decrementedRemaining, "remaining-records metric should decrement to exactly zero");
        assertEquals(8L, totalRecords, "restore-total should count the records actually restored");
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
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
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
        Mockito.verify(mockTask).recordRestoration(any(), anyLong(), anyLong(), anyBoolean());

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

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
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

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
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
        Mockito.verify(mockTask).recordRestoration(any(), anyLong(), anyLong(), anyBoolean());
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
            Mockito.verify(mockTask).recordRestoration(any(), anyLong(), anyLong(), anyBoolean());
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
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
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
    public void changelogEndOffsetsShouldFallBackToStoreMetadataWhenLogicalChangelogMetadataIsNull() {
        // Verifies the 3-step fallback chain in changelogEndOffsets():
        //   1) ChangelogMetadata.restoreEndOffset (preferred)
        //   2) StateStoreMetadata.endOffset (physical Fetch high-water-mark)
        //   3) null  (caller treats as MAX_VALUE — conservative for warm-up promotion)
        // This test wires a standby with restoreEndOffset == null but
        // storeMetadata.endOffset == 42L → fallback returns 42L.
        setupStandbyStateManager();
        when(storeMetadata.endOffset()).thenReturn(42L);

        changelogReader.register(tp, standbyStateManager);

        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(Long.valueOf(42L), changelogReader.logicalChangelogEndOffsets().get(tp));
    }

    @Test
    public void logicalChangelogEndOffsetsShouldReturnNullWhenBothSourcesUnknown() {
        setupStandbyStateManager();
        when(storeMetadata.endOffset()).thenReturn(null);

        changelogReader.register(tp, standbyStateManager);

        assertNull(changelogReader.logicalChangelogEndOffsets().get(tp));
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
        assertEquals(ACTIVE_RESTORING, changelogReader.state());
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

    @Test
    public void shouldCallRecordRestoreTimeAtTheEndOfRestore() {
        setupActiveStateManager();

        final MeteredKeyValueStore<?, ?> meteredStateStore = mock(MeteredKeyValueStore.class);

        when(storeMetadata.changelogPartition()).thenReturn(tp);
        when(storeMetadata.store()).thenReturn(meteredStateStore);
        when(meteredStateStore.name()).thenReturn(storeName);
        final TaskId taskId = new TaskId(0, 0);

        when(storeMetadata.offset()).thenReturn(0L);
        when(activeStateManager.taskId()).thenReturn(taskId);

        setupConsumer(2, tp);
        consumer.updateEndOffsets(Collections.singletonMap(tp, 2L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 2L));

        changelogReader.register(tp, activeStateManager);

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(1L, changelogReader.changelogMetadata(tp).totalRestored());
        verify(meteredStateStore).recordRestoreTime(anyLong());
    }

    @Test
    public void shouldNotCallRecordRestoreTimeIfRestoreDoesNotComplete() {
        setupActiveStateManager();

        final MeteredKeyValueStore<?, ?> meteredStateStore = mock(MeteredKeyValueStore.class);

        when(storeMetadata.changelogPartition()).thenReturn(tp);
        when(storeMetadata.store()).thenReturn(meteredStateStore);
        when(meteredStateStore.name()).thenReturn(storeName);
        final TaskId taskId = new TaskId(0, 0);

        when(storeMetadata.offset()).thenReturn(0L);
        when(activeStateManager.taskId()).thenReturn(taskId);

        setupConsumer(2, tp);
        consumer.updateEndOffsets(Collections.singletonMap(tp, 3L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 3L));

        changelogReader.register(tp, activeStateManager);

        changelogReader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(1L, changelogReader.changelogMetadata(tp).totalRestored());
        verify(meteredStateStore, never()).recordRestoreTime(anyLong());
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

    /**
     * The first poll after assignment returns nothing and every record arrives from the next poll
     * on. An empty poll only means the fetch has not landed, so giving up on it sends partitions
     * to a log-start seek with no margin against retention.
     */
    @Test
    public void shouldRetryProbePollBeforeFallingBackToLogStart() {
        final long shortRetentionMs = Duration.ofSeconds(3).toMillis();
        final long beginOffset = 900_000L;
        final long logEndOffset = 1_000_000L;
        final long seekTarget = 999_000L;
        final int numPartitions = 12;      // more than one poll can plausibly serve at once

        final TopicPartition[] tps = new TopicPartition[numPartitions];
        final Map<TopicPartition, Long> begins = new HashMap<>();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            tps[i] = new TopicPartition(tp.topic(), i);
            begins.put(tps[i], beginOffset);
            ends.put(tps[i], logEndOffset);
        }

        // a position after restore reflects records since consumed, not where it was seeked, so
        // seekToBeginning is the only unambiguous signal that the optimisation was abandoned
        final Set<TopicPartition> seekedToBeginning = new HashSet<>();
        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                        final Map<TopicPartition, Long> timestampsToSearch) {
                    final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                    timestampsToSearch.forEach((k, v) ->
                        result.put(k, new OffsetAndTimestamp(seekTarget, v)));
                    return result;
                }

                @Override
                public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {
                    seekedToBeginning.addAll(partitions);
                    super.seekToBeginning(partitions);
                }
            };
        probeConsumer.updateBeginningOffsets(begins);
        probeConsumer.updateEndOffsets(ends);
        adminClient.updateEndOffsets(ends);

        // records can only be added once assigned, and earlier polls happen before that; the first
        // poll after assignment delivers nothing, standing in for a fetch that has not landed
        final int[] assignedPolls = {0};
        for (int round = 0; round < numPartitions * 4; round++) {
            probeConsumer.schedulePollTask(() -> {
                if (!probeConsumer.assignment().contains(tps[0])) {
                    return;
                }
                if (++assignedPolls[0] <= 1) {
                    return;
                }
                for (final TopicPartition partition : tps) {
                    probeConsumer.addRecord(new ConsumerRecord<>(
                        partition.topic(), partition.partition(), logEndOffset - 1,
                        10_000_000L, TimestampType.CREATE_TIME,
                        0, 0, new byte[0], new byte[0], new RecordHeaders(), Optional.empty()));
                }
            });
        }

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        for (int i = 0; i < numPartitions; i++) {
            final StateStoreMetadata meta = mock(StateStoreMetadata.class);
            final ProcessorStateManager manager = mock(ProcessorStateManager.class);
            final StateStore store = mock(StateStore.class);
            when(meta.changelogPartition()).thenReturn(tps[i]);
            when(meta.store()).thenReturn(store);
            when(meta.offset()).thenReturn(null, 0L);   // no checkpoint, then a value once restoring
            when(meta.retentionPeriod()).thenReturn(shortRetentionMs);
            when(store.name()).thenReturn(storeName);
            when(manager.storeMetadata(tps[i])).thenReturn(meta);
            when(manager.taskType()).thenReturn(ACTIVE);
            when(manager.taskId()).thenReturn(new TaskId(0, i));
            probeReader.register(tps[i], manager);
        }

        final Map<TaskId, Task> probeTasks = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            probeTasks.put(new TaskId(0, i), mock(Task.class));
        }
        probeReader.restore(probeTasks);

        assertEquals(Collections.emptySet(), seekedToBeginning,
            "every partition has a 3s retention against a 100k-record log, so the optimisation "
                + "should apply to all of them; these were abandoned on one empty poll");
    }

    /**
     * One partition is answered per poll, so resolving all of them takes more polls than any fixed
     * budget in the old design allowed. A budget that stops while partitions are still being served
     * sends them to a log-start restore with no margin against retention.
     */
    @Test
    public void shouldKeepPollingWhileTheWindowIsStillResolvingPartitions() {
        final long shortRetentionMs = Duration.ofSeconds(3).toMillis();
        final long beginOffset = 900_000L;
        final long logEndOffset = 1_000_000L;
        final long seekTarget = 999_000L;
        final int numPartitions = 20;      // more polls than any fixed budget in the old design

        final TopicPartition[] tps = new TopicPartition[numPartitions];
        final Map<TopicPartition, Long> begins = new HashMap<>();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            tps[i] = new TopicPartition(tp.topic(), i);
            begins.put(tps[i], beginOffset);
            ends.put(tps[i], logEndOffset);
        }

        final Set<TopicPartition> seekedToBeginning = new HashSet<>();
        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                        final Map<TopicPartition, Long> timestampsToSearch) {
                    final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                    timestampsToSearch.forEach((k, v) ->
                        result.put(k, new OffsetAndTimestamp(seekTarget, v)));
                    return result;
                }

                @Override
                public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {
                    seekedToBeginning.addAll(partitions);
                    super.seekToBeginning(partitions);
                }
            };
        probeConsumer.updateBeginningOffsets(begins);
        probeConsumer.updateEndOffsets(ends);
        adminClient.updateEndOffsets(ends);

        // one partition per poll: the shape a shared max.poll.records budget produces when each
        // window is large enough to fill a poll on its own
        final int[] answered = {0};
        for (int round = 0; round < numPartitions * 4; round++) {
            probeConsumer.schedulePollTask(() -> {
                if (!probeConsumer.assignment().contains(tps[0]) || answered[0] >= numPartitions) {
                    return;
                }
                final TopicPartition partition = tps[answered[0]++];
                probeConsumer.addRecord(new ConsumerRecord<>(
                    partition.topic(), partition.partition(), logEndOffset - 1,
                    10_000_000L, TimestampType.CREATE_TIME,
                    0, 0, new byte[0], new byte[0], new RecordHeaders(), Optional.empty()));
            });
        }

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        for (int i = 0; i < numPartitions; i++) {
            final StateStoreMetadata meta = mock(StateStoreMetadata.class);
            final ProcessorStateManager manager = mock(ProcessorStateManager.class);
            final StateStore store = mock(StateStore.class);
            when(meta.changelogPartition()).thenReturn(tps[i]);
            when(meta.store()).thenReturn(store);
            when(meta.offset()).thenReturn(null, 0L);   // no checkpoint, then a value once restoring
            when(meta.retentionPeriod()).thenReturn(shortRetentionMs);
            when(store.name()).thenReturn(storeName);
            when(manager.storeMetadata(tps[i])).thenReturn(meta);
            when(manager.taskType()).thenReturn(ACTIVE);
            when(manager.taskId()).thenReturn(new TaskId(0, i));
            probeReader.register(tps[i], manager);
        }

        final Map<TaskId, Task> probeTasks = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            probeTasks.put(new TaskId(0, i), mock(Task.class));
        }
        probeReader.restore(probeTasks);

        assertEquals(Collections.emptySet(), seekedToBeginning,
            "every partition answered the probe, just not all in the same poll; these were "
                + "abandoned by a budget that stopped while they were still being served");
    }

    /**
     * A poll that returns without waiting has given no fetch a chance to land, so a run of them is
     * no more evidence than none. Here ten polls deliver nothing while the clock does not move,
     * which must not be read as an empty window.
     */
    @Test
    public void shouldNotGiveUpOnPollsThatHaveNotWaited() {
        final long shortRetentionMs = Duration.ofSeconds(3).toMillis();
        final long beginOffset = 900_000L;
        final long logEndOffset = 1_000_000L;
        final long seekTarget = 999_000L;
        final int numPartitions = 6;
        final int silentPolls = 10;        // well past PROBE_IDLE_POLLS, with time standing still

        final TopicPartition[] tps = new TopicPartition[numPartitions];
        final Map<TopicPartition, Long> begins = new HashMap<>();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            tps[i] = new TopicPartition(tp.topic(), i);
            begins.put(tps[i], beginOffset);
            ends.put(tps[i], logEndOffset);
        }

        final Set<TopicPartition> seekedToBeginning = new HashSet<>();
        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                        final Map<TopicPartition, Long> timestampsToSearch) {
                    final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                    timestampsToSearch.forEach((k, v) ->
                        result.put(k, new OffsetAndTimestamp(seekTarget, v)));
                    return result;
                }

                @Override
                public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {
                    seekedToBeginning.addAll(partitions);
                    super.seekToBeginning(partitions);
                }
            };
        probeConsumer.updateBeginningOffsets(begins);
        probeConsumer.updateEndOffsets(ends);
        adminClient.updateEndOffsets(ends);

        final int[] answered = {0};
        for (int round = 0; round < 60; round++) {
            probeConsumer.schedulePollTask(() -> {
                if (!probeConsumer.assignment().contains(tps[0]) || ++answered[0] <= silentPolls) {
                    return;
                }
                for (final TopicPartition partition : tps) {
                    probeConsumer.addRecord(new ConsumerRecord<>(
                        partition.topic(), partition.partition(), logEndOffset - 1,
                        10_000_000L, TimestampType.CREATE_TIME,
                        0, 0, new byte[0], new byte[0], new RecordHeaders(), Optional.empty()));
                }
            });
        }

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        for (int i = 0; i < numPartitions; i++) {
            final StateStoreMetadata meta = mock(StateStoreMetadata.class);
            final ProcessorStateManager manager = mock(ProcessorStateManager.class);
            final StateStore store = mock(StateStore.class);
            when(meta.changelogPartition()).thenReturn(tps[i]);
            when(meta.store()).thenReturn(store);
            when(meta.offset()).thenReturn(null, 0L);   // no checkpoint, then a value once restoring
            when(meta.retentionPeriod()).thenReturn(shortRetentionMs);
            when(store.name()).thenReturn(storeName);
            when(manager.storeMetadata(tps[i])).thenReturn(meta);
            when(manager.taskType()).thenReturn(ACTIVE);
            when(manager.taskId()).thenReturn(new TaskId(0, i));
            probeReader.register(tps[i], manager);
        }

        final Map<TaskId, Task> probeTasks = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            probeTasks.put(new TaskId(0, i), mock(Task.class));
        }
        probeReader.restore(probeTasks);

        assertEquals(Collections.emptySet(), seekedToBeginning,
            "no time passed while those polls came back empty, so none of them waited on a fetch; "
                + "abandoning the window on that basis sends partitions to a log-start restore");
    }

    /**
     * After a producer fence the restore consumer answers nothing for a while. Here twelve polls
     * each block a full poll timeout and return empty before the window finally answers, which is
     * shorter than the slowest probes measured on a soak, so none of it may be called an empty
     * window.
     */
    @Test
    public void shouldWaitOutAnUnreadyConsumerBeforeAbandoningTheWindow() {
        final long shortRetentionMs = Duration.ofSeconds(3).toMillis();
        final long beginOffset = 900_000L;
        final long logEndOffset = 1_000_000L;
        final long seekTarget = 999_000L;
        final int numPartitions = 6;
        final int unreadyPolls = 12;       // 1.2s at the default poll.ms, all of it fruitless

        final TopicPartition[] tps = new TopicPartition[numPartitions];
        final Map<TopicPartition, Long> begins = new HashMap<>();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            tps[i] = new TopicPartition(tp.topic(), i);
            begins.put(tps[i], beginOffset);
            ends.put(tps[i], logEndOffset);
        }

        final Set<TopicPartition> seekedToBeginning = new HashSet<>();
        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                        final Map<TopicPartition, Long> timestampsToSearch) {
                    final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                    timestampsToSearch.forEach((k, v) ->
                        result.put(k, new OffsetAndTimestamp(seekTarget, v)));
                    return result;
                }

                @Override
                public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {
                    seekedToBeginning.addAll(partitions);
                    super.seekToBeginning(partitions);
                }
            };
        probeConsumer.updateBeginningOffsets(begins);
        probeConsumer.updateEndOffsets(ends);
        adminClient.updateEndOffsets(ends);

        final long pollMs = config.getLong(StreamsConfig.POLL_MS_CONFIG);
        final int[] answered = {0};
        for (int round = 0; round < 60; round++) {
            probeConsumer.schedulePollTask(() -> {
                if (!probeConsumer.assignment().contains(tps[0])) {
                    return;
                }
                if (++answered[0] <= unreadyPolls) {
                    time.sleep(pollMs);      // the poll blocked its full timeout and found nothing
                    return;
                }
                for (final TopicPartition partition : tps) {
                    probeConsumer.addRecord(new ConsumerRecord<>(
                        partition.topic(), partition.partition(), logEndOffset - 1,
                        10_000_000L, TimestampType.CREATE_TIME,
                        0, 0, new byte[0], new byte[0], new RecordHeaders(), Optional.empty()));
                }
            });
        }

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        for (int i = 0; i < numPartitions; i++) {
            final StateStoreMetadata meta = mock(StateStoreMetadata.class);
            final ProcessorStateManager manager = mock(ProcessorStateManager.class);
            final StateStore store = mock(StateStore.class);
            when(meta.changelogPartition()).thenReturn(tps[i]);
            when(meta.store()).thenReturn(store);
            when(meta.offset()).thenReturn(null, 0L);   // no checkpoint, then a value once restoring
            when(meta.retentionPeriod()).thenReturn(shortRetentionMs);
            when(store.name()).thenReturn(storeName);
            when(manager.storeMetadata(tps[i])).thenReturn(meta);
            when(manager.taskType()).thenReturn(ACTIVE);
            when(manager.taskId()).thenReturn(new TaskId(0, i));
            probeReader.register(tps[i], manager);
        }

        final Map<TaskId, Task> probeTasks = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            probeTasks.put(new TaskId(0, i), mock(Task.class));
        }
        probeReader.restore(probeTasks);

        assertEquals(Collections.emptySet(), seekedToBeginning,
            "the consumer was not ready to answer for 1.2s, which is inside the window the probe "
                + "owes before it may conclude anything; these were abandoned early");
    }

    /**
     * A task corrupted, wiped and re-registered with no offset would otherwise pay a full probe on
     * every iteration. A probe that fell back is left alone until the backoff expires, which bounds
     * a loop to one probe per interval without suppressing a probe that was working.
     */
    @Test
    public void shouldBackOffProbingAPartitionWhoseProbeJustFailed() {
        final long retentionMs = Duration.ofSeconds(3).toMillis();
        final long endOffset = 1_000L;
        final int[] probeRounds = {0};

        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
                    probeRounds[0]++;      // only the probe looks these up
                    return super.beginningOffsets(partitions);
                }
            };
        probeConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        probeConsumer.updateEndOffsets(Collections.singletonMap(tp, endOffset));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, endOffset));
        // no records are ever scheduled, so no window can answer and every probe falls back

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        final TaskId probeTaskId = new TaskId(0, 0);
        final Map<TaskId, Task> probeTasks = Collections.singletonMap(probeTaskId, mock(Task.class));

        for (int round = 0; round < 5; round++) {
            registerRestoreAndRevoke(probeReader, probeTaskId, probeTasks, retentionMs);
        }
        assertEquals(1, probeRounds[0],
            "the probe fell back, so re-registering in a loop should not probe again");

        time.sleep(Duration.ofSeconds(60).toMillis());     // PROBE_RETRY_BACKOFF
        registerRestoreAndRevoke(probeReader, probeTaskId, probeTasks, retentionMs);
        assertEquals(2, probeRounds[0],
            "once the backoff expires the partition is probed again, so a transient failure does "
                + "not disable the optimisation for good");
    }

    private void registerRestoreAndRevoke(final StoreChangelogReader probeReader,
                                          final TaskId probeTaskId,
                                          final Map<TaskId, Task> probeTasks,
                                          final long retentionMs) {
        final StateStoreMetadata meta = mock(StateStoreMetadata.class);
        final ProcessorStateManager manager = mock(ProcessorStateManager.class);
        final StateStore store = mock(StateStore.class);
        when(meta.changelogPartition()).thenReturn(tp);
        when(meta.store()).thenReturn(store);
        when(meta.offset()).thenReturn(null, 0L);
        when(meta.retentionPeriod()).thenReturn(retentionMs);
        when(store.name()).thenReturn(storeName);
        when(manager.storeMetadata(tp)).thenReturn(meta);
        when(manager.taskType()).thenReturn(ACTIVE);
        when(manager.taskId()).thenReturn(probeTaskId);

        probeReader.register(tp, manager);
        probeReader.restore(probeTasks);
        probeReader.unregister(Collections.singleton(tp));
    }

    /**
     * A probe can also fail by the offset lookup timing out, which leaves through a catch rather
     * than the normal path. That has to arm the backoff too, or a task looping on a slow broker
     * re-probes on every iteration.
     */
    @Test
    public void shouldBackOffWhenTheProbeFailsByTimeout() {
        final long retentionMs = Duration.ofSeconds(3).toMillis();
        final long endOffset = 1_000L;
        final int[] lookups = {0};

        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                    lookups[0]++;
                    throw new TimeoutException("timed out looking up end offsets");
                }
            };
        probeConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        probeConsumer.updateEndOffsets(Collections.singletonMap(tp, endOffset));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, endOffset));

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        final TaskId probeTaskId = new TaskId(0, 0);
        final Map<TaskId, Task> probeTasks = Collections.singletonMap(probeTaskId, mock(Task.class));

        for (int round = 0; round < 4; round++) {
            registerRestoreAndRevoke(probeReader, probeTaskId, probeTasks, retentionMs);
        }

        assertEquals(1, lookups[0],
            "the probe failed by timeout, so re-registering in a loop should not probe again");
    }

    /**
     * Only a probe that fell back arms the backoff. Suppressing a probe that was working would send
     * the next restore to log start, which is what gets a partition lapped and corrupted in the
     * first place -- the guard would sustain the loop it exists to bound.
     */
    @Test
    public void shouldNotBackOffAfterAProbeThatSucceeded() {
        final long retentionMs = Duration.ofSeconds(3).toMillis();
        final long endOffset = 1_000L;
        final int[] probeRounds = {0};

        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
                    probeRounds[0]++;
                    return super.beginningOffsets(partitions);
                }

                @Override
                public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                        final Map<TopicPartition, Long> timestampsToSearch) {
                    final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                    timestampsToSearch.forEach((k, v) -> result.put(k, new OffsetAndTimestamp(1L, v)));
                    return result;
                }
            };
        probeConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        probeConsumer.updateEndOffsets(Collections.singletonMap(tp, endOffset));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, endOffset));

        for (int round = 0; round < 40; round++) {
            probeConsumer.schedulePollTask(() -> {
                if (probeConsumer.assignment().contains(tp)) {
                    probeConsumer.addRecord(new ConsumerRecord<>(
                        tp.topic(), tp.partition(), endOffset - 1, 10_000_000L, TimestampType.CREATE_TIME,
                        0, 0, new byte[0], new byte[0], new RecordHeaders(), Optional.empty()));
                }
            });
        }

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        final TaskId probeTaskId = new TaskId(0, 0);
        final Map<TaskId, Task> probeTasks = Collections.singletonMap(probeTaskId, mock(Task.class));

        // back to back, with no time passing at all
        registerRestoreAndRevoke(probeReader, probeTaskId, probeTasks, retentionMs);
        registerRestoreAndRevoke(probeReader, probeTaskId, probeTasks, retentionMs);

        assertEquals(2, probeRounds[0],
            "the probe answered, so nothing should be held back on the next registration");
    }

    /**
     * The newest timestamp sits in the middle of the window, so taking the first or the last record
     * in offset order picks the wrong one. The probe estimates observed stream time, a maximum.
     */
    @Test
    public void shouldSeekFromTheNewestTimestampInTheProbedWindow() {
        final long retentionMs = Duration.ofSeconds(30).toMillis();
        final long logEndOffset = 1_000L;
        final long oldest = 500_000L;
        final long newest = 900_000L;
        final long middle = 700_000L;    // last in offset order, but not the newest

        final Map<TopicPartition, Long> requested = new HashMap<>();
        final MockConsumer<byte[], byte[]> probeConsumer =
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                        final Map<TopicPartition, Long> timestampsToSearch) {
                    requested.putAll(timestampsToSearch);
                    final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                    timestampsToSearch.forEach((k, v) -> result.put(k, new OffsetAndTimestamp(1L, v)));
                    return result;
                }
            };
        probeConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        probeConsumer.updateEndOffsets(Collections.singletonMap(tp, logEndOffset));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, logEndOffset));

        for (int round = 0; round < 8; round++) {
            probeConsumer.schedulePollTask(() -> {
                if (!probeConsumer.assignment().contains(tp)) {
                    return;
                }
                long offset = logEndOffset - 3;
                for (final long timestamp : new long[] {oldest, newest, middle}) {
                    probeConsumer.addRecord(new ConsumerRecord<>(
                        tp.topic(), tp.partition(), offset++, timestamp, TimestampType.CREATE_TIME,
                        0, 0, new byte[0], new byte[0], new RecordHeaders(), Optional.empty()));
                }
            });
        }

        final StoreChangelogReader probeReader = new StoreChangelogReader(
            time, config, logContext, adminClient, probeConsumer, callback, standbyListener);

        final TaskId probeTaskId = new TaskId(0, 0);
        final StateStoreMetadata meta = mock(StateStoreMetadata.class);
        final ProcessorStateManager manager = mock(ProcessorStateManager.class);
        final StateStore store = mock(StateStore.class);
        when(meta.changelogPartition()).thenReturn(tp);
        when(meta.store()).thenReturn(store);
        when(meta.offset()).thenReturn(null, 0L);   // no checkpoint, then a value once restoring
        when(meta.retentionPeriod()).thenReturn(retentionMs);
        when(store.name()).thenReturn(storeName);
        when(manager.storeMetadata(tp)).thenReturn(meta);
        when(manager.taskType()).thenReturn(ACTIVE);
        when(manager.taskId()).thenReturn(probeTaskId);
        probeReader.register(tp, manager);

        probeReader.restore(Collections.singletonMap(probeTaskId, mock(Task.class)));

        assertEquals(newest - retentionMs, requested.get(tp).longValue(),
            "the seek must be derived from the newest timestamp in the window (" + newest
                + "), not the first or last record in offset order");
    }

    @Test
    public void shouldSeekByTimestampForWindowedStoreWithoutCheckpoint() {
        final long retentionMs = Duration.ofHours(2).toMillis();
        final long offsetForTimestamp = 42L;
        final long latestRecordTimestamp = 10_000_000L;
        final long endOffset = 100L;

        final MockConsumer<byte[], byte[]> timestampConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
                final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                timestampsToSearch.forEach((key, value) -> result.put(key, new OffsetAndTimestamp(offsetForTimestamp, value)));
                return result;
            }
        };

        final StateStoreMetadata windowStoreMetadata = mock(StateStoreMetadata.class);
        final ProcessorStateManager windowStateManager = mock(ProcessorStateManager.class);
        final StateStore windowStore = mock(StateStore.class);
        when(windowStoreMetadata.changelogPartition()).thenReturn(tp);
        when(windowStoreMetadata.store()).thenReturn(windowStore);
        when(windowStoreMetadata.offset()).thenReturn(null);
        when(windowStoreMetadata.retentionPeriod()).thenReturn(retentionMs);
        when(windowStore.name()).thenReturn(storeName);
        when(windowStateManager.storeMetadata(tp)).thenReturn(windowStoreMetadata);
        when(windowStateManager.taskType()).thenReturn(ACTIVE);

        final TaskId taskId = new TaskId(0, 0);
        when(windowStateManager.taskId()).thenReturn(taskId);

        timestampConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        timestampConsumer.updateEndOffsets(Collections.singletonMap(tp, endOffset));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, endOffset));

        // schedule adding the record during poll, after the partition is assigned
        timestampConsumer.schedulePollTask(() -> timestampConsumer.addRecord(new ConsumerRecord<>(
            tp.topic(), tp.partition(), endOffset - 1,
            latestRecordTimestamp, TimestampType.CREATE_TIME,
            0, 0, new byte[0], new byte[0],
            new RecordHeaders(), Optional.empty())));

        final StoreChangelogReader reader =
            new StoreChangelogReader(time, config, logContext, adminClient, timestampConsumer, callback, standbyListener);

        reader.register(tp, windowStateManager);
        reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(offsetForTimestamp, timestampConsumer.position(tp), "The consumer should be seeked to the offset returned by offsetsForTimes, not to the beginning");
    }

    @Test
    public void shouldSeekToBeginningWhenBrokerReturnsNullForOffsetsForTimes() {
        final long retentionMs = Duration.ofHours(2).toMillis();
        final long latestRecordTimestamp = 10_000_000L;
        final long endOffset = 100L;

        final MockConsumer<byte[], byte[]> timestampConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
                final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                timestampsToSearch.forEach((key, value) -> result.put(key, null));
                return result;
            }
        };

        final StateStoreMetadata windowStoreMetadata = mock(StateStoreMetadata.class);
        final ProcessorStateManager windowStateManager = mock(ProcessorStateManager.class);
        final StateStore windowStore = mock(StateStore.class);
        when(windowStoreMetadata.changelogPartition()).thenReturn(tp);
        when(windowStoreMetadata.store()).thenReturn(windowStore);
        when(windowStoreMetadata.offset()).thenReturn(null);
        when(windowStoreMetadata.retentionPeriod()).thenReturn(retentionMs);
        when(windowStore.name()).thenReturn(storeName);
        when(windowStateManager.storeMetadata(tp)).thenReturn(windowStoreMetadata);
        when(windowStateManager.taskType()).thenReturn(ACTIVE);

        final TaskId taskId = new TaskId(0, 0);
        when(windowStateManager.taskId()).thenReturn(taskId);

        timestampConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        timestampConsumer.updateEndOffsets(Collections.singletonMap(tp, endOffset));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, endOffset));

        // schedule adding the record during poll, after the partition is assigned
        timestampConsumer.schedulePollTask(() -> timestampConsumer.addRecord(new ConsumerRecord<>(
            tp.topic(), tp.partition(), endOffset - 1,
            latestRecordTimestamp, TimestampType.CREATE_TIME,
            0, 0, new byte[0], new byte[0],
            new RecordHeaders(), Optional.empty())));

        final StoreChangelogReader reader =
            new StoreChangelogReader(time, config, logContext, adminClient, timestampConsumer, callback, standbyListener);

        reader.register(tp, windowStateManager);
        reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(0L, timestampConsumer.position(tp), "When broker returns null, should fall back to seeking to the beginning");
    }

    @Test
    public void shouldSeekToBeginningForNonWindowedStoreWithoutCheckpoint() {
        final StateStoreMetadata kvStoreMetadata = mock(StateStoreMetadata.class);
        final ProcessorStateManager kvStateManager = mock(ProcessorStateManager.class);
        final StateStore kvStore = mock(StateStore.class);
        when(kvStoreMetadata.changelogPartition()).thenReturn(tp);
        when(kvStoreMetadata.store()).thenReturn(kvStore);
        when(kvStoreMetadata.offset()).thenReturn(null);
        when(kvStoreMetadata.retentionPeriod()).thenReturn(-1L);
        when(kvStore.name()).thenReturn(storeName);
        when(kvStateManager.storeMetadata(tp)).thenReturn(kvStoreMetadata);
        when(kvStateManager.taskType()).thenReturn(ACTIVE);

        final TaskId taskId = new TaskId(0, 0);
        when(kvStateManager.taskId()).thenReturn(taskId);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, 100L));

        final StoreChangelogReader reader =
            new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);

        reader.register(tp, kvStateManager);
        reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(0L, consumer.position(tp), "Non-windowed store should seek to beginning, not by timestamp");
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
