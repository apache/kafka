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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockStandbyUpdateListener;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.Task.TaskType.ACTIVE;
import static org.apache.kafka.streams.processor.internals.Task.TaskType.STANDBY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StoreChangelogReaderCaughtUpOffsetTest {

    private final String storeName = "store";
    private final String topicName = "topic";
    private final LogContext logContext = new LogContext("test-reader ");
    private final TopicPartition tp = new TopicPartition(topicName, 0);
    private final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader"));
    private final MockTime time = new MockTime();
    private final MockStateRestoreListener callback = new MockStateRestoreListener();
    private final MockStandbyUpdateListener standbyListener = new MockStandbyUpdateListener();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
    private final MockAdminClient adminClient = new MockAdminClient();

    @Test
    public void shouldAdvertiseZeroAfterActiveRestoreOfEmptyChangelogWithZeroEndOffset() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 0L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 0L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, reader.changelogMetadata(tp).state());
            assertNull(stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 0L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldNotAdvanceActiveOffsetOnEmptyPollWhileStillBehindEndOffset() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 20_000L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 20_000L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, reader.changelogMetadata(tp).state());
            assertNull(stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 0L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldAdvertiseRestoreEndOffsetNotLivePositionWhenActiveCompletesPastEnd() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            // Compacted log start is beyond the restore snapshot; live position is 15_000, restore end is 10_000.
            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 15_000L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 15_000L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 10_000L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, reader.changelogMetadata(tp).state());
            assertEquals(15_000L, consumer.position(tp));
            assertEquals(Collections.singletonMap(tp, 10_000L), stateManager.changelogOffsets());
            assertEquals(9_999L, stateManager.storeMetadata(tp).offset());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldNotAdvanceActiveOffsetWhenPositionIsBehindRestoreEndOffset() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 15_000L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 20_000L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 20_000L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, reader.changelogMetadata(tp).state());
            assertEquals(15_000L, consumer.position(tp));
            assertNull(stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 0L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldNotAdvanceStandbyOffsetOnEmptyPollWhileLagIsPositive() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(STANDBY, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 20_000L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 20_000L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.transitToUpdateStandby();
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, reader.changelogMetadata(tp).state());
            assertNull(stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 0L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldNotAdvertiseBufferedSourceChangelogRecordsBeyondCommittedLimit() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(STANDBY, stateDir, true);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 12L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 12L));
            adminClient.updateConsumerGroupOffsets(Collections.singletonMap(tp, 7L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.transitToUpdateStandby();
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            final byte[] keyBytes = new byte[] {0x0, 0x0, 0x0, 0x1};
            for (long offset = 5L; offset <= 11L; offset++) {
                consumer.addRecord(new ConsumerRecord<>(topicName, 0, offset, keyBytes, "value".getBytes()));
            }
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertTrue(reader.changelogMetadata(tp).bufferedRecords().size() > 0);
            assertEquals(7L, (long) reader.changelogMetadata(tp).endOffset());
            // Applied offsets 5 and 6 (strictly below the committed limit); do not advertise buffered 7+.
            assertEquals(Collections.singletonMap(tp, 7L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldNotAdvanceStandbyOffsetWhenPositionTimesOut() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(STANDBY, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        final MockConsumer<byte[], byte[]> timeoutConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public long position(final TopicPartition partition) {
                throw new TimeoutException("KABOOM!");
            }
        };
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            timeoutConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 20_000L));
            timeoutConsumer.updateEndOffsets(Collections.singletonMap(tp, 20_000L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 20_000L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, timeoutConsumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.transitToUpdateStandby();
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertNull(stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 0L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldReportZeroTaskLagAfterEmptyHighEndOffsetRestore() throws IOException {
        final long changelogEndOffset = 20_000L;
        final long acceptableRecoveryLag = 10_000L;
        final TaskId taskId = new TaskId(0, 0);
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(STANDBY, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, changelogEndOffset));
            consumer.updateEndOffsets(Collections.singletonMap(tp, changelogEndOffset));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, changelogEndOffset));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.transitToUpdateStandby();
            reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

            assertEquals(changelogEndOffset - 1L, stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, changelogEndOffset), stateManager.changelogOffsets());

            final long taskOffsetSum = StateDirectory.sumOfChangelogOffsets(taskId, stateManager.changelogOffsets());
            assertEquals(changelogEndOffset, taskOffsetSum);

            final ClientState restored = new ClientState();
            restored.addPreviousTasksAndOffsetSums("c1", Collections.singletonMap(taskId, taskOffsetSum));
            restored.computeTaskLags(null, Collections.singletonMap(taskId, changelogEndOffset));
            assertEquals(0L, restored.lagFor(taskId));
            assertTrue(restored.lagFor(taskId) <= acceptableRecoveryLag);

            final ClientState unrestored = new ClientState();
            unrestored.addPreviousTasksAndOffsetSums("c1", Collections.singletonMap(taskId, 0L));
            unrestored.computeTaskLags(null, Collections.singletonMap(taskId, changelogEndOffset));
            assertEquals(changelogEndOffset, unrestored.lagFor(taskId));
            assertTrue(unrestored.lagFor(taskId) > acceptableRecoveryLag);
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldKeepLastAppliedOffsetAfterNonEmptyActiveRestore() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 8L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 8L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            final byte[] keyBytes = new byte[] {0x0, 0x0, 0x0, 0x1};
            for (long offset = 0L; offset < 8L; offset++) {
                consumer.addRecord(new ConsumerRecord<>(topicName, 0, offset, keyBytes, "value".getBytes()));
            }
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));

            assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, reader.changelogMetadata(tp).state());
            assertEquals(7L, stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 8L), stateManager.changelogOffsets());
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldAccountForTrailingOffsetHolesBeforeAdvancingCaughtUpOffset() throws IOException {
        // end offset is 10 but only records 0..7 are restored; 8 and 9 are trailing holes the restore
        // consumer never returns. remaining-records must decrement from the pre-advance last-applied
        // offset (7), then the catch-up helper may set last-applied to 9 so changelogOffsets() == 10.
        final File stateDir = TestUtils.tempDirectory();
        final ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        final MockKeyValueStore kvStore = new MockKeyValueStore(storeName, true);
        final Task mockTask = mock(Task.class);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            consumer.updateEndOffsets(Collections.singletonMap(tp, 10L));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, 10L));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);

            reader.restore(Collections.singletonMap(new TaskId(0, 0), mockTask));
            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, reader.changelogMetadata(tp).state());

            final byte[] keyBytes = new byte[] {0x0, 0x0, 0x0, 0x1};
            for (long offset = 0L; offset < 8L; offset++) {
                consumer.addRecord(new ConsumerRecord<>(topicName, 0, offset, keyBytes, "value".getBytes()));
            }
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mockTask));
            assertEquals(8L, reader.changelogMetadata(tp).totalRestored());
            assertEquals(7L, stateManager.storeMetadata(tp).offset());
            assertEquals(StoreChangelogReader.ChangelogState.RESTORING, reader.changelogMetadata(tp).state());

            consumer.seek(tp, 10L);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mockTask));

            assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, reader.changelogMetadata(tp).state());
            assertEquals(8L, reader.changelogMetadata(tp).totalRestored());
            assertEquals(9L, stateManager.storeMetadata(tp).offset());
            assertEquals(Collections.singletonMap(tp, 10L), stateManager.changelogOffsets());

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
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldResumeAtLogEndOffsetAfterCheckpointingEmptyHighEndOffsetRestore() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final long changelogEndOffset = 20_000L;
        final OffsetTrackingStore kvStore = new OffsetTrackingStore(storeName);
        ProcessorStateManager stateManager = newRealStateManager(ACTIVE, stateDir, false);
        try {
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);

            consumer.updateBeginningOffsets(Collections.singletonMap(tp, changelogEndOffset));
            consumer.updateEndOffsets(Collections.singletonMap(tp, changelogEndOffset));
            adminClient.updateEndOffsets(Collections.singletonMap(tp, changelogEndOffset));

            final StoreChangelogReader reader =
                new StoreChangelogReader(time, config, logContext, adminClient, consumer, callback, standbyListener);
            reader.register(tp, stateManager);
            reader.restore(Collections.singletonMap(new TaskId(0, 0), mock(Task.class)));
            assertEquals(Collections.singletonMap(tp, changelogEndOffset), stateManager.changelogOffsets());

            stateManager.commit();
            stateManager.close();

            stateManager = newRealStateManager(ACTIVE, stateDir, false);
            stateManager.registerStore(kvStore, kvStore.stateRestoreCallback, null);
            stateManager.initializeStoreOffsets(true);
            assertEquals(
                Collections.singletonMap(tp, changelogEndOffset),
                stateManager.changelogOffsets(),
                "re-init from checkpoint must resume at LEO, not 0"
            );
        } finally {
            stateManager.close();
            Utils.delete(stateDir);
        }
    }

    private ProcessorStateManager newRealStateManager(final Task.TaskType type,
                                                      final File stateDir,
                                                      final boolean sourceChangelog) {
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
        return new ProcessorStateManager(
            new TaskId(0, 0),
            type,
            false,
            false,
            logContext,
            stateDirectory,
            time,
            mkMap(mkEntry(storeName, topicName)),
            sourceChangelog ? Set.of(tp) : Collections.emptySet()
        );
    }

    private static final class OffsetTrackingStore extends MockKeyValueStore {
        private final Map<TopicPartition, Long> committed = new HashMap<>();

        OffsetTrackingStore(final String name) {
            super(name, true);
        }

        @Override
        @SuppressWarnings("deprecation")
        public boolean managesOffsets() {
            return true;
        }

        @Override
        public void commit(final Map<TopicPartition, Long> changelogOffsets) {
            super.commit(changelogOffsets);
            committed.clear();
            committed.putAll(changelogOffsets);
        }

        @Override
        public Long committedOffset(final TopicPartition partition) {
            return committed.get(partition);
        }
    }
}
