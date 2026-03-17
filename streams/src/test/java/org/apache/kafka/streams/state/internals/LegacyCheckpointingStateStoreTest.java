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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.state.internals.LegacyCheckpointingStateStore.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.state.internals.LegacyCheckpointingStateStore.OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LegacyCheckpointingStateStore}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class LegacyCheckpointingStateStoreTest {

    private static final String APPLICATION_ID = "test-application";
    private static final String LOG_PREFIX = "test-prefix ";
    private static final String STORE_NAME = "test-store";
    private static final String CHANGELOG_TOPIC = "test-topic";

    private final TopicPartition partition = new TopicPartition(CHANGELOG_TOPIC, 0);
    private final TaskId taskId = new TaskId(0, 0);

    private File baseDir;
    private StateDirectory stateDirectory;
    private MockKeyValueStore persistentStore;

    @Mock
    private StateStoreContext context;

    @BeforeEach
    public void setUp() {
        baseDir = TestUtils.tempDirectory();
        stateDirectory = new StateDirectory(new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, baseDir.getPath());
            }
        }), new MockTime(), true, true);
        persistentStore = new MockKeyValueStore(STORE_NAME, true);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(baseDir);
    }

    // =====================================================================
    // maybeWrapStore()
    // =====================================================================

    @Test
    public void shouldWrapPersistentNonOffsetManagingStore() {
        final StateStore result = LegacyCheckpointingStateStore.maybeWrapStore(
            persistentStore, false, Set.of(partition), stateDirectory, taskId, LOG_PREFIX);

        assertThat(result, instanceOf(LegacyCheckpointingStateStore.class));
    }

    @Test
    public void shouldNotWrapNonPersistentStore() {
        final MockKeyValueStore nonPersistentStore = new MockKeyValueStore(STORE_NAME, false);

        final StateStore result = LegacyCheckpointingStateStore.maybeWrapStore(
            nonPersistentStore, false, Set.of(partition), stateDirectory, taskId, LOG_PREFIX);

        assertThat(result, not(instanceOf(LegacyCheckpointingStateStore.class)));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldNotWrapOffsetManagingStore() {
        final MockKeyValueStore offsetManagingStore = new MockKeyValueStore(STORE_NAME, true) {
            @Override
            public boolean managesOffsets() {
                return true;
            }
        };

        final StateStore result = LegacyCheckpointingStateStore.maybeWrapStore(
            offsetManagingStore, false, Set.of(partition), stateDirectory, taskId, LOG_PREFIX);

        assertThat(result, not(instanceOf(LegacyCheckpointingStateStore.class)));
    }

    // =====================================================================
    // maybeUnwrapStore()
    // =====================================================================

    @Test
    public void shouldUnwrapLCSSToInnerStore() {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> lcss = createStore(false);

        final StateStore result = LegacyCheckpointingStateStore.maybeUnwrapStore(lcss);

        assertEquals(persistentStore, result);
    }

    @Test
    public void shouldReturnNonLCSSStoreUnchangedFromMaybeUnwrap() {
        final StateStore result = LegacyCheckpointingStateStore.maybeUnwrapStore(persistentStore);

        assertEquals(persistentStore, result);
    }

    // =====================================================================
    // maybeMarkCorrupted()
    // =====================================================================

    @Test
    public void shouldMarkLCSSAsCorruptedSoCloseDoesNotWriteCheckpoint() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);

        LegacyCheckpointingStateStore.maybeMarkCorrupted(store);
        // commit() won't write checkpoint either: checkpointedOffsets is null -> checkpointNeeded returns false
        store.commit(Collections.singletonMap(partition, 100L));
        store.close();

        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldBeNoOpForNonLCSSStoreInMaybeMarkCorrupted() {
        // Should not throw
        LegacyCheckpointingStateStore.maybeMarkCorrupted(persistentStore);
    }

    // =====================================================================
    // checkpointFileFor() — static helper
    // =====================================================================

    @Test
    public void shouldReturnPerStoreCheckpointFileForTask() {
        final File result = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, persistentStore);
        final File expected = new File(
            stateDirectory.getOrCreateDirectoryForTask(taskId),
            CHECKPOINT_FILE_NAME + "_" + STORE_NAME
        );

        assertEquals(expected, result);
    }

    @Test
    public void shouldReturnLegacyPerTaskCheckpointFile() {
        final File result = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, null);
        final File expected = new File(
            stateDirectory.getOrCreateDirectoryForTask(taskId),
            CHECKPOINT_FILE_NAME
        );

        assertEquals(expected, result);
    }

    @Test
    public void shouldReturnPerStoreCheckpointFileForGlobalStore() {
        // globalStateDir() = new File(new File(baseDir, APPLICATION_ID), "global")
        final File expectedGlobalDir = new File(new File(baseDir, APPLICATION_ID), "global");
        final File expected = new File(expectedGlobalDir, CHECKPOINT_FILE_NAME + "_" + STORE_NAME);

        final File result = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, null, persistentStore);

        assertEquals(expected, result);
    }

    @Test
    public void shouldReturnLegacyGlobalCheckpointFile() {
        final File expectedGlobalDir = new File(new File(baseDir, APPLICATION_ID), "global");
        final File expected = new File(expectedGlobalDir, CHECKPOINT_FILE_NAME);

        final File result = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, null, null);

        assertEquals(expected, result);
    }

    // =====================================================================
    // checkpointNeeded() — static helper
    // =====================================================================

    @Test
    public void shouldReturnFalseFromCheckpointNeededWhenOldSnapshotIsNull() {
        assertFalse(LegacyCheckpointingStateStore.checkpointNeeded(
            null, Collections.singletonMap(partition, 100L)));
    }

    @Test
    public void shouldReturnFalseFromCheckpointNeededWhenDeltaEqualsThreshold() {
        // delta == threshold → NOT greater than → no checkpoint needed
        final Map<TopicPartition, Long> oldOffsets = Collections.emptyMap();
        final Map<TopicPartition, Long> newOffsets = Collections.singletonMap(
            partition, OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT);

        assertFalse(LegacyCheckpointingStateStore.checkpointNeeded(oldOffsets, newOffsets));
    }

    @Test
    public void shouldReturnFalseFromCheckpointNeededWhenDeltaBelowThreshold() {
        final Map<TopicPartition, Long> oldOffsets = Collections.emptyMap();
        final Map<TopicPartition, Long> newOffsets = Collections.singletonMap(
            partition, OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT / 2);

        assertFalse(LegacyCheckpointingStateStore.checkpointNeeded(oldOffsets, newOffsets));
    }

    @Test
    public void shouldReturnTrueFromCheckpointNeededWhenDeltaExceedsThreshold() {
        final Map<TopicPartition, Long> oldOffsets = Collections.emptyMap();
        final Map<TopicPartition, Long> newOffsets = Collections.singletonMap(
            partition, OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT + 1L);

        assertTrue(LegacyCheckpointingStateStore.checkpointNeeded(oldOffsets, newOffsets));
    }

    // =====================================================================
    // init()
    // =====================================================================

    @Test
    public void shouldSucceedOnInitWhenNoCheckpointFileExists() {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);

        assertNull(store.committedOffset(partition));
        assertTrue(persistentStore.initialized);
    }

    @Test
    public void shouldLoadOffsetsFromCheckpointFileOnInit() throws IOException {
        final long expectedOffset = 10L;
        writeCheckpointFile(Collections.singletonMap(partition, expectedOffset));

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);

        assertEquals(expectedOffset, store.committedOffset(partition));
        assertTrue(persistentStore.initialized);
    }

    @Test
    public void shouldIgnoreCheckpointEntriesForPartitionsNotInChangelogSet() throws IOException {
        final TopicPartition irrelevantPartition = new TopicPartition("irrelevant-topic", 0);
        final Map<TopicPartition, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(partition, 10L);
        checkpointOffsets.put(irrelevantPartition, 999L);
        writeCheckpointFile(checkpointOffsets);

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);

        assertEquals(10L, store.committedOffset(partition));
        assertNull(store.committedOffset(irrelevantPartition));
    }

    @Test
    public void shouldReturnNullForPartitionNotInCheckpointOnInit() throws IOException {
        // write a checkpoint with no entry for our partition
        writeCheckpointFile(Collections.emptyMap());

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);

        assertNull(store.committedOffset(partition));
    }

    @Test
    public void shouldNotDeleteCheckpointFileAfterInitUnderALOS() throws IOException {
        writeCheckpointFile(Collections.singletonMap(partition, 10L));

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);

        assertTrue(storeCheckpointFile().exists());
    }

    @Test
    public void shouldDeleteCheckpointFileAfterInitUnderEOS() throws IOException {
        writeCheckpointFile(Collections.singletonMap(partition, 10L));

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(true);
        store.init(context, persistentStore);

        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnCorruptCheckpointFile() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        final File file = storeCheckpointFile();
        Files.write(file.toPath(), "abcdefg".getBytes());

        assertThrows(ProcessorStateException.class, () -> store.init(context, persistentStore));
    }

    // =====================================================================
    // managesOffsets() / committedOffset()
    // =====================================================================

    @Test
    @SuppressWarnings("deprecation")
    public void shouldReturnTrueForManagesOffsets() {
        assertTrue(createStore(false).managesOffsets());
    }

    @Test
    public void shouldReturnNullForCommittedOffsetWhenNoneCommitted() {
        assertNull(createStore(false).committedOffset(partition));
    }

    @Test
    public void shouldReturnCommittedOffset() {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.commit(Collections.singletonMap(partition, 42L));

        assertEquals(42L, store.committedOffset(partition));
    }

    // =====================================================================
    // commit()
    // =====================================================================

    @Test
    public void shouldUpdateInMemoryOffsetsOnCommit() {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.commit(Collections.singletonMap(partition, 100L));

        assertEquals(100L, store.committedOffset(partition));
    }

    @Test
    public void shouldPreserveExistingOffsetsWhenCommittingSubsetOfPartitions() {
        final TopicPartition partitionTwo = new TopicPartition(CHANGELOG_TOPIC, 1);
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store =
            new LegacyCheckpointingStateStore<>(
                persistentStore, false, Set.of(partition, partitionTwo), stateDirectory, taskId, LOG_PREFIX);

        store.commit(Collections.singletonMap(partition, 100L));
        store.commit(Collections.singletonMap(partitionTwo, 200L));

        assertEquals(100L, store.committedOffset(partition));
        assertEquals(200L, store.committedOffset(partitionTwo));
    }

    @Test
    public void shouldWriteCheckpointDuringCommitUnderALOSWhenDeltaExceedsThreshold() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore); // sets checkpointedOffsets = {}

        final long offsetBeyondThreshold = OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT + 1L;
        store.commit(Collections.singletonMap(partition, offsetBeyondThreshold));

        final Map<TopicPartition, Long> checkpointed = readCheckpointFile();
        assertEquals(offsetBeyondThreshold, checkpointed.get(partition));
    }

    @Test
    public void shouldNotWriteCheckpointDuringCommitUnderALOSWhenDeltaDoesNotExceedThreshold() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore); // sets checkpointedOffsets = {}

        store.commit(Collections.singletonMap(partition, OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT));

        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldNotWriteCheckpointDuringCommitUnderEOS() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(true);
        store.init(context, persistentStore); // sets checkpointedOffsets = {} and deletes any pre-existing file

        final long offsetBeyondThreshold = OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT + 1L;
        store.commit(Collections.singletonMap(partition, offsetBeyondThreshold));

        // Under EOS, commit() never writes the checkpoint file
        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldAcceptNullOffsets() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);
        store.commit(Collections.singletonMap(partition, null));
        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldCommitWhenOldOffsetIsNull() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.init(context, persistentStore);

        store.commit(Collections.singletonMap(partition, null));

        final long offsetBeyondThreshold = OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT + 1L;
        store.commit(Collections.singletonMap(partition, offsetBeyondThreshold));

        assertTrue(storeCheckpointFile().exists());
    }

    // =====================================================================
    // checkpoint()
    // =====================================================================

    @Test
    public void shouldWriteCommittedOffsetsToCheckpointFile() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.commit(Collections.singletonMap(partition, 100L));
        store.checkpoint();

        final Map<TopicPartition, Long> checkpointed = readCheckpointFile();
        assertEquals(100L, checkpointed.get(partition));
    }

    @Test
    public void shouldRemoveOffsetWhenCommittedWithNull() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        // first commit a real offset
        store.commit(Collections.singletonMap(partition, 42L));
        store.checkpoint();
        assertEquals(42L, (long) readCheckpointFile().get(partition));

        // now commit null for the same partition — should remove it
        final Map<TopicPartition, Long> nullOffset = new HashMap<>();
        nullOffset.put(partition, null);
        store.commit(nullOffset);
        store.checkpoint();

        assertFalse(readCheckpointFile().containsKey(partition));
        assertNull(store.committedOffset(partition));
    }

    @Test
    public void shouldLogWarningInsteadOfThrowingWhenCheckpointWriteFails() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.commit(Collections.singletonMap(partition, 10L));

        // Delete the task directory so the checkpoint write will fail with IOException
        Utils.delete(stateDirectory.getOrCreateDirectoryForTask(taskId));

        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(LegacyCheckpointingStateStore.class)) {
            store.checkpoint(); // should log a warning, not throw

            assertThat(appender.getMessages(),
                hasItem(containsString("Failed to write offset checkpoint file")));
        }
    }

    @Test
    public void shouldNotWriteCheckpointWhenChangelogPartitionsIsEmpty() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store =
            new LegacyCheckpointingStateStore<>(
                persistentStore, false, Collections.emptySet(), stateDirectory, taskId, LOG_PREFIX);

        store.commit(Collections.singletonMap(partition, 100L));
        store.checkpoint();

        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistentStore() throws IOException {
        final MockKeyValueStore nonPersistentStore = new MockKeyValueStore(STORE_NAME, false);
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store =
            new LegacyCheckpointingStateStore<>(
                nonPersistentStore, false, Set.of(partition), stateDirectory, taskId, LOG_PREFIX);

        store.commit(Collections.singletonMap(partition, 100L));
        store.checkpoint();

        assertFalse(LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, nonPersistentStore).exists());
    }

    // =====================================================================
    // close()
    // =====================================================================

    @Test
    public void shouldWriteCheckpointOnClose() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.commit(Collections.singletonMap(partition, 100L));
        store.close();

        final Map<TopicPartition, Long> checkpointed = readCheckpointFile();
        assertEquals(100L, checkpointed.get(partition));
    }

    @Test
    public void shouldWriteCheckpointOnCloseUnderEOS() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(true);
        store.init(context, persistentStore); // deletes any pre-existing file; sets checkpointedOffsets
        store.commit(Collections.singletonMap(partition, 100L)); // no file write under EOS

        assertFalse(storeCheckpointFile().exists());

        store.close(); // writes checkpoint on close even under EOS

        final Map<TopicPartition, Long> checkpointed = readCheckpointFile();
        assertEquals(100L, checkpointed.get(partition));
    }

    @Test
    public void shouldNotWriteCheckpointOnCloseWhenCorrupted() throws IOException {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.commit(Collections.singletonMap(partition, 100L));
        store.markAsCorrupted();
        store.close();

        assertFalse(storeCheckpointFile().exists());
    }

    @Test
    public void shouldCloseWrappedStoreOnClose() {
        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        store.close();

        assertTrue(persistentStore.closed);
    }

    // =====================================================================
    // migrateLegacyOffsets()
    // =====================================================================

    @Test
    public void shouldMigrateOffsetsFromLegacyPerTaskFileAndDeleteIt() throws IOException {
        final File legacyFile = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, null);
        new OffsetCheckpoint(legacyFile).write(Collections.singletonMap(partition, 100L));
        assertTrue(legacyFile.exists());

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        LegacyCheckpointingStateStore.migrateLegacyOffsets(
            LOG_PREFIX, stateDirectory, taskId, Collections.singletonMap(partition, store));

        assertFalse(legacyFile.exists());
        assertEquals(100L, store.committedOffset(partition));
    }

    @Test
    public void shouldMigrateOffsetsFromLegacyGlobalFileAndDeleteIt() throws IOException {
        final File legacyGlobalFile = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, null, null);
        legacyGlobalFile.getParentFile().mkdirs();
        new OffsetCheckpoint(legacyGlobalFile).write(Collections.singletonMap(partition, 200L));
        assertTrue(legacyGlobalFile.exists());

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store =
            new LegacyCheckpointingStateStore<>(
                persistentStore, false, Set.of(partition), stateDirectory, null, LOG_PREFIX);
        LegacyCheckpointingStateStore.migrateLegacyOffsets(
            LOG_PREFIX, stateDirectory, null, Collections.singletonMap(partition, store));

        assertFalse(legacyGlobalFile.exists());
        assertEquals(200L, store.committedOffset(partition));
    }

    @Test
    public void shouldBeNoOpForMigrationWhenNoLegacyFileExists() {
        final File legacyFile = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, null);
        assertFalse(legacyFile.exists());

        final LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> store = createStore(false);
        // Should not throw
        LegacyCheckpointingStateStore.migrateLegacyOffsets(
            LOG_PREFIX, stateDirectory, taskId, Collections.singletonMap(partition, store));

        assertNull(store.committedOffset(partition));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldLogWarningWhenMigratingToStoreThatDoesNotManageOffsets() throws IOException {
        final File legacyFile = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, null);
        new OffsetCheckpoint(legacyFile).write(Collections.singletonMap(partition, 100L));

        // Use a raw MockKeyValueStore (managesOffsets() == false, not wrapped in LCSS)
        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(LegacyCheckpointingStateStore.class)) {
            LegacyCheckpointingStateStore.migrateLegacyOffsets(
                LOG_PREFIX, stateDirectory, taskId, Collections.singletonMap(partition, persistentStore));

            assertThat(appender.getMessages(),
                hasItem(containsString("does not manage its own offsets")));
        }
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenMigrationFails() throws IOException {
        final File legacyFile = LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, null);
        new OffsetCheckpoint(legacyFile).write(Collections.singletonMap(partition, 100L));

        final MockKeyValueStore throwingStore = new MockKeyValueStore(STORE_NAME, true) {
            @Override
            public void commit(final Map<TopicPartition, Long> changelogOffsets) {
                throw new RuntimeException("KABOOM!");
            }
        };

        assertThrows(ProcessorStateException.class, () ->
            LegacyCheckpointingStateStore.migrateLegacyOffsets(
                LOG_PREFIX, stateDirectory, taskId, Collections.singletonMap(partition, throwingStore)));
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private LegacyCheckpointingStateStore<MockKeyValueStore, Object, Object> createStore(final boolean eosEnabled) {
        return new LegacyCheckpointingStateStore<>(
            persistentStore, eosEnabled, Set.of(partition), stateDirectory, taskId, LOG_PREFIX);
    }

    private File storeCheckpointFile() {
        return LegacyCheckpointingStateStore.checkpointFileFor(stateDirectory, taskId, persistentStore);
    }

    private void writeCheckpointFile(final Map<TopicPartition, Long> offsets) throws IOException {
        new OffsetCheckpoint(storeCheckpointFile()).write(offsets);
    }

    private Map<TopicPartition, Long> readCheckpointFile() throws IOException {
        return new OffsetCheckpoint(storeCheckpointFile()).read();
    }
}
