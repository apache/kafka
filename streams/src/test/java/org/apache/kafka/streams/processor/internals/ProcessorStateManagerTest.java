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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockBatchingStateRestoreListener;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class ProcessorStateManagerTest {

    private final String applicationId = "test-application";
    private final String persistentStoreName = "persistentStore";
    private final String persistentStoreTwoName = "persistentStore2";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String persistentStoreTwoTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreTwoName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    private final MockKeyValueStore persistentStoreTwo = new MockKeyValueStore(persistentStoreTwoName, true);
    private final MockKeyValueStore nonPersistentStore = new MockKeyValueStore(nonPersistentStoreName, false);
    private final TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
    private final TopicPartition persistentStoreTwoPartition = new TopicPartition(persistentStoreTwoTopicName, 1);
    private final TopicPartition nonPersistentStorePartition = new TopicPartition(nonPersistentStoreTopicName, 1);
    private final TopicPartition irrelevantPartition = new TopicPartition("other-topic", 1);
    private final TaskId taskId = new TaskId(0, 1);
    private final Integer key = 1;
    private final String value = "the-value";
    private final byte[] keyBytes = new byte[] {0x0, 0x0, 0x0, 0x1};
    private final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    private final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(persistentStoreTopicName, 1, 100L, keyBytes, valueBytes);
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final LogContext logContext = new LogContext("process-state-manager-test ");

    private File baseDir;
    private File checkpointFile;
    private OffsetCheckpoint checkpoint;
    private StateDirectory stateDirectory;

    @Mock(type = MockType.NICE)
    private StateStore store;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadata;

    @Before
    public void setup() {
        baseDir = TestUtils.tempDirectory();

        stateDirectory = new StateDirectory(new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, baseDir.getPath());
            }
        }), new MockTime(), true);
        checkpointFile = new File(stateDirectory.directoryForTask(taskId), CHECKPOINT_FILE_NAME);
        checkpoint = new OffsetCheckpoint(checkpointFile);

        EasyMock.expect(storeMetadata.changelogPartition()).andReturn(persistentStorePartition).anyTimes();
        EasyMock.expect(storeMetadata.store()).andReturn(store).anyTimes();
        EasyMock.expect(store.name()).andReturn(persistentStoreName).anyTimes();
        EasyMock.replay(storeMetadata, store);
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(baseDir);
    }

    @Test
    public void shouldReturnBaseDir() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        assertEquals(stateDirectory.directoryForTask(taskId), stateMgr.baseDir());
    }

    // except this test for all other tests active / standby state managers acts the same, so
    // for all others we always use ACTIVE unless explained specifically.
    @Test
    public void shouldReportTaskType() {
        ProcessorStateManager stateMgr = getStateManager(Task.TaskType.STANDBY);
        assertEquals(Task.TaskType.STANDBY, stateMgr.taskType());

        stateMgr = getStateManager(Task.TaskType.ACTIVE);
        assertEquals(Task.TaskType.ACTIVE, stateMgr.taskType());
    }

    @Test
    public void shouldReportChangelogAsSource() {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            mkSet(persistentStorePartition, nonPersistentStorePartition),
            Task.TaskType.STANDBY,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(persistentStoreTwoName, persistentStoreTwoTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
            ),
            changelogReader,
            logContext);

        assertTrue(stateMgr.changelogAsSource(persistentStorePartition));
        assertTrue(stateMgr.changelogAsSource(nonPersistentStorePartition));
        assertFalse(stateMgr.changelogAsSource(persistentStoreTwoPartition));
    }

    @Test
    public void shouldFindSingleStoreForChangelog() {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            Collections.emptySet(),
            Task.TaskType.STANDBY,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(persistentStoreTwoName, persistentStoreTopicName)
            ),
            changelogReader,
            logContext);

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.registerStore(persistentStoreTwo, persistentStore.stateRestoreCallback);

        assertThrows(IllegalStateException.class, () -> stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 0L)));
    }

    @Test
    public void shouldRestoreStoreWithRestoreCallback() {
        final MockBatchingStateRestoreListener batchingRestoreCallback = new MockBatchingStateRestoreListener();

        final KeyValue<byte[], byte[]> expectedKeyValue = KeyValue.pair(keyBytes, valueBytes);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(persistentStore, batchingRestoreCallback);
            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            assertThat(batchingRestoreCallback.getRestoredRecords().size(), is(1));
            assertTrue(batchingRestoreCallback.getRestoredRecords().contains(expectedKeyValue));

            assertEquals(Collections.singletonMap(persistentStorePartition, 101L), stateMgr.changelogOffsets());
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldRestoreNonTimestampedStoreWithNoConverter() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            assertThat(persistentStore.keys.size(), is(1));
            assertTrue(persistentStore.keys.contains(key));
            // we just check non timestamped value length
            assertEquals(9, persistentStore.values.get(0).length);
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldRestoreTimestampedStoreWithConverter() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        final MockKeyValueStore store = getConverterStore();

        try {
            stateMgr.registerStore(store, store.stateRestoreCallback);
            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            assertThat(store.keys.size(), is(1));
            assertTrue(store.keys.contains(key));
            // we just check timestamped value length
            assertEquals(17, store.values.get(0).length);
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldRegisterPersistentStores() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            assertTrue(changelogReader.isPartitionRegistered(persistentStorePartition));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldRegisterNonPersistentStore() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            assertTrue(changelogReader.isPartitionRegistered(nonPersistentStorePartition));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldNotRegisterNonLoggedStore() {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            emptySet(),
            Task.TaskType.STANDBY,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            assertFalse(changelogReader.isPartitionRegistered(persistentStorePartition));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldInitializeOffsetsFromCheckpointFile() throws IOException {
        final long checkpointOffset = 10L;

        final Map<TopicPartition, Long> offsets = mkMap(
            mkEntry(persistentStorePartition, checkpointOffset),
            mkEntry(nonPersistentStorePartition, checkpointOffset),
            mkEntry(irrelevantPartition, 999L)
        );
        checkpoint.write(offsets);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(persistentStoreTwo, persistentStoreTwo.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint();

            assertFalse(checkpointFile.exists());
            assertEquals(mkSet(
                persistentStorePartition,
                persistentStoreTwoPartition,
                nonPersistentStorePartition),
                stateMgr.changelogPartitions());
            assertEquals(mkMap(
                mkEntry(persistentStorePartition, checkpointOffset + 1L),
                mkEntry(persistentStoreTwoPartition, 0L),
                mkEntry(nonPersistentStorePartition, checkpointOffset + 1L)),
                stateMgr.changelogOffsets()
            );

            assertNull(stateMgr.storeMetadata(irrelevantPartition));
            assertNull(stateMgr.storeMetadata(persistentStoreTwoPartition).offset());
            assertThat(stateMgr.storeMetadata(persistentStorePartition).offset(), equalTo(checkpointOffset));
            assertThat(stateMgr.storeMetadata(nonPersistentStorePartition).offset(), equalTo(checkpointOffset));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldGetRegisteredStore() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(persistentStore, stateMgr.getStore(persistentStoreName));
            assertEquals(nonPersistentStore, stateMgr.getStore(nonPersistentStoreName));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldFlushCheckpointAndClose() throws IOException {
        checkpoint.write(emptyMap());

        // set up ack'ed offsets
        final HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
        ackedOffsets.put(persistentStorePartition, 123L);
        ackedOffsets.put(nonPersistentStorePartition, 456L);
        ackedOffsets.put(new TopicPartition("nonRegisteredTopic", 1), 789L);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        try {
            // make sure the checkpoint file is not written yet
            assertFalse(checkpointFile.exists());

            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        } finally {
            stateMgr.flush();

            assertTrue(persistentStore.flushed);
            assertTrue(nonPersistentStore.flushed);

            // make sure that flush is called in the proper order
            assertThat(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));

            stateMgr.checkpoint(ackedOffsets);

            assertTrue(checkpointFile.exists());

            // the checkpoint file should contain an offset from the persistent store only.
            final Map<TopicPartition, Long> checkpointedOffsets = checkpoint.read();
            assertThat(checkpointedOffsets, is(singletonMap(new TopicPartition(persistentStoreTopicName, 1), 123L)));

            stateMgr.close();

            assertTrue(persistentStore.closed);
            assertTrue(nonPersistentStore.closed);
        }
    }

    @Test
    public void shouldOverrideOffsetsWhenRestoreAndProcess() throws IOException {
        final Map<TopicPartition, Long> offsets = singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint();

            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());
            assertThat(storeMetadata.offset(), equalTo(99L));

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            assertThat(storeMetadata.offset(), equalTo(100L));

            // should ignore irrelevant topic partitions
            stateMgr.checkpoint(mkMap(
                mkEntry(persistentStorePartition, 220L),
                mkEntry(irrelevantPartition, 9000L)
            ));

            assertThat(stateMgr.storeMetadata(irrelevantPartition), equalTo(null));
            assertThat(storeMetadata.offset(), equalTo(220L));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldWriteCheckpointForPersistentStore() throws IOException {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint();

            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            stateMgr.checkpoint(emptyMap());

            final Map<TopicPartition, Long> read = checkpoint.read();
            assertThat(read, equalTo(singletonMap(persistentStorePartition, 100L)));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistent() throws IOException {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint();

            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(nonPersistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.checkpoint(singletonMap(nonPersistentStorePartition, 876L));

            final Map<TopicPartition, Long> read = checkpoint.read();
            assertThat(read, equalTo(emptyMap()));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldNotWriteCheckpointForStoresWithoutChangelogTopic() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            emptySet(),
            Task.TaskType.STANDBY,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

            stateMgr.checkpoint(singletonMap(persistentStorePartition, 987L));

            final Map<TopicPartition, Long> read = checkpoint.read();
            assertThat(read, equalTo(emptyMap()));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() {
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);

        assertThrows(IllegalArgumentException.class, () ->
            stateManager.registerStore(new MockKeyValueStore(CHECKPOINT_FILE_NAME, true), null));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered() {
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);

        stateManager.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        assertThrows(IllegalArgumentException.class, () ->
            stateManager.registerStore(persistentStore, persistentStore.stateRestoreCallback));
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnFlushIfStoreThrowsAnException() {
        final RuntimeException exception = new RuntimeException("KABOOM!");
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);
        final MockKeyValueStore stateStore = new MockKeyValueStore(persistentStoreName, true) {
            @Override
            public void flush() {
                throw exception;
            }
        };
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);

        final ProcessorStateException thrown = assertThrows(ProcessorStateException.class, stateManager::flush);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldPreserveStreamsExceptionOnFlushIfStoreThrows() {
        final StreamsException exception = new StreamsException("KABOOM!");
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);
        final MockKeyValueStore stateStore = new MockKeyValueStore(persistentStoreName, true) {
            @Override
            public void flush() {
                throw exception;
            }
        };
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);

        final StreamsException thrown = assertThrows(StreamsException.class, stateManager::flush);
        assertEquals(exception, thrown);
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException() {
        final RuntimeException exception = new RuntimeException("KABOOM!");
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);
        final MockKeyValueStore stateStore = new MockKeyValueStore(persistentStoreName, true) {
            @Override
            public void close() {
                throw exception;
            }
        };
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);

        final ProcessorStateException thrown = assertThrows(ProcessorStateException.class, stateManager::close);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldPreserveStreamsExceptionOnCloseIfStoreThrows() {
        final StreamsException exception = new StreamsException("KABOOM!");
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);
        final MockKeyValueStore stateStore = new MockKeyValueStore(persistentStoreName, true) {
            @Override
            public void close() {
                throw exception;
            }
        };
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);

        final StreamsException thrown = assertThrows(StreamsException.class, stateManager::close);
        assertEquals(exception, thrown);
    }

    @Test
    public void shouldThrowIfRestoringUnregisteredStore() {
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);

        assertThrows(IllegalStateException.class, () -> stateManager.restore(storeMetadata, Collections.emptyList()));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void shouldLogAWarningIfCheckpointThrowsAnIOException() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        stateDirectory.clean();
        stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
        LogCaptureAppender.unregister(appender);

        boolean foundExpectedLogMessage = false;
        for (final LogCaptureAppender.Event event : appender.getEvents()) {
            if ("WARN".equals(event.getLevel())
                && event.getMessage().startsWith("process-state-manager-test Failed to write offset checkpoint file to [")
                && event.getMessage().endsWith(".checkpoint]")
                && event.getThrowableInfo().get().startsWith("java.io.FileNotFoundException: ")) {

                foundExpectedLogMessage = true;
                break;
            }
        }
        assertTrue(foundExpectedLogMessage);
    }

    @Test
    public void shouldThrowIfLoadCheckpointThrows() throws IOException {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
        final File file = new File(stateMgr.baseDir(), CHECKPOINT_FILE_NAME);
        file.createNewFile();
        final FileWriter writer = new FileWriter(file);
        writer.write("abcdefg");
        writer.close();

        try {
            stateMgr.initializeStoreOffsetsFromCheckpoint();
            fail("should have thrown processor state exception when IO exception happens");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIfRestoreCallbackThrows() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        stateMgr.registerStore(persistentStore, new MockBatchingStateRestoreListener() {
            @Override
            public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
                throw new RuntimeException("KABOOM!");
            }
        });

        final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);

        try {
            stateMgr.restore(storeMetadata, singletonList(consumerRecord));
            fail("should have thrown processor state exception when IO exception happens");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldFlushGoodStoresEvenSomeThrowsException() {
        final AtomicBoolean flushedStore = new AtomicBoolean(false);

        final MockKeyValueStore stateStore1 = new MockKeyValueStore(persistentStoreName, true) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(persistentStoreTwoName, true) {
            @Override
            public void flush() {
                flushedStore.set(true);
            }
        };
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);

        stateManager.registerStore(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.registerStore(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.flush();
        } catch (final ProcessorStateException expected) { /* ignode */ }

        Assert.assertTrue(flushedStore.get());
    }

    @Test
    public void shouldCloseAllStoresEvenIfStoreThrowsExcepiton() {
        final AtomicBoolean closedStore = new AtomicBoolean(false);

        final MockKeyValueStore stateStore1 = new MockKeyValueStore(persistentStoreName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(persistentStoreTwoName, true) {
            @Override
            public void close() {
                closedStore.set(true);
            }
        };
        final ProcessorStateManager stateManager = getStateManager(Task.TaskType.ACTIVE);

        stateManager.registerStore(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.registerStore(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.close();
        } catch (final ProcessorStateException expected) { /* ignode */ }

        Assert.assertTrue(closedStore.get());
    }

    private ProcessorStateManager getStateManager(final Task.TaskType taskType) {
        return new ProcessorStateManager(
            taskId,
            emptySet(),
            taskType,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(persistentStoreTwoName, persistentStoreTwoTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
            ),
            changelogReader,
            logContext);
    }

    private MockKeyValueStore getConverterStore() {
        return new ConverterStore(persistentStoreName, true);
    }

    private static class ConverterStore extends MockKeyValueStore implements TimestampedBytesStore {
        ConverterStore(final String name, final boolean persistent) {
            super(name, persistent);
        }
    }
}
