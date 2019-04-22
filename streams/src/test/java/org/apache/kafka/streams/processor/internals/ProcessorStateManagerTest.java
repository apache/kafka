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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.internals.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockBatchingStateRestoreListener;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.NoOpProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProcessorStateManagerTest {

    private final Set<TopicPartition> noPartitions = Collections.emptySet();
    private final String applicationId = "test-application";
    private final String persistentStoreName = "persistentStore";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    private final MockKeyValueStore nonPersistentStore = new MockKeyValueStore(nonPersistentStoreName, false);
    private final TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
    private final String storeName = "mockKeyValueStore";
    private final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);
    private final TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
    private final TaskId taskId = new TaskId(0, 1);
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(storeName, true);
    private final byte[] key = new byte[] {0x0, 0x0, 0x0, 0x1};
    private final byte[] value = "the-value".getBytes(StandardCharsets.UTF_8);
    private final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(changelogTopic, 0, 0, key, value);
    private final LogContext logContext = new LogContext("process-state-manager-test ");

    private File baseDir;
    private File checkpointFile;
    private OffsetCheckpoint checkpoint;
    private StateDirectory stateDirectory;

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
        checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        checkpoint = new OffsetCheckpoint(checkpointFile);
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(baseDir);
    }

    @Test
    public void shouldRestoreStoreWithBatchingRestoreSpecification() throws Exception {
        final TaskId taskId = new TaskId(0, 2);
        final MockBatchingStateRestoreListener batchingRestoreCallback = new MockBatchingStateRestoreListener();

        final KeyValue<byte[], byte[]> expectedKeyValue = KeyValue.pair(key, value);

        final MockKeyValueStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.register(persistentStore, batchingRestoreCallback);
            stateMgr.updateStandbyStates(
                persistentStorePartition,
                singletonList(consumerRecord),
                consumerRecord.offset()
            );
            assertThat(batchingRestoreCallback.getRestoredRecords().size(), is(1));
            assertTrue(batchingRestoreCallback.getRestoredRecords().contains(expectedKeyValue));
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void shouldRestoreStoreWithSinglePutRestoreSpecification() throws Exception {
        final TaskId taskId = new TaskId(0, 2);
        final Integer intKey = 1;

        final MockKeyValueStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.updateStandbyStates(
                persistentStorePartition,
                singletonList(consumerRecord),
                consumerRecord.offset()
            );
            assertThat(persistentStore.keys.size(), is(1));
            assertTrue(persistentStore.keys.contains(intKey));
            assertEquals(9, persistentStore.values.get(0).length);
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void shouldConvertDataOnRestoreIfStoreImplementsTimestampedBytesStore() throws Exception {
        final TaskId taskId = new TaskId(0, 2);
        final Integer intKey = 1;

        final MockKeyValueStore persistentStore = getConverterStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.updateStandbyStates(
                persistentStorePartition,
                singletonList(consumerRecord),
                consumerRecord.offset()
            );
            assertThat(persistentStore.keys.size(), is(1));
            assertTrue(persistentStore.keys.contains(intKey));
            assertEquals(17, persistentStore.values.get(0).length);
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testRegisterPersistentStore() throws IOException {
        final TaskId taskId = new TaskId(0, 2);

        final MockKeyValueStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreName)
            ),
            changelogReader,
            false,
            logContext);

        try {
            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testRegisterNonPersistentStore() throws IOException {
        final MockKeyValueStore nonPersistentStore =
            new MockKeyValueStore(nonPersistentStoreName, false); // non persistent store
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 2),
            noPartitions,
            false,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
            ),
            changelogReader,
            false,
            logContext);

        try {
            stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testChangeLogOffsets() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final long lastCheckpointedOffset = 10L;
        final String storeName1 = "store1";
        final String storeName2 = "store2";
        final String storeName3 = "store3";

        final String storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
        final String storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
        final String storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

        final Map<String, String> storeToChangelogTopic = new HashMap<>();
        storeToChangelogTopic.put(storeName1, storeTopicName1);
        storeToChangelogTopic.put(storeName2, storeTopicName2);
        storeToChangelogTopic.put(storeName3, storeTopicName3);

        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(singletonMap(new TopicPartition(storeTopicName1, 0), lastCheckpointedOffset));

        final TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        final TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        final TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

        final MockKeyValueStore store1 = new MockKeyValueStore(storeName1, true);
        final MockKeyValueStore store2 = new MockKeyValueStore(storeName2, true);
        final MockKeyValueStore store3 = new MockKeyValueStore(storeName3, true);

        // if there is a source partition, inherit the partition id
        final Set<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            sourcePartitions,
            true, // standby
            stateDirectory,
            storeToChangelogTopic,
            changelogReader,
            false,
            logContext);

        try {
            stateMgr.register(store1, store1.stateRestoreCallback);
            stateMgr.register(store2, store2.stateRestoreCallback);
            stateMgr.register(store3, store3.stateRestoreCallback);

            final Map<TopicPartition, Long> changeLogOffsets = stateMgr.checkpointed();

            assertEquals(3, changeLogOffsets.size());
            assertTrue(changeLogOffsets.containsKey(partition1));
            assertTrue(changeLogOffsets.containsKey(partition2));
            assertTrue(changeLogOffsets.containsKey(partition3));
            assertEquals(lastCheckpointedOffset, (long) changeLogOffsets.get(partition1));
            assertEquals(-1L, (long) changeLogOffsets.get(partition2));
            assertEquals(-1L, (long) changeLogOffsets.get(partition3));

        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testGetStore() throws IOException {
        final MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(nonPersistentStoreName, false);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            false,
            stateDirectory,
            emptyMap(),
            changelogReader,
            false,
            logContext);
        try {
            stateMgr.register(mockKeyValueStore, mockKeyValueStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(mockKeyValueStore, stateMgr.getStore(nonPersistentStoreName));

        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testFlushAndClose() throws IOException {
        checkpoint.write(emptyMap());

        // set up ack'ed offsets
        final HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
        ackedOffsets.put(new TopicPartition(persistentStoreTopicName, 1), 123L);
        ackedOffsets.put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
        ackedOffsets.put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
                  mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
            changelogReader,
            false,
            logContext);
        try {
            // make sure the checkpoint file is not written yet
            assertFalse(checkpointFile.exists());

            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        } finally {
            // close the state manager with the ack'ed offsets
            stateMgr.flush();
            stateMgr.checkpoint(ackedOffsets);
            stateMgr.close(true);
        }
        // make sure all stores are closed, and the checkpoint file is written.
        assertTrue(persistentStore.flushed);
        assertTrue(persistentStore.closed);
        assertTrue(nonPersistentStore.flushed);
        assertTrue(nonPersistentStore.closed);
        assertTrue(checkpointFile.exists());

        // make sure that flush is called in the proper order
        assertThat(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));

        // the checkpoint file should contain an offset from the persistent store only.
        final Map<TopicPartition, Long> checkpointedOffsets = checkpoint.read();
        assertEquals(1, checkpointedOffsets.size());
        assertEquals(new Long(124), checkpointedOffsets.get(new TopicPartition(persistentStoreTopicName, 1)));
    }

    @Test
    public void shouldMaintainRegistrationOrderWhenReregistered() throws IOException {
        checkpoint.write(emptyMap());

        // set up ack'ed offsets
        final TopicPartition persistentTopicPartition = new TopicPartition(persistentStoreTopicName, 1);
        final TopicPartition nonPersistentTopicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
                  mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
            changelogReader,
            false,
            logContext);
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        // de-registers the stores, but doesn't re-register them because
        // the context isn't connected to our state manager
        stateMgr.reinitializeStateStoresForPartitions(asList(nonPersistentTopicPartition, persistentTopicPartition),
                                                      new MockInternalProcessorContext());
        // register them in backward order
        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.flush();

        // make sure that flush is called in the proper order
        assertTrue(persistentStore.flushed);
        assertTrue(nonPersistentStore.flushed);
        assertThat(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));
    }

    @Test
    public void shouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            false,
            stateDirectory,
            emptyMap(),
            changelogReader,
            false,
            logContext);
        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }

    @Test
    public void shouldNotChangeOffsetsIfAckedOffsetsIsNull() throws IOException {
        final Map<TopicPartition, Long> offsets = singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);

        final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            emptyMap(),
            changelogReader,
            false,
            logContext);
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.close(true);
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(offsets));
    }

    @Test
    public void shouldWriteCheckpointForPersistentLogEnabledStore() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false,
            logContext);
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 11L)));
    }

    @Test
    public void shouldWriteCheckpointForStandbyReplica() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false,
            logContext);

        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        final byte[] bytes = Serdes.Integer().serializer().serialize("", 10);
        stateMgr.updateStandbyStates(
            persistentStorePartition,
            singletonList(new ConsumerRecord<>("", 0, 0L, bytes, bytes)),
            888L
        );

        stateMgr.checkpoint(emptyMap());

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 889L)));

    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistent() throws IOException {
        final TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            singletonMap(nonPersistentStoreName, nonPersistentStoreTopicName),
            changelogReader,
            false,
            logContext);

        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        stateMgr.checkpoint(singletonMap(topicPartition, 876L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(emptyMap()));
    }

    @Test
    public void shouldNotWriteCheckpointForStoresWithoutChangelogTopic() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            emptyMap(),
            changelogReader,
            false,
            logContext);

        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(singletonMap(persistentStorePartition, 987L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(emptyMap()));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() throws IOException {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            emptyMap(),
            changelogReader,
            false,
            logContext);

        try {
            stateManager.register(new MockKeyValueStore(ProcessorStateManager.CHECKPOINT_FILE_NAME, true), null);
            fail("should have thrown illegal argument exception when store name same as checkpoint file");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered() throws IOException {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            emptyMap(),
            changelogReader,
            false,
            logContext);

        stateManager.register(mockKeyValueStore, null);

        try {
            stateManager.register(mockKeyValueStore, null);
            fail("should have thrown illegal argument exception when store with same name already registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }

    }

    @Test
    public void shouldThrowProcessorStateExceptionOnFlushIfStoreThrowsAnException() throws IOException {

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.register(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.flush();
            fail("Should throw ProcessorStateException if store flush throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException() throws IOException {

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.register(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.close(true);
            fail("Should throw ProcessorStateException if store close throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    // if the optional is absent, it'll throw an exception and fail the test.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void shouldLogAWarningIfCheckpointThrowsAnIOException() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final ProcessorStateManager stateMgr;
        try {
            stateMgr = new ProcessorStateManager(
                taskId,
                noPartitions,
                false,
                stateDirectory,
                singletonMap(persistentStore.name(), persistentStoreTopicName),
                changelogReader,
                false,
                logContext);
        } catch (final IOException e) {
            e.printStackTrace();
            throw new AssertionError(e);
        }
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

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
    public void shouldFlushAllStoresEvenIfStoreThrowsException() throws IOException {
        final AtomicBoolean flushedStore = new AtomicBoolean(false);

        final MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true) {
            @Override
            public void flush() {
                flushedStore.set(true);
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.flush();
        } catch (final ProcessorStateException expected) { /* ignode */ }
        Assert.assertTrue(flushedStore.get());
    }

    @Test
    public void shouldCloseAllStoresEvenIfStoreThrowsExcepiton() throws IOException {

        final AtomicBoolean closedStore = new AtomicBoolean(false);

        final MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true) {
            @Override
            public void close() {
                closedStore.set(true);
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.close(true);
        } catch (final ProcessorStateException expected) { /* ignode */ }
        Assert.assertTrue(closedStore.get());
    }

    @Test
    public void shouldDeleteCheckpointFileOnCreationIfEosEnabled() throws IOException {
        checkpoint.write(singletonMap(new TopicPartition(persistentStoreTopicName, 1), 123L));
        assertTrue(checkpointFile.exists());

        ProcessorStateManager stateManager = null;
        try {
            stateManager = new ProcessorStateManager(
                taskId,
                noPartitions,
                false,
                stateDirectory,
                emptyMap(),
                changelogReader,
                true,
                logContext);

            assertFalse(checkpointFile.exists());
        } finally {
            if (stateManager != null) {
                stateManager.close(true);
            }
        }
    }

    @Test
    public void shouldSuccessfullyReInitializeStateStoresWithEosDisable() throws Exception {
        shouldSuccessfullyReInitializeStateStores(false);
    }

    @Test
    public void shouldSuccessfullyReInitializeStateStoresWithEosEnable() throws Exception {
        shouldSuccessfullyReInitializeStateStores(true);
    }

    private void shouldSuccessfullyReInitializeStateStores(final boolean eosEnabled) throws Exception {
        final String store2Name = "store2";
        final String store2Changelog = "store2-changelog";
        final TopicPartition store2Partition = new TopicPartition(store2Changelog, 0);
        final List<TopicPartition> changelogPartitions = asList(changelogTopicPartition, store2Partition);
        final Map<String, String> storeToChangelog = mkMap(
                mkEntry(storeName, changelogTopic),
                mkEntry(store2Name, store2Changelog)
        );

        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true);
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(store2Name, true);

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            changelogPartitions,
            false,
            stateDirectory,
            storeToChangelog,
            changelogReader,
            eosEnabled,
            logContext);

        stateManager.register(stateStore, stateStore.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        stateStore.initialized = false;
        stateStore2.initialized = false;

        stateManager.reinitializeStateStoresForPartitions(changelogPartitions, new NoOpProcessorContext() {
            @Override
            public void register(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
                stateManager.register(store, stateRestoreCallback);
            }
        });

        assertTrue(stateStore.initialized);
        assertTrue(stateStore2.initialized);
    }

    private ProcessorStateManager getStandByStateManager(final TaskId taskId) throws IOException {
        return new ProcessorStateManager(
            taskId,
            noPartitions,
            true,
            stateDirectory,
            singletonMap(persistentStoreName, persistentStoreTopicName),
            changelogReader,
            false,
            logContext);
    }

    private MockKeyValueStore getPersistentStore() {
        return new MockKeyValueStore("persistentStore", true);
    }

    private MockKeyValueStore getConverterStore() {
        return new ConverterStore("persistentStore", true);
    }

    private class ConverterStore extends MockKeyValueStore implements TimestampedBytesStore {
        ConverterStore(final String name,
                       final boolean persistent) {
            super(name, persistent);
        }
    }
}
