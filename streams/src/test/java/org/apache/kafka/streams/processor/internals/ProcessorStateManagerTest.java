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
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockBatchingStateRestoreListener;
import org.apache.kafka.test.MockStateStore;
import org.apache.kafka.test.NoOpProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final MockStateStore persistentStore = new MockStateStore(persistentStoreName, true);
    private final MockStateStore nonPersistentStore = new MockStateStore(nonPersistentStoreName, false);
    private final TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
    private final String storeName = "mockStateStore";
    private final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);
    private final TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
    private final TaskId taskId = new TaskId(0, 1);
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final MockStateStore mockStateStore = new MockStateStore(storeName, true);
    private final byte[] key = new byte[]{0x0, 0x0, 0x0, 0x1};
    private final byte[] value = "the-value".getBytes(Charset.forName("UTF-8"));
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
        }), new MockTime());
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

        final MockStateStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.register(persistentStore, batchingRestoreCallback);
            stateMgr.updateStandbyStates(persistentStorePartition, Collections.singletonList(consumerRecord));
            assertThat(batchingRestoreCallback.getRestoredRecords().size(), is(1));
            assertTrue(batchingRestoreCallback.getRestoredRecords().contains(expectedKeyValue));
        } finally {
            stateMgr.close(Collections.emptyMap());
        }
    }

    @Test
    public void shouldRestoreStoreWithSinglePutRestoreSpecification() throws Exception {
        final TaskId taskId = new TaskId(0, 2);
        final Integer intKey = 1;

        final MockStateStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.updateStandbyStates(persistentStorePartition, Collections.singletonList(consumerRecord));
            assertThat(persistentStore.keys.size(), is(1));
            assertTrue(persistentStore.keys.contains(intKey));
        } finally {
            stateMgr.close(Collections.emptyMap());
        }
    }

    @Test
    public void testRegisterPersistentStore() throws IOException {
        final TaskId taskId = new TaskId(0, 2);

        final MockStateStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                    put(nonPersistentStoreName, nonPersistentStoreName);
                }
            },
            changelogReader,
            false,
            logContext);

        try {
            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(Collections.emptyMap());
        }
    }

    @Test
    public void testRegisterNonPersistentStore() throws IOException {
        final MockStateStore nonPersistentStore
            = new MockStateStore(nonPersistentStoreName, false); // non persistent store
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 2),
            noPartitions,
            false,
            stateDirectory,
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                    put(nonPersistentStoreName, nonPersistentStoreTopicName);
                }
            },
            changelogReader,
            false,
            logContext);

        try {
            stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(Collections.emptyMap());
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
        checkpoint.write(Collections.singletonMap(new TopicPartition(storeTopicName1, 0), lastCheckpointedOffset));

        final TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        final TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        final TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

        final MockStateStore store1 = new MockStateStore(storeName1, true);
        final MockStateStore store2 = new MockStateStore(storeName2, true);
        final MockStateStore store3 = new MockStateStore(storeName3, true);

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
            stateMgr.close(Collections.emptyMap());
        }
    }

    @Test
    public void testGetStore() throws IOException {
        final MockStateStore mockStateStore = new MockStateStore(nonPersistentStoreName, false);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            false,
            stateDirectory,
            Collections.emptyMap(),
            changelogReader,
            false,
            logContext);
        try {
            stateMgr.register(mockStateStore, mockStateStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(mockStateStore, stateMgr.getStore(nonPersistentStoreName));

        } finally {
            stateMgr.close(Collections.emptyMap());
        }
    }

    @Test
    public void testFlushAndClose() throws IOException {
        checkpoint.write(Collections.emptyMap());

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
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                    put(nonPersistentStoreName, nonPersistentStoreTopicName);
                }
            },
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
            stateMgr.close(ackedOffsets);
        }
        // make sure all stores are closed, and the checkpoint file is written.
        assertTrue(persistentStore.flushed);
        assertTrue(persistentStore.closed);
        assertTrue(nonPersistentStore.flushed);
        assertTrue(nonPersistentStore.closed);
        assertTrue(checkpointFile.exists());

        // the checkpoint file should contain an offset from the persistent store only.
        final Map<TopicPartition, Long> checkpointedOffsets = checkpoint.read();
        assertEquals(1, checkpointedOffsets.size());
        assertEquals(new Long(124), checkpointedOffsets.get(new TopicPartition(persistentStoreTopicName, 1)));
    }

    @Test
    public void shouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            false,
            stateDirectory,
            Collections.emptyMap(),
            changelogReader,
            false,
            logContext);
        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }

    @Test
    public void shouldNotChangeOffsetsIfAckedOffsetsIsNull() throws IOException {
        final Map<TopicPartition, Long> offsets = Collections.singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);

        final MockStateStore persistentStore = new MockStateStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            Collections.emptyMap(),
            changelogReader,
            false,
            logContext);
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.close(null);
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
            Collections.singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false,
            logContext);
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 10L));
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.singletonMap(persistentStorePartition, 11L)));
    }

    @Test
    public void shouldWriteCheckpointForStandbyReplica() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            Collections.singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false,
            logContext);

        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        final byte[] bytes = Serdes.Integer().serializer().serialize("", 10);
        stateMgr.updateStandbyStates(persistentStorePartition,
                                     Collections.singletonList(
                                             new ConsumerRecord<>(persistentStorePartition.topic(),
                                                                  persistentStorePartition.partition(),
                                                                  888L,
                                                                  bytes,
                                                                  bytes)));

        stateMgr.checkpoint(Collections.emptyMap());

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.singletonMap(persistentStorePartition, 889L)));

    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistent() throws IOException {
        final TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            Collections.singletonMap(nonPersistentStoreName, nonPersistentStoreTopicName),
            changelogReader,
            false,
            logContext);

        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        stateMgr.checkpoint(Collections.singletonMap(topicPartition, 876L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.emptyMap()));
    }

    @Test
    public void shouldNotWriteCheckpointForStoresWithoutChangelogTopic() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            Collections.emptyMap(),
            changelogReader,
            false,
            logContext);

        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 987L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.emptyMap()));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() throws IOException {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            Collections.emptyMap(),
            changelogReader,
            false,
            logContext);

        try {
            stateManager.register(new MockStateStore(ProcessorStateManager.CHECKPOINT_FILE_NAME, true), null);
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
            Collections.emptyMap(),
            changelogReader,
            false,
            logContext);

        stateManager.register(mockStateStore, null);

        try {
            stateManager.register(mockStateStore, null);
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
            Collections.singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        final MockStateStore stateStore = new MockStateStore(storeName, true) {
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
            Collections.singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        final MockStateStore stateStore = new MockStateStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.register(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.close(Collections.emptyMap());
            fail("Should throw ProcessorStateException if store close throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldFlushAllStoresEvenIfStoreThrowsExcepiton() throws IOException {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            Collections.singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        final AtomicBoolean flushedStore = new AtomicBoolean(false);

        final MockStateStore stateStore1 = new MockStateStore(storeName, true) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockStateStore stateStore2 = new MockStateStore(storeName + "2", true) {
            @Override
            public void flush() {
                flushedStore.set(true);
            }
        };
        stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.flush();
        } catch (final ProcessorStateException expected) { /* ignode */ }
        Assert.assertTrue(flushedStore.get());
    }

    @Test
    public void shouldCloseAllStoresEvenIfStoreThrowsExcepiton() throws IOException {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            Collections.singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

        final AtomicBoolean closedStore = new AtomicBoolean(false);

        final MockStateStore stateStore1 = new MockStateStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockStateStore stateStore2 = new MockStateStore(storeName + "2", true) {
            @Override
            public void close() {
                closedStore.set(true);
            }
        };
        stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.close(Collections.emptyMap());
        } catch (final ProcessorStateException expected) { /* ignode */ }
        Assert.assertTrue(closedStore.get());
    }

    @Test
    public void shouldDeleteCheckpointFileOnCreationIfEosEnabled() throws IOException {
        checkpoint.write(Collections.singletonMap(new TopicPartition(persistentStoreTopicName, 1), 123L));
        assertTrue(checkpointFile.exists());

        ProcessorStateManager stateManager = null;
        try {
            stateManager = new ProcessorStateManager(
                taskId,
                noPartitions,
                false,
                stateDirectory,
                Collections.emptyMap(),
                changelogReader,
                true,
                logContext);

            assertFalse(checkpointFile.exists());
        } finally {
            if (stateManager != null) {
                stateManager.close(null);
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
        final List<TopicPartition> changelogPartitions = Arrays.asList(changelogTopicPartition, store2Partition);
        final Map<String, String> storeToChangelog = new HashMap<String, String>() {
            {
                put(storeName, changelogTopic);
                put(store2Name, store2Changelog);
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            changelogPartitions,
            false,
            stateDirectory,
            storeToChangelog,
            changelogReader,
            eosEnabled,
            logContext);

        final MockStateStore stateStore = new MockStateStore(storeName, true);
        final MockStateStore stateStore2 = new MockStateStore(store2Name, true);

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
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                }
            },
            changelogReader,
            false,
            logContext);
    }

    private MockStateStore getPersistentStore() {
        return new MockStateStore("persistentStore", true);
    }

}
