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
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockRestoreCallback;
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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
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
    private final String persistentStoreTopicName =
        ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String persistentStoreTwoTopicName =
        ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreTwoName);
    private final String nonPersistentStoreTopicName =
        ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
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
    private final ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>(persistentStoreTopicName, 1, 100L, keyBytes, valueBytes);
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final LogContext logContext = new LogContext("process-state-manager-test ");
    private final StateRestoreCallback noopStateRestoreCallback = (k, v) -> { };

    private File baseDir;
    private File checkpointFile;
    private OffsetCheckpoint checkpoint;
    private StateDirectory stateDirectory;

    @Mock(type = MockType.NICE)
    private StateStore store;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadata;
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

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

        expect(storeMetadata.changelogPartition()).andReturn(persistentStorePartition).anyTimes();
        expect(storeMetadata.store()).andReturn(store).anyTimes();
        expect(store.name()).andReturn(persistentStoreName).anyTimes();
        replay(storeMetadata, store);
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(baseDir);
    }

    @Test
    public void shouldReturnDefaultChangelogTopicName() {
        final String applicationId = "appId";
        final String storeName = "store";

        assertThat(
            ProcessorStateManager.storeChangelogTopic(applicationId, storeName),
            is(applicationId + "-" + storeName + "-changelog")
        );
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
            Task.TaskType.STANDBY,
            false,
            logContext,
            stateDirectory,
            changelogReader,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(persistentStoreTwoName, persistentStoreTwoTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
            ),
            mkSet(persistentStorePartition, nonPersistentStorePartition));

        assertTrue(stateMgr.changelogAsSource(persistentStorePartition));
        assertTrue(stateMgr.changelogAsSource(nonPersistentStorePartition));
        assertFalse(stateMgr.changelogAsSource(persistentStoreTwoPartition));
    }

    @Test
    public void shouldFindSingleStoreForChangelog() {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            Task.TaskType.STANDBY,
            false,
            logContext,
            stateDirectory,
            changelogReader, mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(persistentStoreTwoName, persistentStoreTopicName)
            ),
            Collections.emptySet());

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.registerStore(persistentStoreTwo, persistentStore.stateRestoreCallback);

        assertThrows(
            IllegalStateException.class,
            () -> stateMgr.updateChangelogOffsets(Collections.singletonMap(persistentStorePartition, 0L))
        );
    }

    @Test
    public void shouldRestoreStoreWithRestoreCallback() {
        final MockRestoreCallback restoreCallback = new MockRestoreCallback();

        final KeyValue<byte[], byte[]> expectedKeyValue = KeyValue.pair(keyBytes, valueBytes);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(persistentStore, restoreCallback);
            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            assertThat(restoreCallback.restored.size(), is(1));
            assertTrue(restoreCallback.restored.contains(expectedKeyValue));

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
    public void shouldUnregisterChangelogsDuringClose() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        reset(storeMetadata);
        final StateStore store = EasyMock.createMock(StateStore.class);
        expect(storeMetadata.changelogPartition()).andStubReturn(persistentStorePartition);
        expect(storeMetadata.store()).andStubReturn(store);
        expect(store.name()).andStubReturn(persistentStoreName);

        context.uninitialize();
        store.init((StateStoreContext) context, store);
        replay(storeMetadata, context, store);

        stateMgr.registerStateStores(singletonList(store), context);
        verify(context, store);

        stateMgr.registerStore(store, noopStateRestoreCallback);
        assertTrue(changelogReader.isPartitionRegistered(persistentStorePartition));

        reset(store);
        expect(store.name()).andStubReturn(persistentStoreName);
        store.close();
        replay(store);

        stateMgr.close();
        verify(store);

        assertFalse(changelogReader.isPartitionRegistered(persistentStorePartition));
    }

    @Test
    public void shouldRecycleStoreAndReregisterChangelog() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        reset(storeMetadata);
        final StateStore store = EasyMock.createMock(StateStore.class);
        expect(storeMetadata.changelogPartition()).andStubReturn(persistentStorePartition);
        expect(storeMetadata.store()).andStubReturn(store);
        expect(store.name()).andStubReturn(persistentStoreName);

        context.uninitialize();
        store.init((StateStoreContext) context, store);
        replay(storeMetadata, context, store);

        stateMgr.registerStateStores(singletonList(store), context);
        verify(context, store);

        stateMgr.registerStore(store, noopStateRestoreCallback);
        assertTrue(changelogReader.isPartitionRegistered(persistentStorePartition));

        stateMgr.recycle();
        assertFalse(changelogReader.isPartitionRegistered(persistentStorePartition));
        assertThat(stateMgr.getStore(persistentStoreName), equalTo(store));

        reset(context, store);
        context.uninitialize();
        expect(store.name()).andStubReturn(persistentStoreName);
        replay(context, store);

        stateMgr.registerStateStores(singletonList(store), context);

        verify(context, store);
        assertTrue(changelogReader.isPartitionRegistered(persistentStorePartition));
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
            Task.TaskType.STANDBY,
            false,
            logContext,
            stateDirectory,
            changelogReader,
            emptyMap(),
            emptySet());

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
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);

            assertTrue(checkpointFile.exists());
            assertEquals(mkSet(
                persistentStorePartition,
                persistentStoreTwoPartition,
                nonPersistentStorePartition),
                stateMgr.changelogPartitions());
            assertEquals(mkMap(
                mkEntry(persistentStorePartition, checkpointOffset + 1L),
                mkEntry(persistentStoreTwoPartition, 0L),
                mkEntry(nonPersistentStorePartition, 0L)),
                stateMgr.changelogOffsets()
            );

            assertNull(stateMgr.storeMetadata(irrelevantPartition));
            assertNull(stateMgr.storeMetadata(persistentStoreTwoPartition).offset());
            assertThat(stateMgr.storeMetadata(persistentStorePartition).offset(), equalTo(checkpointOffset));
            assertNull(stateMgr.storeMetadata(nonPersistentStorePartition).offset());
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldInitializeOffsetsFromCheckpointFileAndDeleteIfEOSEnabled() throws IOException {
        final long checkpointOffset = 10L;

        final Map<TopicPartition, Long> offsets = mkMap(
                mkEntry(persistentStorePartition, checkpointOffset),
                mkEntry(nonPersistentStorePartition, checkpointOffset),
                mkEntry(irrelevantPartition, 999L)
        );
        checkpoint.write(offsets);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(persistentStoreTwo, persistentStoreTwo.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);

            assertFalse(checkpointFile.exists());
            assertEquals(mkSet(
                    persistentStorePartition,
                    persistentStoreTwoPartition,
                    nonPersistentStorePartition),
                    stateMgr.changelogPartitions());
            assertEquals(mkMap(
                    mkEntry(persistentStorePartition, checkpointOffset + 1L),
                    mkEntry(persistentStoreTwoPartition, 0L),
                    mkEntry(nonPersistentStorePartition, 0L)),
                    stateMgr.changelogOffsets()
            );

            assertNull(stateMgr.storeMetadata(irrelevantPartition));
            assertNull(stateMgr.storeMetadata(persistentStoreTwoPartition).offset());
            assertThat(stateMgr.storeMetadata(persistentStorePartition).offset(), equalTo(checkpointOffset));
            assertNull(stateMgr.storeMetadata(nonPersistentStorePartition).offset());
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
    public void shouldGetChangelogPartitionForRegisteredStore() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        final TopicPartition changelogPartition = stateMgr.registeredChangelogPartitionFor(persistentStoreName);

        assertThat(changelogPartition.topic(), is(persistentStoreTopicName));
        assertThat(changelogPartition.partition(), is(taskId.partition));
    }

    @Test
    public void shouldThrowIfStateStoreIsNotRegistered() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        assertThrows("State store " + persistentStoreName
            + " for which the registered changelog partition should be"
            + " retrieved has not been registered",
            IllegalStateException.class,
            () -> stateMgr.registeredChangelogPartitionFor(persistentStoreName)
        );
    }

    @Test
    public void shouldThrowIfStateStoreHasLoggingDisabled() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        final String storeName = "store-with-logging-disabled";
        final MockKeyValueStore storeWithLoggingDisabled = new MockKeyValueStore(storeName, true);
        stateMgr.registerStore(storeWithLoggingDisabled, null);

        assertThrows("Registered state store " + storeName
                + " does not have a registered changelog partition."
                + " This may happen if logging is disabled for the state store.",
            IllegalStateException.class,
            () -> stateMgr.registeredChangelogPartitionFor(storeName)
        );
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

            stateMgr.updateChangelogOffsets(ackedOffsets);
            stateMgr.checkpoint();

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
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);

            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());
            assertThat(storeMetadata.offset(), equalTo(99L));

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            assertThat(storeMetadata.offset(), equalTo(100L));

            // should ignore irrelevant topic partitions
            stateMgr.updateChangelogOffsets(mkMap(
                mkEntry(persistentStorePartition, 220L),
                mkEntry(irrelevantPartition, 9000L)
            ));
            stateMgr.checkpoint();

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
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);

            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(persistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.restore(storeMetadata, singletonList(consumerRecord));

            stateMgr.checkpoint();

            final Map<TopicPartition, Long> read = checkpoint.read();
            assertThat(read, equalTo(singletonMap(persistentStorePartition, 100L)));
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistentStore() throws IOException {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        try {
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);

            final StateStoreMetadata storeMetadata = stateMgr.storeMetadata(nonPersistentStorePartition);
            assertThat(storeMetadata, notNullValue());

            stateMgr.updateChangelogOffsets(singletonMap(nonPersistentStorePartition, 876L));
            stateMgr.checkpoint();

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
            Task.TaskType.STANDBY,
            false,
            logContext,
            stateDirectory,
            changelogReader,
            emptyMap(),
            emptySet());

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

            stateMgr.updateChangelogOffsets(singletonMap(persistentStorePartition, 987L));
            stateMgr.checkpoint();

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
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
        stateDirectory.clean();

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(ProcessorStateManager.class)) {
            stateMgr.updateChangelogOffsets(singletonMap(persistentStorePartition, 10L));
            stateMgr.checkpoint();


            boolean foundExpectedLogMessage = false;
            for (final LogCaptureAppender.Event event : appender.getEvents()) {
                if ("WARN".equals(event.getLevel())
                    && event.getMessage().startsWith("process-state-manager-test Failed to write offset checkpoint file to [")
                    && event.getMessage().endsWith(".checkpoint]." +
                        " This may occur if OS cleaned the state.dir in case when it located in /tmp directory." +
                        " This may also occur due to running multiple instances on the same machine using the same state dir." +
                        " Changing the location of state.dir may resolve the problem.")
                    && event.getThrowableInfo().get().startsWith("java.io.FileNotFoundException: ")) {

                    foundExpectedLogMessage = true;
                    break;
                }
            }
            assertTrue(foundExpectedLogMessage);
        }
    }

    @Test
    public void shouldThrowIfLoadCheckpointThrows() throws Exception {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
        final File file = new File(stateMgr.baseDir(), CHECKPOINT_FILE_NAME);
        file.createNewFile();
        final FileWriter writer = new FileWriter(file);
        writer.write("abcdefg");
        writer.close();

        try {
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);
            fail("should have thrown processor state exception when IO exception happens");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIfRestoreCallbackThrows() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE);

        stateMgr.registerStore(persistentStore, (key, value) -> {
            throw new RuntimeException("KABOOM!");
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
        } catch (final ProcessorStateException expected) { /* ignore */ }

        Assert.assertTrue(flushedStore.get());
    }

    @Test
    public void shouldCloseAllStoresEvenIfStoreThrowsException() {
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
        } catch (final ProcessorStateException expected) { /* ignore */ }

        Assert.assertTrue(closedStore.get());
    }

    @Test
    public void shouldThrowTaskCorruptedWithoutPersistentStoreCheckpointAndNonEmptyDir() throws IOException {
        final long checkpointOffset = 10L;

        final Map<TopicPartition, Long> offsets = mkMap(
            mkEntry(persistentStorePartition, checkpointOffset),
            mkEntry(nonPersistentStorePartition, checkpointOffset),
            mkEntry(irrelevantPartition, 999L)
        );
        checkpoint.write(offsets);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(persistentStoreTwo, persistentStoreTwo.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);

            final TaskCorruptedException exception = assertThrows(TaskCorruptedException.class,
                () -> stateMgr.initializeStoreOffsetsFromCheckpoint(false));

            assertEquals(
                Collections.singletonMap(taskId, stateMgr.changelogPartitions()),
                exception.corruptedTaskWithChangelogs()
            );
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldNotThrowTaskCorruptedWithoutInMemoryStoreCheckpointAndNonEmptyDir() throws IOException {
        final long checkpointOffset = 10L;

        final Map<TopicPartition, Long> offsets = mkMap(
            mkEntry(persistentStorePartition, checkpointOffset),
            mkEntry(irrelevantPartition, 999L)
        );
        checkpoint.write(offsets);

        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);

            stateMgr.initializeStoreOffsetsFromCheckpoint(false);
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldNotThrowTaskCorruptedExceptionAfterCheckpointing() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            stateMgr.initializeStoreOffsetsFromCheckpoint(true);

            assertThat(stateMgr.storeMetadata(nonPersistentStorePartition), notNullValue());
            assertThat(stateMgr.storeMetadata(persistentStorePartition), notNullValue());

            stateMgr.updateChangelogOffsets(mkMap(
                mkEntry(nonPersistentStorePartition, 876L),
                mkEntry(persistentStorePartition, 666L))
            );
            stateMgr.checkpoint();

            // reset the state and offsets, for example as in a corrupted task
            stateMgr.close();
            assertNull(stateMgr.storeMetadata(nonPersistentStorePartition));
            assertNull(stateMgr.storeMetadata(persistentStorePartition));

            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);

            // This should not throw a TaskCorruptedException!
            stateMgr.initializeStoreOffsetsFromCheckpoint(false);
            assertThat(stateMgr.storeMetadata(nonPersistentStorePartition), notNullValue());
            assertThat(stateMgr.storeMetadata(persistentStorePartition), notNullValue());
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldThrowIllegalStateIfInitializingOffsetsForCorruptedTasks() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.markChangelogAsCorrupted(mkSet(persistentStorePartition));

            final ProcessorStateException thrown = assertThrows(ProcessorStateException.class, () -> stateMgr.initializeStoreOffsetsFromCheckpoint(true));
            assertTrue(thrown.getCause() instanceof IllegalStateException);
        } finally {
            stateMgr.close();
        }
    }

    @Test
    public void shouldBeAbleToCloseWithoutRegisteringAnyStores() {
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);

        stateMgr.close();
    }

    @Test
    public void shouldDeleteCheckPointFileIfEosEnabled() throws IOException {
        final long checkpointOffset = 10L;
        final Map<TopicPartition, Long> offsets = mkMap(
                mkEntry(persistentStorePartition, checkpointOffset),
                mkEntry(nonPersistentStorePartition, checkpointOffset),
                mkEntry(irrelevantPartition, 999L)
        );
        checkpoint.write(offsets);
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, true);
        stateMgr.deleteCheckPointFileIfEOSEnabled();
        stateMgr.close();
        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldNotDeleteCheckPointFileIfEosNotEnabled() throws IOException {
        final long checkpointOffset = 10L;
        final Map<TopicPartition, Long> offsets = mkMap(
                mkEntry(persistentStorePartition, checkpointOffset),
                mkEntry(nonPersistentStorePartition, checkpointOffset),
                mkEntry(irrelevantPartition, 999L)
        );
        checkpoint.write(offsets);
        final ProcessorStateManager stateMgr = getStateManager(Task.TaskType.ACTIVE, false);
        stateMgr.deleteCheckPointFileIfEOSEnabled();
        stateMgr.close();
        assertTrue(checkpointFile.exists());
    }

    private ProcessorStateManager getStateManager(final Task.TaskType taskType, final boolean eosEnabled) {
        return new ProcessorStateManager(
            taskId,
            taskType,
            eosEnabled,
            logContext,
            stateDirectory,
            changelogReader,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(persistentStoreTwoName, persistentStoreTwoTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
            ),
            emptySet());
    }

    private ProcessorStateManager getStateManager(final Task.TaskType taskType) {
        return getStateManager(taskType, false);
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
