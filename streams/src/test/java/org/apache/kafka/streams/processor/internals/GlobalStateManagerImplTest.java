/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.NoOpProcessorContext;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobalStateManagerImplTest {


    private final TopicPartition t1 = new TopicPartition("t1", 1);
    private final TopicPartition t2 = new TopicPartition("t2", 1);
    private GlobalStateManagerImpl stateManager;
    private NoOpProcessorContext context;
    private StateDirectory stateDirectory;
    private String stateDirPath;
    private NoOpReadOnlyStore<Object, Object> store1;
    private NoOpReadOnlyStore store2;
    private MockConsumer<byte[], byte[]> consumer;
    private File checkpointFile;
    private final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
    private ProcessorTopology topology;

    @Before
    public void before() throws IOException {
        final Map<String, String> storeToTopic = new HashMap<>();
        storeToTopic.put("t1-store", "t1");
        storeToTopic.put("t2-store", "t2");

        final Map<StateStore, ProcessorNode> storeToProcessorNode = new HashMap<>();
        store1 = new NoOpReadOnlyStore<>("t1-store");
        storeToProcessorNode.put(store1, new MockProcessorNode(-1));
        store2 = new NoOpReadOnlyStore("t2-store");
        storeToProcessorNode.put(store2, new MockProcessorNode(-1));
        topology = new ProcessorTopology(Collections.<ProcessorNode>emptyList(),
                                         Collections.<String, SourceNode>emptyMap(),
                                         Collections.<String, SinkNode>emptyMap(),
                                         Collections.<StateStore>emptyList(),
                                         storeToTopic,
                                         Arrays.<StateStore>asList(store1, store2));

        context = new NoOpProcessorContext();
        stateDirPath = TestUtils.tempDirectory().getPath();
        stateDirectory = new StateDirectory("appId", stateDirPath);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        stateManager = new GlobalStateManagerImpl(topology, consumer, stateDirectory);
        checkpointFile = new File(stateManager.baseDir(), ProcessorStateManager.CHECKPOINT_FILE_NAME);
    }

    @After
    public void after() throws IOException {
        stateDirectory.unlockGlobalState();
    }

    @Test
    public void shouldLockGlobalStateDirectory() throws Exception {
        stateManager.initialize(context);
        assertTrue(new File(stateDirectory.globalStateDir(), ".lock").exists());
    }

    @Test(expected = LockException.class)
    public void shouldThrowLockExceptionIfCantGetLock() throws Exception {
        final StateDirectory stateDir = new StateDirectory("appId", stateDirPath);
        try {
            stateDir.lockGlobalState(1);
            stateManager.initialize(context);
        } finally {
            stateDir.unlockGlobalState();
        }
    }

    @Test
    public void shouldReadCheckpointOffsets() throws Exception {
        final Map<TopicPartition, Long> expected = writeCheckpoint();

        stateManager.initialize(context);
        final Map<TopicPartition, Long> offsets = stateManager.checkpointedOffsets();
        assertEquals(expected, offsets);
    }

    @Test
    public void shouldDeleteCheckpointFileAfteLoaded() throws Exception {
        writeCheckpoint();
        stateManager.initialize(context);
        assertFalse(checkpointFile.exists());
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfFailedToReadCheckpointedOffsets() throws Exception {
        writeCorruptCheckpoint();
        stateManager.initialize(context);
    }

    @Test
    public void shouldInitializeStateStores() throws Exception {
        stateManager.initialize(context);
        assertTrue(store1.initialized);
        assertTrue(store2.initialized);
    }

    @Test
    public void shouldReturnInitializedStoreNames() throws Exception {
        final Set<String> storeNames = stateManager.initialize(context);
        assertEquals(Utils.mkSet(store1.name(), store2.name()), storeNames);
    }

    @Test
    public void shouldThrowIllegalArgumentIfTryingToRegisterStoreThatIsNotGlobal() throws Exception {
        stateManager.initialize(context);

        try {
            stateManager.register(new NoOpReadOnlyStore<>("not-in-topology"), false, new TheStateRestoreCallback());
            fail("should have raised an illegal argument exception as store is not in the topology");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumenExceptionIfAttemptingToRegisterStoreTwice() throws Exception {
        stateManager.initialize(context);
        initializeConsumer(2, 1, t1);
        stateManager.register(store1, false, new TheStateRestoreCallback());
        try {
            stateManager.register(store1, false, new TheStateRestoreCallback());
            fail("should have raised an illegal argument exception as store has already been registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowStreamsExceptionIfNoPartitionsFoundForStore() throws Exception {
        stateManager.initialize(context);
        try {
            stateManager.register(store1, false, new TheStateRestoreCallback());
            fail("Should have raised a StreamsException as there are no partition for the store");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldRestoreRecordsUpToHighwatermark() throws Exception {
        initializeConsumer(2, 1, t1);

        stateManager.initialize(context);

        final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
        stateManager.register(store1, false, stateRestoreCallback);
        assertEquals(2, stateRestoreCallback.restored.size());
    }

    @Test
    public void shouldRestoreRecordsFromCheckpointToHighwatermark() throws Exception {
        initializeConsumer(5, 6, t1);

        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
                                                                                ProcessorStateManager.CHECKPOINT_FILE_NAME));
        offsetCheckpoint.write(Collections.singletonMap(t1, 6L));

        stateManager.initialize(context);
        final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
        stateManager.register(store1, false, stateRestoreCallback);
        assertEquals(5, stateRestoreCallback.restored.size());
    }


    @Test
    public void shouldFlushStateStores() throws Exception {
        stateManager.initialize(context);
        final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
        // register the stores
        initializeConsumer(1, 1, t1);
        stateManager.register(store1, false, stateRestoreCallback);
        initializeConsumer(1, 1, t2);
        stateManager.register(store2, false, stateRestoreCallback);

        stateManager.flush(context);
        assertTrue(store1.flushed);
        assertTrue(store2.flushed);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateStoreExceptionIfStoreFlushFailed() throws Exception {
        stateManager.initialize(context);
        final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
        // register the stores
        initializeConsumer(1, 1, t1);
        stateManager.register(new NoOpReadOnlyStore(store1.name()) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        }, false, stateRestoreCallback);

        stateManager.flush(context);
    }

    @Test
    public void shouldCloseStateStores() throws Exception {
        stateManager.initialize(context);
        final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
        // register the stores
        initializeConsumer(1, 1, t1);
        stateManager.register(store1, false, stateRestoreCallback);
        initializeConsumer(1, 1, t2);
        stateManager.register(store2, false, stateRestoreCallback);

        stateManager.close(Collections.<TopicPartition, Long>emptyMap());
        assertFalse(store1.isOpen());
        assertFalse(store2.isOpen());
    }

    @Test
    public void shouldWriteCheckpointsOnClose() throws Exception {
        stateManager.initialize(context);
        final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
        initializeConsumer(1, 1, t1);
        stateManager.register(store1, false, stateRestoreCallback);
        final Map<TopicPartition, Long> expected = Collections.singletonMap(t1, 25L);
        stateManager.close(expected);
        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
                                                                                ProcessorStateManager.CHECKPOINT_FILE_NAME));
        final Map<TopicPartition, Long> result = offsetCheckpoint.read();
        assertEquals(expected, result);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateStoreExceptionIfStoreCloseFailed() throws Exception {
        stateManager.initialize(context);
        initializeConsumer(1, 1, t1);
        stateManager.register(new NoOpReadOnlyStore(store1.name()) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        }, false, stateRestoreCallback);

        stateManager.close(Collections.<TopicPartition, Long>emptyMap());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallbackIsNull() throws Exception {
        stateManager.initialize(context);
        try {
            stateManager.register(store1, false, null);
            fail("should have thrown due to null callback");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldUnlockGlobalStateDirectoryOnClose() throws Exception {
        stateManager.initialize(context);
        stateManager.close(Collections.<TopicPartition, Long>emptyMap());
        final StateDirectory stateDir = new StateDirectory("appId", stateDirPath);
        try {
            // should be able to get the lock now as it should've been released in close
            assertTrue(stateDir.lockGlobalState(1));
        } finally {
            stateDir.unlockGlobalState();
        }
    }

    @Test
    public void shouldNotCloseStoresIfCloseAlreadyCalled() throws Exception {
        stateManager.initialize(context);
        initializeConsumer(1, 1, t1);
        stateManager.register(new NoOpReadOnlyStore("t1-store") {
            @Override
            public void close() {
                if (!isOpen()) {
                    throw new RuntimeException("store already closed");
                }
                super.close();
            }
        }, false, stateRestoreCallback);
        stateManager.close(Collections.<TopicPartition, Long>emptyMap());


        stateManager.close(Collections.<TopicPartition, Long>emptyMap());
    }

    @Test
    public void shouldAttemptToCloseAllStoresEvenWhenSomeException() throws Exception {
        stateManager.initialize(context);
        initializeConsumer(1, 1, t1);
        initializeConsumer(1, 1, t2);
        final NoOpReadOnlyStore store = new NoOpReadOnlyStore("t1-store") {
            @Override
            public void close() {
                super.close();
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.register(store, false, stateRestoreCallback);

        stateManager.register(store2, false, stateRestoreCallback);

        try {
            stateManager.close(Collections.<TopicPartition, Long>emptyMap());
        } catch (ProcessorStateException e) {
            // expected
        }
        assertFalse(store.isOpen());
        assertFalse(store2.isOpen());
    }

    @Test
    public void shouldReleaseLockIfExceptionWhenLoadingCheckpoints() throws Exception {
        writeCorruptCheckpoint();
        try {
            stateManager.initialize(context);
        } catch (StreamsException e) {
            // expected
        }
        final StateDirectory stateDir = new StateDirectory("appId", stateDirPath);
        try {
            // should be able to get the lock now as it should've been released
            assertTrue(stateDir.lockGlobalState(1));
        } finally {
            stateDir.unlockGlobalState();
        }
    }


    @Test(expected = LockException.class)
    public void shouldThrowLockExceptionIfIOExceptionCaughtWhenTryingToLockStateDir() throws Exception {
        stateManager = new GlobalStateManagerImpl(topology, consumer, new StateDirectory("appId", stateDirPath) {
            @Override
            public boolean lockGlobalState(final int retry) throws IOException {
                throw new IOException("KABOOM!");
            }
        });

        stateManager.initialize(context);
    }

    private void writeCorruptCheckpoint() throws IOException {
        final File checkpointFile = new File(stateManager.baseDir(), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        try (final FileOutputStream stream = new FileOutputStream(checkpointFile)) {
            stream.write("0\n1\nfoo".getBytes());
        }
    }

    private void initializeConsumer(final long numRecords, final long startOffset, final TopicPartition topicPartition) {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(topicPartition, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, startOffset + numRecords - 1);
        consumer.updatePartitions(topicPartition.topic(), Collections.singletonList(new PartitionInfo(topicPartition.topic(), topicPartition.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);

        for (int i = 0; i < numRecords; i++) {
            consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), startOffset + i, "key".getBytes(), "value".getBytes()));
        }
    }

    private Map<TopicPartition, Long> writeCheckpoint() throws IOException {
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(checkpointFile);
        final Map<TopicPartition, Long> expected = Collections.singletonMap(t1, 1L);
        checkpoint.write(expected);
        return expected;
    }

    private static class TheStateRestoreCallback implements StateRestoreCallback {
        private final List<KeyValue<byte[], byte[]>> restored = new ArrayList<>();

        @Override
        public void restore(final byte[] key, final byte[] value) {
            restored.add(KeyValue.pair(key, value));
        }
    }
}