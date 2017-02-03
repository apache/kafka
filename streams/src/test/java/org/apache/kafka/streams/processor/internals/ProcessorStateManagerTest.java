/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ProcessorStateManagerTest {

    public static class MockRestoreConsumer extends MockConsumer<byte[], byte[]> {
        private final Serializer<Integer> serializer = new IntegerSerializer();

        private TopicPartition assignedPartition = null;
        private TopicPartition seekPartition = null;
        private long seekOffset = -1L;
        private boolean seekToBeginingCalled = false;
        private boolean seekToEndCalled = false;
        private long endOffset = 0L;
        private long currentOffset = 0L;

        private ArrayList<ConsumerRecord<byte[], byte[]>> recordBuffer = new ArrayList<>();

        MockRestoreConsumer() {
            super(OffsetResetStrategy.EARLIEST);

            reset();
        }

        // reset this mock restore consumer for a state store registration
        public void reset() {
            assignedPartition = null;
            seekOffset = -1L;
            seekToBeginingCalled = false;
            seekToEndCalled = false;
            endOffset = 0L;
            recordBuffer.clear();
        }

        // buffer a record (we cannot use addRecord because we need to add records before assigning a partition)
        public void bufferRecord(ConsumerRecord<Integer, Integer> record) {
            recordBuffer.add(
                new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), 0L,
                    TimestampType.CREATE_TIME, 0L, 0, 0,
                    serializer.serialize(record.topic(), record.key()),
                    serializer.serialize(record.topic(), record.value())));
            endOffset = record.offset();

            super.updateEndOffsets(Collections.singletonMap(assignedPartition, endOffset));
        }

        @Override
        public synchronized void assign(Collection<TopicPartition> partitions) {
            int numPartitions = partitions.size();
            if (numPartitions > 1)
                throw new IllegalArgumentException("RestoreConsumer: more than one partition specified");

            if (numPartitions == 1) {
                if (assignedPartition != null)
                    throw new IllegalStateException("RestoreConsumer: partition already assigned");
                assignedPartition = partitions.iterator().next();

                // set the beginning offset to 0
                // NOTE: this is users responsible to set the initial lEO.
                super.updateBeginningOffsets(Collections.singletonMap(assignedPartition, 0L));
            }

            super.assign(partitions);
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(long timeout) {
            // add buffered records to MockConsumer
            for (ConsumerRecord<byte[], byte[]> record : recordBuffer) {
                super.addRecord(record);
            }
            recordBuffer.clear();

            ConsumerRecords<byte[], byte[]> records = super.poll(timeout);

            // set the current offset
            Iterable<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(assignedPartition);
            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                currentOffset = record.offset();
            }

            return records;
        }

        @Override
        public synchronized long position(TopicPartition partition) {
            if (!partition.equals(assignedPartition))
                throw new IllegalStateException("RestoreConsumer: unassigned partition");

            return currentOffset;
        }

        @Override
        public synchronized void seek(TopicPartition partition, long offset) {
            if (offset < 0)
                throw new IllegalArgumentException("RestoreConsumer: offset should not be negative");

            if (seekOffset >= 0)
                throw new IllegalStateException("RestoreConsumer: offset already seeked");

            seekPartition = partition;
            seekOffset = offset;
            currentOffset = offset;
            super.seek(partition, offset);
        }

        @Override
        public synchronized void seekToBeginning(Collection<TopicPartition> partitions) {
            if (partitions.size() != 1)
                throw new IllegalStateException("RestoreConsumer: other than one partition specified");

            for (TopicPartition partition : partitions) {
                if (!partition.equals(assignedPartition))
                    throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
            }

            seekToBeginingCalled = true;
            currentOffset = 0L;
        }

        @Override
        public synchronized void seekToEnd(Collection<TopicPartition> partitions) {
            if (partitions.size() != 1)
                throw new IllegalStateException("RestoreConsumer: other than one partition specified");

            for (TopicPartition partition : partitions) {
                if (!partition.equals(assignedPartition))
                    throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
            }

            seekToEndCalled = true;
            currentOffset = endOffset;
        }
    }

    private final Set<TopicPartition> noPartitions = Collections.emptySet();
    private final String applicationId = "test-application";
    private final String persistentStoreName = "persistentStore";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private final String storeName = "mockStateStore";
    private final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);
    private final TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
    private final TaskId taskId = new TaskId(0, 1);
    private final MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
    private final MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(storeName, true);
    private File baseDir;
    private StateDirectory stateDirectory;


    @Before
    public void setup() {
        baseDir = TestUtils.tempDirectory();
        stateDirectory = new StateDirectory(applicationId, baseDir.getPath());
    }

    @After
    public void cleanup() {
        Utils.delete(baseDir);
    }

    @Test(expected = StreamsException.class)
    public void testNoTopic() throws IOException {
        MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);

        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, new MockRestoreConsumer(), false, stateDirectory, new HashMap<String, String>() {
            {
                put(nonPersistentStoreName, nonPersistentStoreName);
            }
        });

        try {
            stateMgr.register(mockStateStore, true, mockStateStore.stateRestoreCallback);
        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testRegisterPersistentStore() throws IOException {
        final TaskId taskId = new TaskId(0, 2);
        long lastCheckpointedOffset = 10L;

        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(new TopicPartition(persistentStoreTopicName, 2), lastCheckpointedOffset));

        MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();

        restoreConsumer.updatePartitions(persistentStoreTopicName, Utils.mkList(
                new PartitionInfo(persistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(persistentStoreTopicName, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        TopicPartition partition = new TopicPartition(persistentStoreTopicName, 2);
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition, 13L));

        MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore("persistentStore", true); // persistent store

        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, restoreConsumer, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreName);
            }
        });
        try {
            restoreConsumer.reset();

            ArrayList<Integer> expectedKeys = new ArrayList<>();
            long offset;
            for (int i = 1; i <= 3; i++) {
                offset = (long) i;
                int key = i * 10;
                expectedKeys.add(key);
                restoreConsumer.bufferRecord(
                        new ConsumerRecord<>(persistentStoreTopicName, 2, 0L, offset, TimestampType.CREATE_TIME, 0L, 0, 0, key, 0)
                );
            }

            stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);

            assertEquals(new TopicPartition(persistentStoreTopicName, 2), restoreConsumer.assignedPartition);
            assertEquals(lastCheckpointedOffset, restoreConsumer.seekOffset);
            assertFalse(restoreConsumer.seekToBeginingCalled);
            assertTrue(restoreConsumer.seekToEndCalled);
            assertEquals(expectedKeys, persistentStore.keys);

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }


    }

    @Test
    public void testRegisterNonPersistentStore() throws IOException {
        long lastCheckpointedOffset = 10L;

        MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();

        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(baseDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(new TopicPartition(persistentStoreTopicName, 2), lastCheckpointedOffset));

        restoreConsumer.updatePartitions(nonPersistentStoreTopicName, Utils.mkList(
                new PartitionInfo(nonPersistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(nonPersistentStoreTopicName, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        TopicPartition partition = new TopicPartition(persistentStoreTopicName, 2);
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition, 13L));

        MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false); // non persistent store

        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 2), noPartitions, restoreConsumer, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreTopicName);
            }
        });
        try {
            restoreConsumer.reset();

            ArrayList<Integer> expectedKeys = new ArrayList<>();
            long offset = -1L;
            for (int i = 1; i <= 3; i++) {
                offset = (long) (i + 100);
                int key = i;
                expectedKeys.add(i);
                restoreConsumer.bufferRecord(
                        new ConsumerRecord<>(nonPersistentStoreTopicName, 2, 0L, offset, TimestampType.CREATE_TIME, 0L, 0, 0, key, 0)
                );
            }

            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);

            assertEquals(new TopicPartition(nonPersistentStoreTopicName, 2), restoreConsumer.assignedPartition);
            assertEquals(0L, restoreConsumer.seekOffset);
            assertTrue(restoreConsumer.seekToBeginingCalled);
            assertTrue(restoreConsumer.seekToEndCalled);
            assertEquals(expectedKeys, nonPersistentStore.keys);

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }

    }

    @Test
    public void testChangeLogOffsets() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        long lastCheckpointedOffset = 10L;
        String storeName1 = "store1";
        String storeName2 = "store2";
        String storeName3 = "store3";

        String storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
        String storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
        String storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

        Map<String, String> storeToChangelogTopic = new HashMap<>();
        storeToChangelogTopic.put(storeName1, storeTopicName1);
        storeToChangelogTopic.put(storeName2, storeTopicName2);
        storeToChangelogTopic.put(storeName3, storeTopicName3);

        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(new TopicPartition(storeTopicName1, 0), lastCheckpointedOffset));

        MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();

        restoreConsumer.updatePartitions(storeTopicName1, Utils.mkList(
                new PartitionInfo(storeTopicName1, 0, Node.noNode(), new Node[0], new Node[0])
        ));
        restoreConsumer.updatePartitions(storeTopicName2, Utils.mkList(
                new PartitionInfo(storeTopicName2, 0, Node.noNode(), new Node[0], new Node[0])
        ));
        restoreConsumer.updatePartitions(storeTopicName3, Utils.mkList(
                new PartitionInfo(storeTopicName3, 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(storeTopicName3, 1, Node.noNode(), new Node[0], new Node[0])
        ));

        TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(partition1, 13L);
        endOffsets.put(partition2, 17L);
        restoreConsumer.updateEndOffsets(endOffsets);

        MockStateStoreSupplier.MockStateStore store1 = new MockStateStoreSupplier.MockStateStore(storeName1, true);
        MockStateStoreSupplier.MockStateStore store2 = new MockStateStoreSupplier.MockStateStore(storeName2, true);
        MockStateStoreSupplier.MockStateStore store3 = new MockStateStoreSupplier.MockStateStore(storeName3, true);

        // if there is an source partition, inherit the partition id
        Set<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, sourcePartitions, restoreConsumer, true, stateDirectory, storeToChangelogTopic); // standby
        try {
            restoreConsumer.reset();

            stateMgr.register(store1, true, store1.stateRestoreCallback);
            stateMgr.register(store2, true, store2.stateRestoreCallback);
            stateMgr.register(store3, true, store3.stateRestoreCallback);

            Map<TopicPartition, Long> changeLogOffsets = stateMgr.checkpointedOffsets();

            assertEquals(3, changeLogOffsets.size());
            assertTrue(changeLogOffsets.containsKey(partition1));
            assertTrue(changeLogOffsets.containsKey(partition2));
            assertTrue(changeLogOffsets.containsKey(partition3));
            assertEquals(lastCheckpointedOffset, (long) changeLogOffsets.get(partition1));
            assertEquals(-1L, (long) changeLogOffsets.get(partition2));
            assertEquals(-1L, (long) changeLogOffsets.get(partition3));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }

    }

    @Test
    public void testGetStore() throws IOException {
        MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();

        restoreConsumer.updatePartitions(nonPersistentStoreTopicName, Utils.mkList(
                new PartitionInfo(nonPersistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));

        MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);

        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, restoreConsumer, false, stateDirectory, Collections.<String, String>emptyMap());
        try {
            stateMgr.register(mockStateStore, true, mockStateStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(mockStateStore, stateMgr.getStore(nonPersistentStoreName));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testFlushAndClose() throws IOException {
        final TaskId taskId = new TaskId(0, 1);
        File checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        // write an empty checkpoint file
        OffsetCheckpoint oldCheckpoint = new OffsetCheckpoint(checkpointFile);
        oldCheckpoint.write(Collections.<TopicPartition, Long>emptyMap());

        MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();

        restoreConsumer.updatePartitions(persistentStoreTopicName, Utils.mkList(
                new PartitionInfo(persistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));
        restoreConsumer.updatePartitions(nonPersistentStoreTopicName, Utils.mkList(
                new PartitionInfo(nonPersistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));

        // set up ack'ed offsets
        HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
        ackedOffsets.put(new TopicPartition(persistentStoreTopicName, 1), 123L);
        ackedOffsets.put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
        ackedOffsets.put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

        MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore(persistentStoreName, true);
        MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);

        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, restoreConsumer, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreTopicName);
            }
        });
        try {
            // make sure the checkpoint file is deleted
            assertFalse(checkpointFile.exists());

            restoreConsumer.reset();
            stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);

            restoreConsumer.reset();
            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
        } finally {
            // close the state manager with the ack'ed offsets
            stateMgr.flush(new MockProcessorContext(StateSerdes.withBuiltinTypes("foo", String.class, String.class), new NoOpRecordCollector()));
            stateMgr.close(ackedOffsets);
        }

        // make sure all stores are closed, and the checkpoint file is written.
        assertTrue(persistentStore.flushed);
        assertTrue(persistentStore.closed);
        assertTrue(nonPersistentStore.flushed);
        assertTrue(nonPersistentStore.closed);
        assertTrue(checkpointFile.exists());

        // the checkpoint file should contain an offset from the persistent store only.
        OffsetCheckpoint newCheckpoint = new OffsetCheckpoint(checkpointFile);
        Map<TopicPartition, Long> checkpointedOffsets = newCheckpoint.read();
        assertEquals(1, checkpointedOffsets.size());
        assertEquals(new Long(123L + 1L), checkpointedOffsets.get(new TopicPartition(persistentStoreTopicName, 1)));
    }

    @Test
    public void shouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic() throws Exception {
        MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);
        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, new MockRestoreConsumer(), false, stateDirectory, Collections.<String, String>emptyMap());
        stateMgr.register(mockStateStore, false, mockStateStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }

    @Test
    public void shouldNotWriteCheckpointsIfAckeOffsetsIsNull() throws Exception {
        final File checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        // write an empty checkpoint file
        final OffsetCheckpoint oldCheckpoint = new OffsetCheckpoint(checkpointFile);
        oldCheckpoint.write(Collections.<TopicPartition, Long>emptyMap());

        restoreConsumer.updatePartitions(persistentStoreTopicName, Utils.mkList(
                new PartitionInfo(persistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));


        final MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, restoreConsumer, false, stateDirectory, Collections.<String, String>emptyMap());

        restoreConsumer.reset();
        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
        stateMgr.close(null);
        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectory() throws Exception {
        final File taskDirectory = stateDirectory.directoryForTask(taskId);
        final FileChannel channel = FileChannel.open(new File(taskDirectory,
                                                              StateDirectory.LOCK_FILE_NAME).toPath(),
                                                     StandardOpenOption.CREATE,
                                                     StandardOpenOption.WRITE);
        // lock the task directory
        final FileLock lock = channel.lock();

        try {
            new ProcessorStateManager(taskId, noPartitions, restoreConsumer, false, stateDirectory, Collections.<String, String>emptyMap());
            fail("Should have thrown LockException");
        } catch (final LockException e) {
           // pass
        } finally {
            lock.release();
            channel.close();
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() throws Exception {
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             noPartitions,
                                                                             restoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.<String, String>emptyMap());

        try {
            stateManager.register(new MockStateStoreSupplier.MockStateStore(ProcessorStateManager.CHECKPOINT_FILE_NAME, true), true, null);
            fail("should have thrown illegal argument exception when store name same as checkpoint file");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered() throws Exception {
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             noPartitions,
                                                                             restoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.<String, String>emptyMap());
        stateManager.register(mockStateStore, false, null);

        try {
            stateManager.register(mockStateStore, false, null);
            fail("should have thrown illegal argument exception when store with same name already registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }
        
    }

    @Test
    public void shouldThrowStreamsExceptionWhenRestoreConsumerThrowsTimeoutException() throws Exception {
        final MockRestoreConsumer mockRestoreConsumer = new MockRestoreConsumer() {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                throw new TimeoutException("KABOOM!");
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             noPartitions,
                                                                             mockRestoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.singletonMap(storeName, changelogTopic));
        try {
            stateManager.register(mockStateStore, false, null);
            fail("should have thrown StreamsException due to timeout exception");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowStreamsExceptionWhenRestoreConsumerReturnsNullPartitions() throws Exception {
        final MockRestoreConsumer mockRestoreConsumer = new MockRestoreConsumer() {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return null;
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             noPartitions,
                                                                             mockRestoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.singletonMap(storeName, changelogTopic));
        try {
            stateManager.register(mockStateStore, false, null);
            fail("should have thrown StreamsException due to timeout exception");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowStreamsExceptionWhenPartitionForTopicNotFound() throws Exception {
        final MockRestoreConsumer mockRestoreConsumer = new MockRestoreConsumer() {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.singletonList(new PartitionInfo(changelogTopic, 0, null, null, null));
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             Collections.singleton(new TopicPartition(changelogTopic, 1)),
                                                                             mockRestoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.singletonMap(storeName, changelogTopic));

        try {
            stateManager.register(mockStateStore, false, null);
            fail("should have thrown StreamsException due to partition for topic not found");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenRestoringStateAndSubscriptionsNonEmpty() throws Exception {
        final MockRestoreConsumer mockRestoreConsumer = new MockRestoreConsumer() {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.singletonList(new PartitionInfo(changelogTopic, 0, null, null, null));
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             Collections.singleton(changelogTopicPartition),
                                                                             mockRestoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.singletonMap(storeName, changelogTopic));

        mockRestoreConsumer.subscribe(Collections.singleton("sometopic"));

        try {
            stateManager.register(mockStateStore, false, null);
            fail("should throw IllegalStateException when restore consumer has non-empty subscriptions");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenRestoreConsumerPositionGreaterThanEndOffset() throws Exception {
        final AtomicInteger position = new AtomicInteger(10);
        final MockRestoreConsumer mockRestoreConsumer = new MockRestoreConsumer() {
            @Override
            public synchronized long position(final TopicPartition partition) {
                // need to make the end position change to trigger the exception
                return position.getAndIncrement();
            }
        };

        mockRestoreConsumer.updatePartitions(changelogTopic, Collections.singletonList(new PartitionInfo(changelogTopic, 0, null, null, null)));

        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             Collections.singleton(changelogTopicPartition),
                                                                             mockRestoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.singletonMap(storeName, changelogTopic));

        stateManager.putOffsetLimit(changelogTopicPartition, 1);
        // add a record with an offset less than the limit of 1
        mockRestoreConsumer.bufferRecord(new ConsumerRecord<>(changelogTopic, 0, 0, 1, 1));


        try {
            stateManager.register(mockStateStore, false, mockStateStore.stateRestoreCallback);
            fail("should have thrown IllegalStateException as end offset has changed");
        } catch (final IllegalStateException e) {
            // pass
        }

    }

    @Test
    public void shouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException() throws Exception {
        restoreConsumer.updatePartitions(changelogTopic, Collections.singletonList(new PartitionInfo(changelogTopic, 0, null, null, null)));

        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             Collections.singleton(changelogTopicPartition),
                                                                             restoreConsumer,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.singletonMap(storeName, changelogTopic));

        final MockStateStoreSupplier.MockStateStore stateStore = new MockStateStoreSupplier.MockStateStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.putOffsetLimit(changelogTopicPartition, 1);
        restoreConsumer.bufferRecord(new ConsumerRecord<>(changelogTopic, 0, 1, 1, 1));
        stateManager.register(stateStore, false, stateStore.stateRestoreCallback);

        try {
            stateManager.close(Collections.<TopicPartition, Long>emptyMap());
            fail("Should throw ProcessorStateException if store close throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }


}
