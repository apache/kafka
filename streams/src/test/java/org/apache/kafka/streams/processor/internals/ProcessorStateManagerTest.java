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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ProcessorStateManagerTest {

    private File baseDir;
    private StateDirectory stateDirectory;

    public static class MockRestoreConsumer extends MockConsumer<byte[], byte[]> {
        private final Serializer<Integer> serializer = new IntegerSerializer();

        public TopicPartition assignedPartition = null;
        public TopicPartition seekPartition = null;
        public long seekOffset = -1L;
        public boolean seekToBeginingCalled = false;
        public boolean seekToEndCalled = false;
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
    private final String stateDir = "test";
    private final String persistentStoreName = "persistentStore";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);

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

        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, new TaskId(0, 1), noPartitions, new MockRestoreConsumer(), false, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap());
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

        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, taskId, noPartitions, restoreConsumer, false, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap());
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

        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, new TaskId(0, 2), noPartitions, restoreConsumer, false, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap());
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

        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, taskId, sourcePartitions, restoreConsumer, true, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap()); // standby
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

        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, new TaskId(0, 1), noPartitions, restoreConsumer, false, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap());
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

        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, taskId, noPartitions, restoreConsumer, false, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap());
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
        ProcessorStateManager stateMgr = new ProcessorStateManager(applicationId, new TaskId(0, 1), noPartitions, new MockRestoreConsumer(), false, stateDirectory, null, Collections.<StateStore, ProcessorNode>emptyMap());
        stateMgr.register(mockStateStore, false, mockStateStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }

}
