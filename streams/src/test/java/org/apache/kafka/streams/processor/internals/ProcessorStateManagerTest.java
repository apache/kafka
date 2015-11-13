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
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.OffsetCheckpoint;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ProcessorStateManagerTest {

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
                new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                    serializer.serialize(record.topic(), record.key()),
                    serializer.serialize(record.topic(), record.value())));
            endOffset = record.offset();

            super.updateEndOffsets(Collections.singletonMap(assignedPartition, endOffset));
        }

        @Override
        public synchronized void assign(List<TopicPartition> partitions) {
            int numPartitions = partitions.size();
            if (numPartitions > 1)
                throw new IllegalArgumentException("RestoreConsumer: more than one partition specified");

            if (numPartitions == 1) {
                if (assignedPartition != null)
                    throw new IllegalStateException("RestoreConsumer: partition already assigned");
                assignedPartition = partitions.get(0);

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
        public synchronized void seekToBeginning(TopicPartition... partitions) {
            if (partitions.length != 1)
                throw new IllegalStateException("RestoreConsumer: other than one partition specified");

            for (TopicPartition partition : partitions) {
                if (!partition.equals(assignedPartition))
                    throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
            }

            seekToBeginingCalled = true;
            currentOffset = 0L;
        }

        @Override
        public synchronized void seekToEnd(TopicPartition... partitions) {
            if (partitions.length != 1)
                throw new IllegalStateException("RestoreConsumer: other than one partition specified");

            for (TopicPartition partition : partitions) {
                if (!partition.equals(assignedPartition))
                    throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
            }

            seekToEndCalled = true;
            currentOffset = endOffset;
        }
    }

    @Test
    public void testLockStateDirectory() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            FileLock lock;

            // the state manager locks the directory
            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, new MockRestoreConsumer(), false);

            try {
                // this should not get the lock
                lock = ProcessorStateManager.lockStateDirectory(baseDir);
                assertNull(lock);
            } finally {
                // by closing the state manager, release the lock
                stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
            }

            // now, this should get the lock
            lock = ProcessorStateManager.lockStateDirectory(baseDir);
            try {
                assertNotNull(lock);
            } finally {
                if (lock != null) lock.release();
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNoTopic() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore("mockStore", false);

            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, new MockRestoreConsumer(), false);
            try {
                stateMgr.register(mockStateStore, mockStateStore.stateRestoreCallback);
            } finally {
                stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testRegisterPersistentStore() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            long lastCheckpointedOffset = 10L;
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(baseDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
            checkpoint.write(Collections.singletonMap(new TopicPartition("persistentStore", 2), lastCheckpointedOffset));

            MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
            restoreConsumer.updatePartitions("persistentStore", Utils.mkList(
                    new PartitionInfo("persistentStore", 1, Node.noNode(), new Node[0], new Node[0]),
                    new PartitionInfo("persistentStore", 2, Node.noNode(), new Node[0], new Node[0])
            ));

            TopicPartition partition = new TopicPartition("persistentStore", 2);
            restoreConsumer.updateEndOffsets(Collections.singletonMap(partition, 13L));

            MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore("persistentStore", true); // persistent store

            ProcessorStateManager stateMgr = new ProcessorStateManager(2, baseDir, restoreConsumer, false);
            try {
                restoreConsumer.reset();

                ArrayList<Integer> expectedKeys = new ArrayList<>();
                long offset = -1L;
                for (int i = 1; i <= 3; i++) {
                    offset = (long) i;
                    int key = i * 10;
                    expectedKeys.add(key);
                    restoreConsumer.bufferRecord(
                            new ConsumerRecord<>("persistentStore", 2, offset, key, 0)
                    );
                }

                stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

                assertEquals(new TopicPartition("persistentStore", 2), restoreConsumer.assignedPartition);
                assertEquals(lastCheckpointedOffset, restoreConsumer.seekOffset);
                assertFalse(restoreConsumer.seekToBeginingCalled);
                assertTrue(restoreConsumer.seekToEndCalled);
                assertEquals(expectedKeys, persistentStore.keys);

            } finally {
                stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testRegisterNonPersistentStore() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            long lastCheckpointedOffset = 10L;
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(baseDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
            checkpoint.write(Collections.singletonMap(new TopicPartition("persistentStore", 2), lastCheckpointedOffset));

            MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
            restoreConsumer.updatePartitions("nonPersistentStore", Utils.mkList(
                    new PartitionInfo("nonPersistentStore", 1, Node.noNode(), new Node[0], new Node[0]),
                    new PartitionInfo("nonPersistentStore", 2, Node.noNode(), new Node[0], new Node[0])
            ));

            TopicPartition partition = new TopicPartition("persistentStore", 2);
            restoreConsumer.updateEndOffsets(Collections.singletonMap(partition, 13L));

            MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore("nonPersistentStore", false); // non persistent store

            ProcessorStateManager stateMgr = new ProcessorStateManager(2, baseDir, restoreConsumer, false);
            try {
                restoreConsumer.reset();

                ArrayList<Integer> expectedKeys = new ArrayList<>();
                long offset = -1L;
                for (int i = 1; i <= 3; i++) {
                    offset = (long) (i + 100);
                    int key = i;
                    expectedKeys.add(i);
                    restoreConsumer.bufferRecord(
                            new ConsumerRecord<>("nonPersistentStore", 2, offset, key, 0)
                    );
                }

                stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);

                assertEquals(new TopicPartition("nonPersistentStore", 2), restoreConsumer.assignedPartition);
                assertEquals(0L, restoreConsumer.seekOffset);
                assertTrue(restoreConsumer.seekToBeginingCalled);
                assertTrue(restoreConsumer.seekToEndCalled);
                assertEquals(expectedKeys, nonPersistentStore.keys);

            } finally {
                stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testChangeLogOffsets() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            long lastCheckpointedOffset = 10L;
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(baseDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
            checkpoint.write(Collections.singletonMap(new TopicPartition("store1", 0), lastCheckpointedOffset));

            MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
            restoreConsumer.updatePartitions("store1", Utils.mkList(
                    new PartitionInfo("store1", 0, Node.noNode(), new Node[0], new Node[0])
            ));
            restoreConsumer.updatePartitions("store2", Utils.mkList(
                    new PartitionInfo("store2", 0, Node.noNode(), new Node[0], new Node[0])
            ));

            TopicPartition partition1 = new TopicPartition("store1", 0);
            TopicPartition partition2 = new TopicPartition("store2", 0);

            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            endOffsets.put(partition1, 13L);
            endOffsets.put(partition2, 17L);
            restoreConsumer.updateEndOffsets(endOffsets);

            MockStateStoreSupplier.MockStateStore store1 = new MockStateStoreSupplier.MockStateStore("store1", true);
            MockStateStoreSupplier.MockStateStore store2 = new MockStateStoreSupplier.MockStateStore("store2", true);

            ProcessorStateManager stateMgr = new ProcessorStateManager(0, baseDir, restoreConsumer, true); // standby
            try {
                restoreConsumer.reset();

                stateMgr.register(store1, store1.stateRestoreCallback);
                stateMgr.register(store2, store2.stateRestoreCallback);

                Map<TopicPartition, Long> changeLogOffsets = stateMgr.checkpointedOffsets();

                assertEquals(2, changeLogOffsets.size());
                assertTrue(changeLogOffsets.containsKey(partition1));
                assertTrue(changeLogOffsets.containsKey(partition2));
                assertEquals(lastCheckpointedOffset, (long) changeLogOffsets.get(partition1));
                assertEquals(-1L, (long) changeLogOffsets.get(partition2));

            } finally {
                stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testGetStore() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
            restoreConsumer.updatePartitions("mockStore", Utils.mkList(
                    new PartitionInfo("mockStore", 1, Node.noNode(), new Node[0], new Node[0])
            ));

            MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore("mockStore", false);

            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, restoreConsumer, false);
            try {
                stateMgr.register(mockStateStore, mockStateStore.stateRestoreCallback);

                assertNull(stateMgr.getStore("noSuchStore"));
                assertEquals(mockStateStore, stateMgr.getStore("mockStore"));

            } finally {
                stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testClose() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        File checkpointFile = new File(baseDir, ProcessorStateManager.CHECKPOINT_FILE_NAME);
        try {
            // write an empty checkpoint file
            OffsetCheckpoint oldCheckpoint = new OffsetCheckpoint(checkpointFile);
            oldCheckpoint.write(Collections.<TopicPartition, Long>emptyMap());

            MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
            restoreConsumer.updatePartitions("persistentStore", Utils.mkList(
                    new PartitionInfo("persistentStore", 1, Node.noNode(), new Node[0], new Node[0])
            ));
            restoreConsumer.updatePartitions("nonPersistentStore", Utils.mkList(
                    new PartitionInfo("nonPersistentStore", 1, Node.noNode(), new Node[0], new Node[0])
            ));

            // set up ack'ed offsets
            HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
            ackedOffsets.put(new TopicPartition("persistentStore", 1), 123L);
            ackedOffsets.put(new TopicPartition("nonPersistentStore", 1), 456L);
            ackedOffsets.put(new TopicPartition("otherTopic", 1), 789L);

            MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore("persistentStore", true);
            MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore("nonPersistentStore", false);

            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, restoreConsumer, false);
            try {
                // make sure the checkpoint file is deleted
                assertFalse(checkpointFile.exists());

                restoreConsumer.reset();
                stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

                restoreConsumer.reset();
                stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            } finally {
                // close the state manager with the ack'ed offsets
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
            assertEquals(new Long(123L + 1L), checkpointedOffsets.get(new TopicPartition("persistentStore", 1)));
        } finally {
            Utils.delete(baseDir);
        }
    }

}
