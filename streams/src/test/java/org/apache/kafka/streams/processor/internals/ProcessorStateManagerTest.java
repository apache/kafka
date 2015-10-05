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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.OffsetCheckpoint;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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

    private static class MockStateStore implements StateStore {
        private final String name;
        private final boolean persistent;

        public boolean flushed = false;
        public boolean closed = false;
        public final ArrayList<Integer> keys = new ArrayList<>();

        public MockStateStore(String name, boolean persistent) {
            this.name = name;
            this.persistent = persistent;
        }
        @Override
        public String name() {
            return name;
        }
        @Override
        public void flush() {
            flushed = true;
        }
        @Override
        public void close() {
            closed = true;
        }
        @Override
        public boolean persistent() {
            return persistent;
        }

        public final StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {
            private final Deserializer<Integer> deserializer = new IntegerDeserializer();

            @Override
            public void restore(byte[] key, byte[] value) {
                keys.add(deserializer.deserialize("", key));
            }
        };
    }

    private class MockRestoreConsumer  extends MockConsumer<byte[], byte[]> {
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

        // buffer a record (we cannot use addRecord because we need to add records before asigning a partition)
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
            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, new MockRestoreConsumer());

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
            MockStateStore mockStateStore = new MockStateStore("mockStore", false);

            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, new MockRestoreConsumer());
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
            restoreConsumer.updatePartitions("persistentStore", Arrays.asList(
                    new PartitionInfo("persistentStore", 1, Node.noNode(), new Node[0], new Node[0]),
                    new PartitionInfo("persistentStore", 2, Node.noNode(), new Node[0], new Node[0])
            ));
            restoreConsumer.updateEndOffsets(Collections.singletonMap(new TopicPartition("persistentStore", 2), 13L));

            MockStateStore persistentStore = new MockStateStore("persistentStore", true); // persistent store

            ProcessorStateManager stateMgr = new ProcessorStateManager(2, baseDir, restoreConsumer);
            try {
                restoreConsumer.reset();

                ArrayList<Integer> expectedKeys = new ArrayList<>();
                for (int i = 1; i <= 3; i++) {
                    long offset = (long) i;
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
            restoreConsumer.updatePartitions("nonPersistentStore", Arrays.asList(
                    new PartitionInfo("nonPersistentStore", 1, Node.noNode(), new Node[0], new Node[0]),
                    new PartitionInfo("nonPersistentStore", 2, Node.noNode(), new Node[0], new Node[0])
            ));
            restoreConsumer.updateEndOffsets(Collections.singletonMap(new TopicPartition("persistentStore", 2), 13L));

            MockStateStore nonPersistentStore = new MockStateStore("nonPersistentStore", false); // non persistent store

            ProcessorStateManager stateMgr = new ProcessorStateManager(2, baseDir, restoreConsumer);
            try {
                restoreConsumer.reset();

                ArrayList<Integer> expectedKeys = new ArrayList<>();
                for (int i = 1; i <= 3; i++) {
                    long offset = (long) (i + 100);
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
    public void testGetStore() throws IOException {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
            restoreConsumer.updatePartitions("mockStore", Arrays.asList(
                    new PartitionInfo("mockStore", 1, Node.noNode(), new Node[0], new Node[0])
            ));

            MockStateStore mockStateStore = new MockStateStore("mockStore", false);

            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, restoreConsumer);
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
            restoreConsumer.updatePartitions("persistentStore", Arrays.asList(
                    new PartitionInfo("persistentStore", 1, Node.noNode(), new Node[0], new Node[0])
            ));
            restoreConsumer.updatePartitions("nonPersistentStore", Arrays.asList(
                    new PartitionInfo("nonPersistentStore", 1, Node.noNode(), new Node[0], new Node[0])
            ));

            // set up ack'ed offsets
            HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
            ackedOffsets.put(new TopicPartition("persistentStore", 1), 123L);
            ackedOffsets.put(new TopicPartition("nonPersistentStore", 1), 456L);
            ackedOffsets.put(new TopicPartition("otherTopic", 1), 789L);

            MockStateStore persistentStore = new MockStateStore("persistentStore", true);
            MockStateStore nonPersistentStore = new MockStateStore("nonPersistentStore", false);

            ProcessorStateManager stateMgr = new ProcessorStateManager(1, baseDir, restoreConsumer);
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
