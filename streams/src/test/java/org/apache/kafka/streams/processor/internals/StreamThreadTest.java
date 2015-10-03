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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class StreamThreadTest {

    private TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private TopicPartition t2p2 = new TopicPartition("topic2", 2);

    private Properties configProps() {
        return new Properties() {
            {
                setProperty(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                setProperty(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                setProperty(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                setProperty(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                setProperty(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.test.MockTimestampExtractor");
                setProperty(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
            }
        };
    }

    private static class TestStreamTask extends StreamTask {
        public boolean committed = false;

        public TestStreamTask(int id,
                              Consumer<byte[], byte[]> consumer,
                              Producer<byte[], byte[]> producer,
                              Consumer<byte[], byte[]> restoreConsumer,
                              Collection<TopicPartition> partitions,
                              ProcessorTopology topology,
                              StreamingConfig config) {
            super(id, consumer, producer, restoreConsumer, partitions, topology, config, null);
        }

        @Override
        public void commit() {
            super.commit();
            committed = true;
        }
    }

    private ByteArraySerializer serializer = new ByteArraySerializer();

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChange() throws Exception {
        StreamingConfig config = new StreamingConfig(configProps());

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");

        StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", new Metrics(), new SystemTime()) {
            @Override
            protected StreamTask createStreamTask(int id, Collection<TopicPartition> partitionsForTask) {
                return new TestStreamTask(id, consumer, producer, mockRestoreConsumer, partitionsForTask, builder.build(), config);
            }
        };

        ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        revokedPartitions = Collections.emptyList();
        assignedPartitions = Collections.singletonList(t1p1);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(1));
        assertEquals(expectedGroup1, thread.tasks().get(1).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.singletonList(t1p2);
        expectedGroup2 = new HashSet<>(Arrays.asList(t1p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(2));
        assertEquals(expectedGroup2, thread.tasks().get(2).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t1p2);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(1));
        assertTrue(thread.tasks().containsKey(2));
        assertEquals(expectedGroup1, thread.tasks().get(1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(2).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t1p2, t2p1, t2p2);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1, t2p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t1p2, t2p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(1));
        assertTrue(thread.tasks().containsKey(2));
        assertEquals(expectedGroup1, thread.tasks().get(1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(2).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.emptyList();

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().isEmpty());
    }

    @Test
    public void testMaybeClean() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final long cleanupDelay = 1000L;
            Properties props = configProps();
            props.setProperty(StreamingConfig.STATE_CLEANUP_DELAY_MS_CONFIG, Long.toString(cleanupDelay));
            props.setProperty(StreamingConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());

            StreamingConfig config = new StreamingConfig(props);

            File stateDir1 = new File(baseDir, "1");
            File stateDir2 = new File(baseDir, "2");
            File stateDir3 = new File(baseDir, "3");
            File extraDir = new File(baseDir, "X");
            stateDir1.mkdir();
            stateDir2.mkdir();
            stateDir3.mkdir();
            extraDir.mkdir();

            MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
            MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
            MockTime mockTime = new MockTime();

            TopologyBuilder builder = new TopologyBuilder();
            builder.addSource("source1", "topic1");

            StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", new Metrics(), mockTime) {
                @Override
                public void maybeClean() {
                    super.maybeClean();
                }
                @Override
                protected StreamTask createStreamTask(int id, Collection<TopicPartition> partitionsForTask) {
                    return new TestStreamTask(id, consumer, producer, mockRestoreConsumer, partitionsForTask, builder.build(), config);
                }
            };

            ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

            assertTrue(thread.tasks().isEmpty());
            mockTime.sleep(cleanupDelay);

            // all directories exist since an assignment didn't happen
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertTrue(stateDir3.exists());
            assertTrue(extraDir.exists());

            List<TopicPartition> revokedPartitions;
            List<TopicPartition> assignedPartitions;
            Map<Integer, StreamTask> prevTasks;

            //
            // Assign t1p1 and t1p2. This should create Task 1 & 2
            //
            revokedPartitions = Collections.emptyList();
            assignedPartitions = Arrays.asList(t1p1, t1p2);
            prevTasks = new HashMap(thread.tasks());

            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            // there shouldn't be any previous task
            assertTrue(prevTasks.isEmpty());

            // task 1 & 2 are created
            assertEquals(2, thread.tasks().size());

            // all directories should still exit before the cleanup delay time
            mockTime.sleep(cleanupDelay - 10L);
            thread.maybeClean();
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertTrue(stateDir3.exists());
            assertTrue(extraDir.exists());

            // all state directories except for task 1 & 2 will be removed. the extra directory should still exists
            mockTime.sleep(11L);
            thread.maybeClean();
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());

            //
            // Revoke t1p1 and t1p2. This should remove Task 1 & 2
            //
            revokedPartitions = assignedPartitions;
            assignedPartitions = Collections.emptyList();
            prevTasks = new HashMap(thread.tasks());

            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            // previous tasks should be committed
            assertEquals(2, prevTasks.size());
            for (StreamTask task : prevTasks.values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

            // no task
            assertTrue(thread.tasks().isEmpty());

            // all state directories for task 1 & 2 still exist before the cleanup delay time
            mockTime.sleep(cleanupDelay - 10L);
            thread.maybeClean();
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());

            // all state directories for task 1 & 2 are removed
            mockTime.sleep(11L);
            thread.maybeClean();
            assertFalse(stateDir1.exists());
            assertFalse(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testMaybeCommit() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final long commitInterval = 1000L;
            Properties props = configProps();
            props.setProperty(StreamingConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
            props.setProperty(StreamingConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

            StreamingConfig config = new StreamingConfig(props);

            MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
            MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            MockTime mockTime = new MockTime();

            TopologyBuilder builder = new TopologyBuilder();
            builder.addSource("source1", "topic1");

            StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", new Metrics(), mockTime) {
                @Override
                public void maybeCommit() {
                    super.maybeCommit();
                }
                @Override
                protected StreamTask createStreamTask(int id, Collection<TopicPartition> partitionsForTask) {
                    return new TestStreamTask(id, consumer, producer, mockRestoreConsumer, partitionsForTask, builder.build(), config);
                }
            };

            ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

            List<TopicPartition> revokedPartitions;
            List<TopicPartition> assignedPartitions;

            //
            // Assign t1p1 and t1p2. This should create Task 1 & 2
            //
            revokedPartitions = Collections.emptyList();
            assignedPartitions = Arrays.asList(t1p1, t1p2);

            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            assertEquals(2, thread.tasks().size());

            // no task is committed before the commit interval
            mockTime.sleep(commitInterval - 10L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertFalse(((TestStreamTask) task).committed);
            }

            // all tasks are committed after the commit interval
            mockTime.sleep(11L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

            // no task is committed before the commit interval, again
            mockTime.sleep(commitInterval - 10L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertFalse(((TestStreamTask) task).committed);
            }

            // all tasks are committed after the commit interval, again
            mockTime.sleep(11L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

        } finally {
            Utils.delete(baseDir);
        }
    }
}
