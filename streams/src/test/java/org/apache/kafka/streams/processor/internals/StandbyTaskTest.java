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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StandbyTaskTest {

    private final TaskId taskId = new TaskId(0, 1);

    private final Serializer<Integer> intSerializer = new IntegerSerializer();

    private final String applicationId = "test-application";
    private final String storeName1 = "store1";
    private final String storeName2 = "store2";
    private final String storeChangelogTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
    private final String storeChangelogTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);

    private final TopicPartition partition1 = new TopicPartition(storeChangelogTopicName1, 1);
    private final TopicPartition partition2 = new TopicPartition(storeChangelogTopicName2, 1);

    private final Set<TopicPartition> topicPartitions = Collections.emptySet();
    private final ProcessorTopology topology = new ProcessorTopology(
            Collections.<ProcessorNode>emptyList(),
            Collections.<String, SourceNode>emptyMap(),
            Collections.<String, SinkNode>emptyMap(),
            Utils.mkList(
                    new MockStateStoreSupplier(storeName1, false).get(),
                    new MockStateStoreSupplier(storeName2, true).get()
            ),
            Collections.<String, String>emptyMap(),
            Collections.<StateStore, ProcessorNode>emptyMap());

    private final TopicPartition ktable = new TopicPartition("ktable1", 0);
    private final Set<TopicPartition> ktablePartitions = Utils.mkSet(ktable);
    private final ProcessorTopology ktableTopology = new ProcessorTopology(
            Collections.<ProcessorNode>emptyList(),
            Collections.<String, SourceNode>emptyMap(),
            Collections.<String, SinkNode>emptyMap(),
            Utils.mkList(
                    new MockStateStoreSupplier(ktable.topic(), true, false).get()
            ),
            new HashMap<String, String>() {
            {
                put("ktable1", ktable.topic());
            }
        },
            Collections.<StateStore, ProcessorNode>emptyMap());
    private File baseDir;
    private StateDirectory stateDirectory;

    private StreamsConfig createConfig(final File baseDir) throws Exception {
        return new StreamsConfig(new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
                setProperty(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
            }
        });
    }

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final ProcessorStateManagerTest.MockRestoreConsumer restoreStateConsumer = new ProcessorStateManagerTest.MockRestoreConsumer();

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    @Before
    public void setup() {
        restoreStateConsumer.reset();
        restoreStateConsumer.updatePartitions(storeChangelogTopicName1, Utils.mkList(
                new PartitionInfo(storeChangelogTopicName1, 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(storeChangelogTopicName1, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(storeChangelogTopicName1, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        restoreStateConsumer.updatePartitions(storeChangelogTopicName2, Utils.mkList(
                new PartitionInfo(storeChangelogTopicName2, 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(storeChangelogTopicName2, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(storeChangelogTopicName2, 2, Node.noNode(), new Node[0], new Node[0])
        ));
        baseDir = TestUtils.tempDirectory();
        stateDirectory = new StateDirectory(applicationId, baseDir.getPath());
    }

    @After
    public void cleanup() {
        Utils.delete(baseDir);
    }

    @Test
    public void testStorePartitions() throws Exception {
        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, applicationId, topicPartitions, topology, consumer, restoreStateConsumer, config, null, stateDirectory);

        assertEquals(Utils.mkSet(partition2), new HashSet<>(task.changeLogPartitions()));

    }

    @SuppressWarnings("unchecked")
    @Test(expected = Exception.class)
    public void testUpdateNonPersistentStore() throws Exception {
        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, applicationId, topicPartitions, topology, consumer, restoreStateConsumer, config, null, stateDirectory);

        restoreStateConsumer.assign(new ArrayList<>(task.changeLogPartitions()));

        task.update(partition1,
                records(new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue))
        );

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdate() throws Exception {
        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, applicationId, topicPartitions, topology, consumer, restoreStateConsumer, config, null, stateDirectory);

        restoreStateConsumer.assign(new ArrayList<>(task.changeLogPartitions()));

        for (ConsumerRecord<Integer, Integer> record : Arrays.asList(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1, 100),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2, 100),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 3, 100))) {
            restoreStateConsumer.bufferRecord(record);
        }

        for (Map.Entry<TopicPartition, Long> entry : task.checkpointedOffsets().entrySet()) {
            TopicPartition partition = entry.getKey();
            long offset = entry.getValue();
            if (offset >= 0) {
                restoreStateConsumer.seek(partition, offset);
            } else {
                restoreStateConsumer.seekToBeginning(singleton(partition));
            }
        }

        task.update(partition2, restoreStateConsumer.poll(100).records(partition2));

        StandbyContextImpl context = (StandbyContextImpl) task.context();
        MockStateStoreSupplier.MockStateStore store1 =
                (MockStateStoreSupplier.MockStateStore) context.getStateMgr().getStore(storeName1);
        MockStateStoreSupplier.MockStateStore store2 =
                (MockStateStoreSupplier.MockStateStore) context.getStateMgr().getStore(storeName2);

        assertEquals(Collections.emptyList(), store1.keys);
        assertEquals(Utils.mkList(1, 2, 3), store2.keys);

        task.closeStateManager();

        File taskDir = stateDirectory.directoryForTask(taskId);
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        Map<TopicPartition, Long> offsets = checkpoint.read();

        assertEquals(1, offsets.size());
        assertEquals(new Long(30L + 1L), offsets.get(partition2));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateKTable() throws Exception {
        consumer.assign(Utils.mkList(ktable));
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(ktable.topic(), ktable.partition()), new OffsetAndMetadata(0L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions("ktable1", Utils.mkList(
                new PartitionInfo("ktable1", 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo("ktable1", 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo("ktable1", 2, Node.noNode(), new Node[0], new Node[0])
        ));

        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, applicationId, ktablePartitions, ktableTopology, consumer, restoreStateConsumer, config, null, stateDirectory);

        restoreStateConsumer.assign(new ArrayList<>(task.changeLogPartitions()));

        for (ConsumerRecord<Integer, Integer> record : Arrays.asList(
                new ConsumerRecord<>(ktable.topic(), ktable.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1, 100),
                new ConsumerRecord<>(ktable.topic(), ktable.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2, 100),
                new ConsumerRecord<>(ktable.topic(), ktable.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 3, 100),
                new ConsumerRecord<>(ktable.topic(), ktable.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 4, 100),
                new ConsumerRecord<>(ktable.topic(), ktable.partition(), 50, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 5, 100))) {
            restoreStateConsumer.bufferRecord(record);
        }

        for (Map.Entry<TopicPartition, Long> entry : task.checkpointedOffsets().entrySet()) {
            TopicPartition partition = entry.getKey();
            long offset = entry.getValue();
            if (offset >= 0) {
                restoreStateConsumer.seek(partition, offset);
            } else {
                restoreStateConsumer.seekToBeginning(singleton(partition));
            }
        }

        // The commit offset is at 0L. Records should not be processed
        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(ktable, restoreStateConsumer.poll(100).records(ktable));
        assertEquals(5, remaining.size());

        committedOffsets.put(new TopicPartition(ktable.topic(), ktable.partition()), new OffsetAndMetadata(10L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset has not reached, yet.
        remaining = task.update(ktable, remaining);
        assertEquals(5, remaining.size());

        committedOffsets.put(new TopicPartition(ktable.topic(), ktable.partition()), new OffsetAndMetadata(11L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // one record should be processed.
        remaining = task.update(ktable, remaining);
        assertEquals(4, remaining.size());

        committedOffsets.put(new TopicPartition(ktable.topic(), ktable.partition()), new OffsetAndMetadata(45L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset is now 45. All record except for the last one should be processed.
        remaining = task.update(ktable, remaining);
        assertEquals(1, remaining.size());

        committedOffsets.put(new TopicPartition(ktable.topic(), ktable.partition()), new OffsetAndMetadata(50L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset is now 50. Still the last record remains.
        remaining = task.update(ktable, remaining);
        assertEquals(1, remaining.size());

        committedOffsets.put(new TopicPartition(ktable.topic(), ktable.partition()), new OffsetAndMetadata(60L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset is now 60. No record should be left.
        remaining = task.update(ktable, remaining);
        assertNull(remaining);

        task.closeStateManager();

        File taskDir = stateDirectory.directoryForTask(taskId);
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        Map<TopicPartition, Long> offsets = checkpoint.read();

        assertEquals(1, offsets.size());
        assertEquals(new Long(51L), offsets.get(ktable));

    }

    private List<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }
}
