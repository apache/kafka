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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.OffsetCheckpoint;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class StandbyTaskTest {

    private final TaskId taskId = new TaskId(0, 1);

    private final Serializer<Integer> intSerializer = new IntegerSerializer();

    private final TopicPartition partition1 = new TopicPartition("store1", 1);
    private final TopicPartition partition2 = new TopicPartition("store2", 1);

    private final ProcessorTopology topology = new ProcessorTopology(
            Collections.<ProcessorNode>emptyList(),
            Collections.<String, SourceNode>emptyMap(),
            Utils.<StateStoreSupplier>mkList(
                    new MockStateStoreSupplier(partition1.topic(), false),
                    new MockStateStoreSupplier(partition2.topic(), true)
            )
    );


    private StreamingConfig createConfig(final File baseDir) throws Exception {
        return new StreamingConfig(new Properties() {
            {
                setProperty(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                setProperty(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                setProperty(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                setProperty(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                setProperty(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.test.MockTimestampExtractor");
                setProperty(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamingConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
            }
        });
    }

    private final ProcessorStateManagerTest.MockRestoreConsumer restoreStateConsumer = new ProcessorStateManagerTest.MockRestoreConsumer();

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    @Before
    public void setup() {
        restoreStateConsumer.reset();
        restoreStateConsumer.updatePartitions("store1", Utils.mkList(
                new PartitionInfo("store1", 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo("store1", 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo("store1", 2, Node.noNode(), new Node[0], new Node[0])
        ));

        restoreStateConsumer.updatePartitions("store2", Utils.mkList(
                new PartitionInfo("store2", 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo("store2", 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo("store2", 2, Node.noNode(), new Node[0], new Node[0])
        ));
    }

    @Test
    public void testStorePartitions() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            StreamingConfig config = createConfig(baseDir);
            StandbyTask task = new StandbyTask(taskId, restoreStateConsumer, topology, config, null);

            assertEquals(Utils.mkSet(partition2), new HashSet<>(task.changeLogPartitions()));

        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test(expected = Exception.class)
    public void testUpdateNonPersistentStore() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            StreamingConfig config = createConfig(baseDir);
            StandbyTask task = new StandbyTask(taskId, restoreStateConsumer, topology, config, null);

            restoreStateConsumer.assign(new ArrayList<>(task.changeLogPartitions()));

            task.update(partition1,
                    records(new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, recordKey, recordValue))
            );

        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdate() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            StreamingConfig config = createConfig(baseDir);
            StandbyTask task = new StandbyTask(taskId, restoreStateConsumer, topology, config, null);

            restoreStateConsumer.assign(new ArrayList<>(task.changeLogPartitions()));

            for (ConsumerRecord<Integer, Integer> record : Arrays.asList(
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 10, 1, 100),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 20, 2, 100),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 30, 3, 100))) {
                restoreStateConsumer.bufferRecord(record);
            }

            for (Map.Entry<TopicPartition, Long> entry : task.checkpointedOffsets().entrySet()) {
                TopicPartition partition = entry.getKey();
                long offset = entry.getValue();
                if (offset >= 0) {
                    restoreStateConsumer.seek(partition, offset);
                } else {
                    restoreStateConsumer.seekToBeginning(partition);
                }
            }

            task.update(partition2, restoreStateConsumer.poll(100).records(partition2));

            StandbyContextImpl context = (StandbyContextImpl) task.context();
            MockStateStoreSupplier.MockStateStore store1 =
                    (MockStateStoreSupplier.MockStateStore) context.getStateMgr().getStore(partition1.topic());
            MockStateStoreSupplier.MockStateStore store2 =
                    (MockStateStoreSupplier.MockStateStore) context.getStateMgr().getStore(partition2.topic());

            assertEquals(Collections.emptyList(), store1.keys);
            assertEquals(Utils.mkList(1, 2, 3), store2.keys);

            task.close();

            File taskDir = new File(baseDir, taskId.toString());
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
            Map<TopicPartition, Long> offsets = checkpoint.read();

            assertEquals(1, offsets.size());
            assertEquals(new Long(30L + 1L), offsets.get(partition2));

        } finally {
            Utils.delete(baseDir);
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }
}
