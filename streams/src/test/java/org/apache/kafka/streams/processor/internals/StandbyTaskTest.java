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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilderTest;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockRestoreConsumer;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockStateStore;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.ConsumerUtils.poll;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StandbyTaskTest {

    private final TaskId taskId = new TaskId(0, 1);

    private final Serializer<Integer> intSerializer = new IntegerSerializer();

    private final String applicationId = "test-application";
    private final String storeName1 = "store1";
    private final String storeName2 = "store2";
    private final String storeChangelogTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
    private final String storeChangelogTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
    private final String globalStoreName = "ktable1";

    private final TopicPartition partition1 = new TopicPartition(storeChangelogTopicName1, 1);
    private final TopicPartition partition2 = new TopicPartition(storeChangelogTopicName2, 1);
    private final MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();

    private final Set<TopicPartition> topicPartitions = Collections.emptySet();
    private final ProcessorTopology topology = ProcessorTopology.withLocalStores(
            Utils.mkList(new MockStateStoreSupplier(storeName1, false).get(), new MockStateStoreSupplier(storeName2, true).get()),
            new HashMap<String, String>() {
                {
                    put(storeName1, storeChangelogTopicName1);
                    put(storeName2, storeChangelogTopicName2);
                }
            });
    private final TopicPartition globalTopicPartition = new TopicPartition(globalStoreName, 0);
    private final Set<TopicPartition> ktablePartitions = Utils.mkSet(globalTopicPartition);
    private final ProcessorTopology ktableTopology = ProcessorTopology.withLocalStores(
            Collections.<StateStore>singletonList(new MockStateStoreSupplier(globalTopicPartition.topic(), true, false).get()),
            new HashMap<String, String>() {
                {
                    put(globalStoreName, globalTopicPartition.topic());
                }
            });

    private File baseDir;
    private StateDirectory stateDirectory;

    private StreamsConfig createConfig(final File baseDir) throws IOException {
        return new StreamsConfig(new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
                setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
            }
        });
    }

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockRestoreConsumer restoreStateConsumer = new MockRestoreConsumer();
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, stateRestoreListener, new LogContext("standby-task-test "));

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    @Before
    public void setup() throws Exception {
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
        stateDirectory = new StateDirectory(createConfig(baseDir), new MockTime());
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(baseDir);
    }

    @Test
    public void testStorePartitions() throws IOException {
        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, topicPartitions, topology, consumer, changelogReader, config, null, stateDirectory);
        task.initializeStateStores();
        assertEquals(Utils.mkSet(partition2, partition1), new HashSet<>(task.checkpointedOffsets().keySet()));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = ProcessorStateException.class)
    public void testUpdateNonPersistentStore() throws IOException {
        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, topicPartitions, topology, consumer, changelogReader, config, null, stateDirectory);

        restoreStateConsumer.assign(new ArrayList<>(task.checkpointedOffsets().keySet()));

        task.update(partition1,
                records(new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue))
        );

    }

    @Test
    public void testUpdate() throws IOException {
        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, topicPartitions, topology, consumer, changelogReader, config, null, stateDirectory);
        task.initializeStateStores();
        final Set<TopicPartition> partition = Collections.singleton(partition2);
        restoreStateConsumer.assign(partition);

        for (ConsumerRecord<Integer, Integer> record : Arrays.asList(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1, 100),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2, 100),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 3, 100))) {
            restoreStateConsumer.bufferRecord(record);
        }

        restoreStateConsumer.seekToBeginning(partition);
        task.update(partition2, poll(restoreStateConsumer, 100).records(partition2));

        StandbyContextImpl context = (StandbyContextImpl) task.context();
        MockStateStore store1 = (MockStateStore) context.getStateMgr().getStore(storeName1);
        MockStateStore store2 = (MockStateStore) context.getStateMgr().getStore(storeName2);

        assertEquals(Collections.emptyList(), store1.keys);
        assertEquals(Utils.mkList(1, 2, 3), store2.keys);

        task.closeStateManager(true);

        File taskDir = stateDirectory.directoryForTask(taskId);
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        Map<TopicPartition, Long> offsets = checkpoint.read();

        assertEquals(1, offsets.size());
        assertEquals(new Long(30L + 1L), offsets.get(partition2));
    }

    @Test
    public void testUpdateKTable() throws IOException {
        consumer.assign(Utils.mkList(globalTopicPartition));
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(0L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(globalStoreName, Utils.mkList(
                new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(globalStoreName, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(globalStoreName, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        StreamsConfig config = createConfig(baseDir);
        StandbyTask task = new StandbyTask(taskId, ktablePartitions, ktableTopology, consumer, changelogReader, config, null, stateDirectory);
        task.initializeStateStores();
        restoreStateConsumer.assign(new ArrayList<>(task.checkpointedOffsets().keySet()));

        for (ConsumerRecord<Integer, Integer> record : Arrays.asList(
                new ConsumerRecord<>(globalTopicPartition.topic(), globalTopicPartition.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1, 100),
                new ConsumerRecord<>(globalTopicPartition.topic(), globalTopicPartition.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2, 100),
                new ConsumerRecord<>(globalTopicPartition.topic(), globalTopicPartition.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 3, 100),
                new ConsumerRecord<>(globalTopicPartition.topic(), globalTopicPartition.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 4, 100),
                new ConsumerRecord<>(globalTopicPartition.topic(), globalTopicPartition.partition(), 50, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 5, 100))) {
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
        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(globalTopicPartition, poll(restoreStateConsumer, 100).records(globalTopicPartition));
        assertEquals(5, remaining.size());

        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(10L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset has not reached, yet.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(5, remaining.size());

        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(11L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // one record should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(4, remaining.size());

        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(45L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset is now 45. All record except for the last one should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(50L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset is now 50. Still the last record remains.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(60L));
        consumer.commitSync(committedOffsets);
        task.commit(); // update offset limits

        // The commit offset is now 60. No record should be left.
        remaining = task.update(globalTopicPartition, remaining);
        assertNull(remaining);

        task.closeStateManager(true);

        File taskDir = stateDirectory.directoryForTask(taskId);
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        Map<TopicPartition, Long> offsets = checkpoint.read();

        assertEquals(1, offsets.size());
        assertEquals(new Long(51L), offsets.get(globalTopicPartition));

    }

    @Test
    public void shouldNotThrowUnsupportedOperationExceptionWhenInitializingStateStores() throws IOException {
        final String changelogName = "test-application-my-store-changelog";
        final List<TopicPartition> partitions = Utils.mkList(new TopicPartition(changelogName, 0));
        consumer.assign(partitions);
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(changelogName, 0), new OffsetAndMetadata(0L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(changelogName, Utils.mkList(
                new PartitionInfo(changelogName, 0, Node.noNode(), new Node[0], new Node[0])));
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().count();

        final StreamsConfig config = createConfig(baseDir);
        final InternalTopologyBuilder internalTopologyBuilder = InternalStreamsBuilderTest.internalTopologyBuilder(builder);
        final ProcessorTopology topology = internalTopologyBuilder.setApplicationId(applicationId).build(0);

        new StandbyTask(taskId, partitions, topology, consumer, changelogReader, config,
            new MockStreamsMetrics(new Metrics()), stateDirectory);
    }

    @Test
    public void shouldCheckpointStoreOffsetsOnCommit() throws IOException {
        consumer.assign(Utils.mkList(globalTopicPartition));
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(100L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(globalStoreName, Utils.mkList(
                new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0])));

        final TaskId taskId = new TaskId(0, 0);
        final MockTime time = new MockTime();
        final StreamsConfig config = createConfig(baseDir);
        final StandbyTask task = new StandbyTask(taskId,
                                                 ktablePartitions,
                                                 ktableTopology,
                                                 consumer,
                                                 changelogReader,
                                                 config,
                                                 null,
                                                 stateDirectory
        );
        task.initializeStateStores();

        restoreStateConsumer.assign(new ArrayList<>(task.checkpointedOffsets().keySet()));

        final byte[] serializedValue = Serdes.Integer().serializer().serialize("", 1);
        task.update(globalTopicPartition, Collections.singletonList(new ConsumerRecord<>(globalTopicPartition.topic(),
                                                                           globalTopicPartition.partition(),
                                                                           50L,
                                                                           serializedValue,
                                                                           serializedValue)));

        time.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
        task.commit();

        final Map<TopicPartition, Long> checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId),
                                                                                   ProcessorStateManager.CHECKPOINT_FILE_NAME)).read();
        assertThat(checkpoint, equalTo(Collections.singletonMap(globalTopicPartition, 51L)));

    }

    @Test
    public void shouldCloseStateMangerOnTaskCloseWhenCommitFailed() throws Exception {
        consumer.assign(Utils.mkList(globalTopicPartition));
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(100L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(globalStoreName, Utils.mkList(
                new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0])));

        final StreamsConfig config = createConfig(baseDir);
        final AtomicBoolean closedStateManager = new AtomicBoolean(false);
        final StandbyTask task = new StandbyTask(taskId,
                                                 ktablePartitions,
                                                 ktableTopology,
                                                 consumer,
                                                 changelogReader,
                                                 config,
                                                 null,
                                                 stateDirectory
        ) {
            @Override
            public void commit() {
                throw new RuntimeException("KABOOM!");
            }

            @Override
            void closeStateManager(final boolean writeCheckpoint) throws ProcessorStateException {
                closedStateManager.set(true);
            }
        };
        task.initializeStateStores();
        try {
            task.close(true, false);
            fail("should have thrown exception");
        } catch (Exception e) {
            // expected
        }
        assertTrue(closedStateManager.get());
    }

    private List<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }
}
