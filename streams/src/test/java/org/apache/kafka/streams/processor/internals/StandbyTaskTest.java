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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilderTest;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.WindowKeySchema;
import org.apache.kafka.test.MockRestoreConsumer;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockStateStore;
import org.apache.kafka.test.MockStoreBuilder;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkList;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
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
        mkList(new MockStoreBuilder(storeName1, false).build(), new MockStoreBuilder(storeName2, true).build()),
        mkMap(
            mkEntry(storeName1, storeChangelogTopicName1),
            mkEntry(storeName2, storeChangelogTopicName2)
        )
    );
    private final TopicPartition globalTopicPartition = new TopicPartition(globalStoreName, 0);
    private final Set<TopicPartition> ktablePartitions = Utils.mkSet(globalTopicPartition);
    private final ProcessorTopology ktableTopology = ProcessorTopology.withLocalStores(
        singletonList(new MockStoreBuilder(globalTopicPartition.topic(), true).withLoggingDisabled().build()),
        mkMap(
            mkEntry(globalStoreName, globalTopicPartition.topic())
        )
    );

    private File baseDir;
    private StateDirectory stateDirectory;

    private StreamsConfig createConfig(final File baseDir) throws IOException {
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath()),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName())
        )));
    }

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockRestoreConsumer<Integer, Integer> restoreStateConsumer = new MockRestoreConsumer<>(new IntegerSerializer(), new IntegerSerializer());
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(
        restoreStateConsumer,
        Duration.ZERO,
        stateRestoreListener,
        new LogContext("standby-task-test ")
    );

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    @Before
    public void setup() throws Exception {
        restoreStateConsumer.reset();
        restoreStateConsumer.updatePartitions(storeChangelogTopicName1, mkList(
            new PartitionInfo(storeChangelogTopicName1, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName1, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName1, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        restoreStateConsumer.updatePartitions(storeChangelogTopicName2, mkList(
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
        final StreamsConfig config = createConfig(baseDir);
        final StandbyTask task = new StandbyTask(taskId, topicPartitions, topology, consumer, changelogReader, config, null, stateDirectory);
        task.initializeStateStores();
        assertEquals(Utils.mkSet(partition2, partition1), new HashSet<>(task.checkpointedOffsets().keySet()));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = ProcessorStateException.class)
    public void testUpdateNonPersistentStore() throws IOException {
        final StreamsConfig config = createConfig(baseDir);
        final StandbyTask task = new StandbyTask(taskId, topicPartitions, topology, consumer, changelogReader, config, null, stateDirectory);

        restoreStateConsumer.assign(new ArrayList<>(task.checkpointedOffsets().keySet()));

        task.update(partition1,
            singletonList(new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue))
        );

    }

    @Test
    public void testUpdate() throws IOException {
        final StreamsConfig config = createConfig(baseDir);
        final StandbyTask task = new StandbyTask(taskId, topicPartitions, topology, consumer, changelogReader, config, null, stateDirectory);
        task.initializeStateStores();
        final Set<TopicPartition> partition = Collections.singleton(partition2);
        restoreStateConsumer.assign(partition);

        for (final ConsumerRecord<Integer, Integer> record : Arrays.asList(
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1, 100),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2, 100),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 3, 100))) {
            restoreStateConsumer.bufferRecord(record);
        }

        restoreStateConsumer.seekToBeginning(partition);
        task.update(partition2, restoreStateConsumer.poll(Duration.ofMillis(100)).records(partition2));

        final StandbyContextImpl context = (StandbyContextImpl) task.context();
        final MockStateStore store1 = (MockStateStore) context.getStateMgr().getStore(storeName1);
        final MockStateStore store2 = (MockStateStore) context.getStateMgr().getStore(storeName2);

        assertEquals(Collections.emptyList(), store1.keys);
        assertEquals(mkList(1, 2, 3), store2.keys);
    }

    @Test
    public void shouldRestoreToWindowedStores() throws IOException {
        final String storeName = "windowed-store";
        final String changelogName = applicationId + "-" + storeName + "-changelog";

        final TopicPartition topicPartition = new TopicPartition(changelogName, 1);

        final List<TopicPartition> partitions = mkList(topicPartition);

        consumer.assign(partitions);

        final InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder().setApplicationId(applicationId);

        new InternalStreamsBuilder(internalTopologyBuilder)
            .stream(Collections.singleton("topic"), new ConsumedInternal<>())
            .groupByKey()
            .windowedBy(TimeWindows.of(60_000).until(120_000))
            .count(Materialized.as(storeName));

        final StandbyTask task = new StandbyTask(
            taskId,
            partitions,
            internalTopologyBuilder.build(0),
            consumer,
            new StoreChangelogReader(
                restoreStateConsumer,
                Duration.ZERO,
                stateRestoreListener,
                new LogContext("standby-task-test ")
            ),
            createConfig(baseDir),
            new MockStreamsMetrics(new Metrics()),
            stateDirectory
        );

        task.initializeStateStores();

        consumer.commitSync(mkMap(mkEntry(topicPartition, new OffsetAndMetadata(35L))));
        task.commit();

        final List<ConsumerRecord<byte[], byte[]>> remaining1 = task.update(
            topicPartition,
            Arrays.asList(
                makeWindowedConsumerRecord(changelogName, 10, 1, 0L, 60_000L),
                makeWindowedConsumerRecord(changelogName, 20, 2, 60_000L, 120_000),
                makeWindowedConsumerRecord(changelogName, 30, 3, 120_000L, 180_000),
                makeWindowedConsumerRecord(changelogName, 40, 4, 180_000L, 240_000)
            )
        );

        assertEquals(
            Arrays.asList(
                new KeyValue<>(new Windowed<>(1, new TimeWindow(0, 60_000)), 100L),
                new KeyValue<>(new Windowed<>(2, new TimeWindow(60_000, 120_000)), 100L),
                new KeyValue<>(new Windowed<>(3, new TimeWindow(120_000, 180_000)), 100L)
            ),
            getWindowedStoreContents(storeName, task)
        );

        consumer.commitSync(mkMap(mkEntry(topicPartition, new OffsetAndMetadata(45L))));
        task.commit();

        final List<ConsumerRecord<byte[], byte[]>> remaining2 = task.update(topicPartition, remaining1);
        assertEquals(emptyList(), remaining2);

        // the first record's window should have expired.
        assertEquals(
            Arrays.asList(
                new KeyValue<>(new Windowed<>(2, new TimeWindow(60_000, 120_000)), 100L),
                new KeyValue<>(new Windowed<>(3, new TimeWindow(120_000, 180_000)), 100L),
                new KeyValue<>(new Windowed<>(4, new TimeWindow(180_000, 240_000)), 100L)
            ),
            getWindowedStoreContents(storeName, task)
        );
    }

    private ConsumerRecord<byte[], byte[]> makeWindowedConsumerRecord(final String changelogName,
                                                                      final int offset,
                                                                      final int key,
                                                                      final long start,
                                                                      final long end) {
        final Windowed<Integer> data = new Windowed<>(key, new TimeWindow(start, end));
        final Bytes wrap = Bytes.wrap(new IntegerSerializer().serialize(null, data.key()));
        final byte[] keyBytes = WindowKeySchema.toStoreKeyBinary(new Windowed<>(wrap, data.window()), 1).get();
        return new ConsumerRecord<>(
            changelogName,
            1,
            offset,
            start,
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            keyBytes,
            new LongSerializer().serialize(null, 100L)
        );
    }

    @Test
    public void shouldWriteCheckpointFile() throws IOException {
        final String storeName = "checkpoint-file-store";
        final String changelogName = applicationId + "-" + storeName + "-changelog";

        final TopicPartition topicPartition = new TopicPartition(changelogName, 1);
        final List<TopicPartition> partitions = mkList(topicPartition);

        final InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder().setApplicationId(applicationId);

        new InternalStreamsBuilder(internalTopologyBuilder)
            .stream(Collections.singleton("topic"), new ConsumedInternal<>())
            .groupByKey()
            .count(Materialized.as(storeName));

        consumer.assign(partitions);

        final StandbyTask task = new StandbyTask(
            taskId,
            partitions,
            internalTopologyBuilder.build(0),
            consumer,
            changelogReader,
            createConfig(baseDir),
            new MockStreamsMetrics(new Metrics()),
            stateDirectory
        );
        task.initializeStateStores();

        consumer.commitSync(mkMap(mkEntry(topicPartition, new OffsetAndMetadata(20L))));
        task.commit();

        task.update(
            topicPartition,
            singletonList(makeWindowedConsumerRecord(changelogName, 10, 1, 0L, 60_000L))
        );

        task.closeStateManager(true);

        final File taskDir = stateDirectory.directoryForTask(taskId);
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        final Map<TopicPartition, Long> offsets = checkpoint.read();

        assertEquals(1, offsets.size());
        assertEquals(new Long(11L), offsets.get(topicPartition));
    }

    @SuppressWarnings("unchecked")
    private List<KeyValue<Windowed<Integer>, Long>> getWindowedStoreContents(final String storeName, final StandbyTask task) {
        final StandbyContextImpl context = (StandbyContextImpl) task.context();

        final List<KeyValue<Windowed<Integer>, Long>> result = new ArrayList<>();

        try (final KeyValueIterator<Windowed<byte[]>, Long> iterator =
                 ((WindowStore) context.getStateMgr().getStore(storeName)).all()) {

            while (iterator.hasNext()) {
                final KeyValue<Windowed<byte[]>, Long> next = iterator.next();
                final Integer deserializedKey = new IntegerDeserializer().deserialize(null, next.key.key());
                result.add(new KeyValue<>(new Windowed<>(deserializedKey, next.key.window()), next.value));
            }
        }

        return result;
    }

    @Test
    public void shouldRestoreToKTable() throws IOException {
        consumer.assign(mkList(globalTopicPartition));
        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

        final StandbyTask task = new StandbyTask(
            taskId,
            ktablePartitions,
            ktableTopology,
            consumer,
            changelogReader,
            createConfig(baseDir),
            null,
            stateDirectory
        );
        task.initializeStateStores();

        // The commit offset is at 0L. Records should not be processed
        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(
            globalTopicPartition,
            Arrays.asList(
                makeConsumerRecord(globalTopicPartition, 10, 1),
                makeConsumerRecord(globalTopicPartition, 20, 2),
                makeConsumerRecord(globalTopicPartition, 30, 3),
                makeConsumerRecord(globalTopicPartition, 40, 4),
                makeConsumerRecord(globalTopicPartition, 50, 5)
            )
        );
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(10L))));
        task.commit(); // update offset limits

        // The commit offset has not reached, yet.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(11L))));
        task.commit(); // update offset limits

        // one record should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(4, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(45L))));
        task.commit(); // update offset limits

        // The commit offset is now 45. All record except for the last one should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(50L))));
        task.commit(); // update offset limits

        // The commit offset is now 50. Still the last record remains.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(60L))));
        task.commit(); // update offset limits

        // The commit offset is now 60. No record should be left.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(emptyList(), remaining);
    }

    private ConsumerRecord<byte[], byte[]> makeConsumerRecord(final TopicPartition topicPartition, final long offset, final int key) {
        final IntegerSerializer integerSerializer = new IntegerSerializer();
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            0L,
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            integerSerializer.serialize(null, key),
            integerSerializer.serialize(null, 100)
        );
    }

    @Test
    public void shouldInitializeStateStoreWithoutException() throws IOException {
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().count();

        initializeStandbyStores(builder);
    }

    @Test
    public void shouldInitializeWindowStoreWithoutException() throws IOException {
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().windowedBy(TimeWindows.of(100)).count();

        initializeStandbyStores(builder);
    }

    private void initializeStandbyStores(final InternalStreamsBuilder builder) throws IOException {
        final StreamsConfig config = createConfig(baseDir);
        final InternalTopologyBuilder internalTopologyBuilder = InternalStreamsBuilderTest.internalTopologyBuilder(builder);
        final ProcessorTopology topology = internalTopologyBuilder.setApplicationId(applicationId).build(0);

        final StandbyTask standbyTask = new StandbyTask(
            taskId,
            emptySet(),
            topology,
            consumer,
            changelogReader,
            config,
            new MockStreamsMetrics(new Metrics()),
            stateDirectory
        );

        standbyTask.initializeStateStores();

        assertTrue(standbyTask.hasStateStores());
    }

    @Test
    public void shouldCheckpointStoreOffsetsOnCommit() throws IOException {
        consumer.assign(mkList(globalTopicPartition));
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(100L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(
            globalStoreName,
            mkList(new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0]))
        );

        final TaskId taskId = new TaskId(0, 0);
        final MockTime time = new MockTime();
        final StreamsConfig config = createConfig(baseDir);
        final StandbyTask task = new StandbyTask(
            taskId,
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
        task.update(
            globalTopicPartition,
            singletonList(
                new ConsumerRecord<>(globalTopicPartition.topic(), globalTopicPartition.partition(), 50L, serializedValue, serializedValue)
            )
        );

        time.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
        task.commit();

        final Map<TopicPartition, Long> checkpoint = new OffsetCheckpoint(
            new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME)
        ).read();
        assertThat(checkpoint, equalTo(Collections.singletonMap(globalTopicPartition, 51L)));

    }

    @Test
    public void shouldCloseStateMangerOnTaskCloseWhenCommitFailed() throws Exception {
        consumer.assign(mkList(globalTopicPartition));
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()), new OffsetAndMetadata(100L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(
            globalStoreName,
            mkList(new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0]))
        );

        final StreamsConfig config = createConfig(baseDir);
        final AtomicBoolean closedStateManager = new AtomicBoolean(false);
        final StandbyTask task = new StandbyTask(
            taskId,
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
        } catch (final Exception e) {
            // expected
        }
        assertTrue(closedStateManager.get());
    }

}
