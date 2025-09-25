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
package org.apache.kafka.jmh.coordinator;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoaderImpl;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.common.runtime.SnapshottableCoordinator;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordSerde;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GroupCoordinatorShardLoadingBenchmark {

    private static final String GROUP_ID = "test-group";

    @Param({"1", "4", "16", "64", "256", "1024", "4096", "16384", "65536", "262144", "1048576"})
    private long commitInterval;

    @Param({"8192"})
    private int batchCount;

    @Param({"2048"})
    private int batchSize;

    private TopicPartition topicPartition;
    private MockTime time;
    private GroupCoordinatorConfig config;
    private GroupCoordinatorRecordSerde serde;
    private GroupCoordinatorShard coordinatorShard;
    private SnapshottableCoordinator<GroupCoordinatorShard, CoordinatorRecord> snapshottableCoordinator;
    private UnifiedLog offsetCommitLog;
    private UnifiedLog transactionalOffsetCommitLog;

    static class OffsetCommitLog extends MockLog {
        private final int batchCount;
        private final SimpleRecord[] batch;

        public OffsetCommitLog(TopicPartition tp, int batchSize, int batchCount) throws IOException {
            super(tp);

            this.batchCount = batchCount;

            List<SimpleRecord> batchRecords = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                String topic = "topic-" + i;
                int partition = 0;

                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
                    0L,
                    OptionalInt.of(0),
                    OffsetAndMetadata.NO_METADATA,
                    0L,
                    OptionalLong.empty(),
                    Uuid.randomUuid()
                );

                CoordinatorRecord coordinatorRecord = GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                    GROUP_ID, topic, partition, offsetAndMetadata
                );

                byte[] keyBytes = new GroupCoordinatorRecordSerde().serializeKey(coordinatorRecord);
                byte[] valueBytes = new GroupCoordinatorRecordSerde().serializeValue(coordinatorRecord);
                SimpleRecord simpleRecord = new SimpleRecord(keyBytes, valueBytes);
                batchRecords.add(simpleRecord);
            }

            this.batch = batchRecords.toArray(new SimpleRecord[0]);
        }

        @Override
        public long logStartOffset() {
            return 0L;
        }

        @Override
        public long logEndOffset() {
            if (batch == null) {
                return 0L;
            }

            return (long) batchCount * (long) batch.length;
        }

        @Override
        public FetchDataInfo read(long startOffset, int maxLength, FetchIsolation isolation, boolean minOneMessage) {
            if (startOffset < 0 || startOffset >= logEndOffset()) {
                return new FetchDataInfo(new LogOffsetMetadata(startOffset), MemoryRecords.EMPTY);
            }

            MemoryRecords records = MemoryRecords.withRecords(
                startOffset,
                Compression.NONE,
                batch
            );
            return new FetchDataInfo(new LogOffsetMetadata(startOffset), records);
        }
    }

    static class TransactionalOffsetCommitLog extends MockLog {
        private final int batchCount;
        private final SimpleRecord[] batch;
        private final long producerId;
        private final short producerEpoch;
        private final int coordinatorEpoch;

        public TransactionalOffsetCommitLog(TopicPartition tp, int batchSize, int batchCount) throws IOException {
            super(tp);

            this.batchCount = batchCount;
            this.producerId = 1000L;
            this.producerEpoch = 0;
            this.coordinatorEpoch = 100;

            List<SimpleRecord> batchRecords = new ArrayList<>();
            for (int i = 0; i < batchSize - 1; i++) {
                String topic = "topic-" + i;
                int partition = 0;

                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
                    0L,
                    OptionalInt.of(0),
                    OffsetAndMetadata.NO_METADATA,
                    0L,
                    OptionalLong.empty(),
                    Uuid.randomUuid()
                );

                CoordinatorRecord coordinatorRecord = GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                    GROUP_ID, topic, partition, offsetAndMetadata
                );

                byte[] keyBytes = new GroupCoordinatorRecordSerde().serializeKey(coordinatorRecord);
                byte[] valueBytes = new GroupCoordinatorRecordSerde().serializeValue(coordinatorRecord);
                SimpleRecord simpleRecord = new SimpleRecord(keyBytes, valueBytes);
                batchRecords.add(simpleRecord);
            }

            this.batch = batchRecords.toArray(new SimpleRecord[0]);
        }

        @Override
        public long logStartOffset() {
            return 0L;
        }

        @Override
        public long logEndOffset() {
            if (batch == null) {
                return 0L;
            }

            return (long) (batch.length + 1) * (long) batchCount;
        }

        @Override
        public FetchDataInfo read(long startOffset, int maxLength, FetchIsolation isolation, boolean minOneMessage) {
            if (startOffset < 0 || startOffset >= logEndOffset()) {
                return new FetchDataInfo(new LogOffsetMetadata(startOffset), MemoryRecords.EMPTY);
            }

            // Repeat the batch followed by a commit marker.
            long patternLength = batch.length + 1;
            if (startOffset % patternLength < batch.length) {
                MemoryRecords records = MemoryRecords.withTransactionalRecords(
                    startOffset,
                    Compression.NONE,
                    producerId,
                    producerEpoch,
                    0,
                    0,
                    batch
                );
                return new FetchDataInfo(new LogOffsetMetadata(startOffset), records);
            } else {
                MemoryRecords records = MemoryRecords.withEndTransactionMarker(
                    startOffset,
                    0L,
                    0,
                    producerId,
                    producerEpoch,
                    new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
                );
                return new FetchDataInfo(new LogOffsetMetadata(startOffset), records);
            }
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        topicPartition = new TopicPartition("__consumer_offsets", 0);
        time = new MockTime();
        Map<String, Object> props = new HashMap<>();
        config = GroupCoordinatorConfig.fromProps(props);
        serde = new GroupCoordinatorRecordSerde();
    }

    @Setup(Level.Iteration)
    public void setupIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) throws IOException {
        // Reduce the data size for warmup iterations, since transactional offset commit loading
        // takes longer than 20 seconds.
        int iterationBatchCount = batchCount;
        if (iterationParams.getType() == IterationType.WARMUP) {
            iterationBatchCount = Math.min(iterationBatchCount, 1024);
        }

        offsetCommitLog = new OffsetCommitLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0), batchSize, iterationBatchCount);
        transactionalOffsetCommitLog = new TransactionalOffsetCommitLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0), batchSize, iterationBatchCount);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        GroupConfigManager configManager = new GroupConfigManager(new HashMap<>());
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(metricsRegistry, metrics);

        coordinatorShard = new GroupCoordinatorShard.Builder(config, configManager)
            .withAuthorizerPlugin(Optional.empty())
            .withLogContext(logContext)
            .withSnapshotRegistry(snapshotRegistry)
            .withTime(time)
            .withTimer(new MockCoordinatorTimer<>(time))
            .withExecutor(new MockCoordinatorExecutor<>())
            .withCoordinatorMetrics(coordinatorMetrics)
            .withTopicPartition(topicPartition)
            .build();

        snapshottableCoordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            coordinatorShard,
            topicPartition
        );
    }

    private CoordinatorLoader.LoadSummary loadRecords(UnifiedLog log) throws ExecutionException, InterruptedException {
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = tp -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = tp -> Optional.of(log.logEndOffset());

        CoordinatorLoaderImpl<CoordinatorRecord> loader = new CoordinatorLoaderImpl<>(
            time,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            config.offsetsLoadBufferSize(),
            commitInterval
        );

        return loader.load(topicPartition, snapshottableCoordinator).get();
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public CoordinatorLoader.LoadSummary loadOffsetCommitRecords() throws ExecutionException, InterruptedException {
        return loadRecords(offsetCommitLog);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public CoordinatorLoader.LoadSummary loadTransactionalOffsetCommitRecords() throws ExecutionException, InterruptedException {
        return loadRecords(transactionalOffsetCommitLog);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupCoordinatorShardLoadingBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
