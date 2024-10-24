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

package org.apache.kafka.jmh.fetcher;

import kafka.cluster.AlterPartitionListener;
import kafka.cluster.DelayedOperations;
import kafka.cluster.Partition;
import kafka.log.LogManager;
import kafka.server.AlterPartitionManager;
import kafka.server.BrokerBlockingSender;
import kafka.server.FailedPartitions;
import kafka.server.InitialFetchState;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.OffsetTruncationState;
import kafka.server.QuotaFactory;
import kafka.server.RemoteLeaderEndPoint;
import kafka.server.ReplicaFetcherThread;
import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;
import kafka.server.builders.LogManagerBuilder;
import kafka.server.builders.ReplicaManagerBuilder;
import kafka.server.metadata.MockConfigRepository;
import kafka.server.metadata.ZkMetadataCache;
import kafka.utils.Pool;
import kafka.utils.TestUtils;
import kafka.zk.KafkaZkClient;

import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.RecordsSend;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.BrokerFeatures;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.network.BrokerEndPoint;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.mockito.Mockito;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.collection.Iterator;
import scala.collection.Map;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReplicaFetcherThreadBenchmark {
    private final File logDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
    private final KafkaScheduler scheduler = new KafkaScheduler(1, true, "scheduler");
    private final Pool<TopicPartition, Partition> pool = new Pool<>(Option.empty());
    private final Metrics metrics = new Metrics();
    private final Option<Uuid> topicId = Option.apply(Uuid.randomUuid());
    @Param({"100", "500", "1000", "5000"})
    private int partitionCount;
    private ReplicaFetcherBenchThread fetcher;
    private LogManager logManager;
    private ReplicaManager replicaManager;
    private ReplicaQuota replicaQuota;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (!logDir.mkdir())
            throw new IOException("error creating test directory");

        scheduler.startup();
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:9999");
        KafkaConfig config = new KafkaConfig(props);
        LogConfig logConfig = createLogConfig();

        BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
        LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);
        List<File> logDirs = Collections.singletonList(logDir);
        logManager = new LogManagerBuilder().
            setLogDirs(logDirs).
            setInitialOfflineDirs(Collections.emptyList()).
            setConfigRepository(new MockConfigRepository()).
            setInitialDefaultConfig(logConfig).
            setCleanerConfig(new CleanerConfig(0, 0, 0, 0, 0, 0.0, 0, false)).
            setRecoveryThreadsPerDataDir(1).
            setFlushCheckMs(1000L).
            setFlushRecoveryOffsetCheckpointMs(10000L).
            setFlushStartOffsetCheckpointMs(10000L).
            setRetentionCheckMs(1000L).
            setProducerStateManagerConfig(60000, false).
            setInterBrokerProtocolVersion(MetadataVersion.latestTesting()).
            setScheduler(scheduler).
            setBrokerTopicStats(brokerTopicStats).
            setLogDirFailureChannel(logDirFailureChannel).
            setTime(Time.SYSTEM).
            setKeepPartitionMetadataFile(true).
            build();

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> initialFetched = new LinkedHashMap<>();
        HashMap<String, Uuid> topicIds = new HashMap<>();
        scala.collection.mutable.Map<TopicPartition, InitialFetchState> initialFetchStates = new scala.collection.mutable.HashMap<>();
        List<UpdateMetadataRequestData.UpdateMetadataPartitionState> updatePartitionState = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition tp = new TopicPartition("topic", i);

            List<Integer> replicas = Arrays.asList(0, 1, 2);
            LeaderAndIsrRequestData.LeaderAndIsrPartitionState partitionState = new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
                    .setControllerEpoch(0)
                    .setLeader(0)
                    .setLeaderEpoch(0)
                    .setIsr(replicas)
                    .setPartitionEpoch(1)
                    .setReplicas(replicas)
                    .setIsNew(true);

            AlterPartitionListener alterPartitionListener = Mockito.mock(AlterPartitionListener.class);
            OffsetCheckpoints offsetCheckpoints = Mockito.mock(OffsetCheckpoints.class);
            Mockito.when(offsetCheckpoints.fetch(logDir.getAbsolutePath(), tp)).thenReturn(Optional.of(0L));
            AlterPartitionManager isrChannelManager = Mockito.mock(AlterPartitionManager.class);
            Partition partition = new Partition(tp, 100, MetadataVersion.latestTesting(),
                    0, () -> -1, Time.SYSTEM, alterPartitionListener, new DelayedOperationsMock(topicId, tp),
                    Mockito.mock(MetadataCache.class), logManager, isrChannelManager, topicId);

            partition.makeFollower(partitionState, offsetCheckpoints, topicId, Option.empty());
            pool.put(tp, partition);
            initialFetchStates.put(tp, new InitialFetchState(topicId, new BrokerEndPoint(3, "host", 3000), 0, 0));
            BaseRecords fetched = new BaseRecords() {
                @Override
                public int sizeInBytes() {
                    return 0;
                }

                @Override
                public RecordsSend<? extends BaseRecords> toSend() {
                    return null;
                }
            };
            initialFetched.put(new TopicIdPartition(topicId.get(), tp), new FetchResponseData.PartitionData()
                    .setPartitionIndex(tp.partition())
                    .setLastStableOffset(0)
                    .setLogStartOffset(0)
                    .setRecords(fetched));

            updatePartitionState.add(
                    new UpdateMetadataRequestData.UpdateMetadataPartitionState()
                            .setTopicName("topic")
                            .setPartitionIndex(i)
                            .setControllerEpoch(0)
                            .setLeader(0)
                            .setLeaderEpoch(0)
                            .setIsr(replicas)
                            .setZkVersion(1)
                            .setReplicas(replicas));
        }
        UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion(),
                0, 0, 0, updatePartitionState, Collections.emptyList(), topicIds).build();

        // TODO: fix to support raft
        ZkMetadataCache metadataCache = MetadataCache.zkMetadataCache(0,
            config.interBrokerProtocolVersion(), BrokerFeatures.createEmpty(), false);
        metadataCache.updateMetadata(0, updateMetadataRequest);

        replicaManager = new ReplicaManagerBuilder().
            setConfig(config).
            setMetrics(metrics).
            setTime(new MockTime()).
            setZkClient(Mockito.mock(KafkaZkClient.class)).
            setScheduler(scheduler).
            setLogManager(logManager).
            setQuotaManagers(Mockito.mock(QuotaFactory.QuotaManagers.class)).
            setBrokerTopicStats(brokerTopicStats).
            setMetadataCache(metadataCache).
            setLogDirFailureChannel(new LogDirFailureChannel(logDirs.size())).
            setAlterPartitionManager(TestUtils.createAlterIsrManager()).
            build();
        replicaQuota = new ReplicaQuota() {
            @Override
            public boolean isQuotaExceeded() {
                return false;
            }

            @Override
            public void record(long value) {
            }

            @Override
            public boolean isThrottled(TopicPartition topicPartition) {
                return false;
            }
        };
        fetcher = new ReplicaFetcherBenchThread(config, replicaManager, replicaQuota, pool);
        fetcher.addPartitions(initialFetchStates);
        // force a pass to move partitions to fetching state. We do this in the setup phase
        // so that we do not measure this time as part of the steady state work
        fetcher.doWork();
        // handle response to engage the incremental fetch session handler
        ((RemoteLeaderEndPoint) fetcher.leader()).fetchSessionHandler().handleResponse(FetchResponse.of(Errors.NONE, 0, 999, initialFetched), ApiKeys.FETCH.latestVersion());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException, InterruptedException {
        metrics.close();
        replicaManager.shutdown(false);
        logManager.shutdown(-1L);
        scheduler.shutdown();
        Utils.delete(logDir);
    }

    @Benchmark
    public long testFetcher() {
        fetcher.doWork();
        return fetcher.fetcherStats().requestRate().count();
    }

    // avoid mocked DelayedOperations to avoid mocked class affecting benchmark results
    private static class DelayedOperationsMock extends DelayedOperations {
        DelayedOperationsMock(Option<Uuid>  topicId, TopicPartition topicPartition) {
            super(topicId, topicPartition, null, null, null, null);
        }

        @Override
        public int numDelayedDelete() {
            return 0;
        }
    }

    private static LogConfig createLogConfig() {
        return new LogConfig(new Properties());
    }


    static class ReplicaFetcherBenchThread extends ReplicaFetcherThread {
        private final Pool<TopicPartition, Partition> pool;

        ReplicaFetcherBenchThread(KafkaConfig config,
                                  ReplicaManager replicaManager,
                                  ReplicaQuota replicaQuota,
                                  Pool<TopicPartition,
                                  Partition> partitions) {
            super("name",
                    new RemoteLeaderEndPoint(
                            String.format("[ReplicaFetcher replicaId=%d, leaderId=%d, fetcherId=%d", config.brokerId(), 3, 3),
                            new BrokerBlockingSender(
                                    new BrokerEndPoint(3, "host", 3000),
                                    config,
                                    new Metrics(),
                                    Time.SYSTEM,
                                    3,
                                    String.format("broker-%d-fetcher-%d", 3, 3),
                                    new LogContext(String.format("[ReplicaFetcher replicaId=%d, leaderId=%d, fetcherId=%d", config.brokerId(), 3, 3))
                            ),
                            new FetchSessionHandler(
                                    new LogContext(String.format("[ReplicaFetcher replicaId=%d, leaderId=%d, fetcherId=%d", config.brokerId(), 3, 3)), 3),
                            config,
                            replicaManager,
                            replicaQuota,
                            config::interBrokerProtocolVersion,
                            () -> -1
                    ) {
                        @Override
                        public OffsetAndEpoch fetchEarliestOffset(TopicPartition topicPartition, int currentLeaderEpoch) {
                            return new OffsetAndEpoch(0L, 0);
                        }

                        @Override
                        public Map<TopicPartition, EpochEndOffset> fetchEpochEndOffsets(Map<TopicPartition, OffsetForLeaderPartition> partitions) {
                            scala.collection.mutable.Map<TopicPartition, EpochEndOffset> endOffsets = new scala.collection.mutable.HashMap<>();
                            Iterator<TopicPartition> iterator = partitions.keys().iterator();
                            while (iterator.hasNext()) {
                                TopicPartition tp = iterator.next();
                                endOffsets.put(tp, new EpochEndOffset()
                                        .setPartition(tp.partition())
                                        .setErrorCode(Errors.NONE.code())
                                        .setLeaderEpoch(0)
                                        .setEndOffset(100));
                            }
                            return endOffsets;
                        }

                        @Override
                        public Map<TopicPartition, FetchResponseData.PartitionData> fetch(FetchRequest.Builder fetchRequest) {
                            return new scala.collection.mutable.HashMap<>();
                        }
                    },
                    config,
                    new FailedPartitions(),
                    replicaManager,
                    replicaQuota,
                    String.format("[ReplicaFetcher replicaId=%d, leaderId=%d, fetcherId=%d", config.brokerId(), 3, 3),
                    config::interBrokerProtocolVersion
            );

            pool = partitions;
        }

        @Override
        public Option<Object> latestEpoch(TopicPartition topicPartition) {
            return Option.apply(0);
        }

        @Override
        public long logStartOffset(TopicPartition topicPartition) {
            return pool.get(topicPartition).localLogOrException().logStartOffset();
        }

        @Override
        public long logEndOffset(TopicPartition topicPartition) {
            return 0;
        }

        @Override
        public void truncate(TopicPartition tp, OffsetTruncationState offsetTruncationState) {
            // pretend to truncate to move to Fetching state
        }

        @Override
        public Option<OffsetAndEpoch> endOffsetForEpoch(TopicPartition topicPartition, int epoch) {
            return Option.apply(new OffsetAndEpoch(0, 0));
        }

        @Override
        public Option<LogAppendInfo> processPartitionData(TopicPartition topicPartition, long fetchOffset,
                                                          FetchResponseData.PartitionData partitionData) {
            return Option.empty();
        }
    }
}
