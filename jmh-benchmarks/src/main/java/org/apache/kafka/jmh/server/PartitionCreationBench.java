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
package org.apache.kafka.jmh.server;

import kafka.cluster.Partition;
import kafka.log.LogManager;
import kafka.server.AlterPartitionManager;
import kafka.server.BrokerFeatures;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;
import kafka.server.builders.LogManagerBuilder;
import kafka.server.builders.ReplicaManagerBuilder;
import kafka.server.checkpoints.OffsetCheckpoints;
import kafka.server.metadata.ConfigRepository;
import kafka.server.metadata.MockConfigRepository;
import kafka.utils.TestUtils;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.Scheduler;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import scala.Option;
import scala.collection.JavaConverters;

@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
@State(value = Scope.Benchmark)
public class PartitionCreationBench {
    @Param({"false", "true"})
    public boolean useTopicIds;

    @Param({"2000"})
    public int numPartitions;

    private final String topicName = "foo";

    private Option<Uuid> topicId;
    private Scheduler scheduler;
    private Metrics metrics;
    private Time time;
    private KafkaConfig brokerProperties;

    private ReplicaManager replicaManager;
    private QuotaFactory.QuotaManagers quotaManagers;
    private KafkaZkClient zkClient;
    private LogDirFailureChannel failureChannel;
    private LogManager logManager;
    private AlterPartitionManager alterPartitionManager;
    private List<TopicPartition> topicPartitions;

    @SuppressWarnings("deprecation")
    @Setup(Level.Invocation)
    public void setup() {
        if (useTopicIds)
            topicId = Option.apply(Uuid.randomUuid());
        else
            topicId = Option.empty();

        this.scheduler = new KafkaScheduler(1, true, "scheduler-thread");
        this.brokerProperties = KafkaConfig.fromProps(TestUtils.createBrokerConfig(
                0, TestUtils.MockZkConnect(), true, true, 9092, Option.empty(), Option.empty(),
                Option.empty(), true, false, 0, false, 0, false, 0, Option.empty(), 1, true, 1,
                (short) 1, false));
        this.metrics = new Metrics();
        this.time = Time.SYSTEM;
        this.failureChannel = new LogDirFailureChannel(brokerProperties.logDirs().size());
        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(Optional.empty());
        final List<File> files =
                JavaConverters.seqAsJavaList(brokerProperties.logDirs()).stream().map(File::new).collect(Collectors.toList());
        CleanerConfig cleanerConfig = new CleanerConfig(1,
                4 * 1024 * 1024L, 0.9d,
                1024 * 1024, 32 * 1024 * 1024,
                Double.MAX_VALUE, 15 * 1000, true);

        ConfigRepository configRepository = new MockConfigRepository();
        this.logManager = new LogManagerBuilder().
            setLogDirs(files).
            setInitialOfflineDirs(Collections.emptyList()).
            setConfigRepository(configRepository).
            setInitialDefaultConfig(createLogConfig()).
            setCleanerConfig(cleanerConfig).
            setRecoveryThreadsPerDataDir(1).
            setFlushCheckMs(1000L).
            setFlushRecoveryOffsetCheckpointMs(10000L).
            setFlushStartOffsetCheckpointMs(10000L).
            setRetentionCheckMs(1000L).
            setProducerStateManagerConfig(60000, false).
            setInterBrokerProtocolVersion(MetadataVersion.latest()).
            setScheduler(scheduler).
            setBrokerTopicStats(brokerTopicStats).
            setLogDirFailureChannel(failureChannel).
            setTime(Time.SYSTEM).
            setKeepPartitionMetadataFile(true).
            build();
        scheduler.startup();
        this.quotaManagers = QuotaFactory.instantiate(this.brokerProperties, this.metrics, this.time, "");
        this.zkClient = new KafkaZkClient(null, false, Time.SYSTEM) {
            @Override
            public Properties getEntityConfigs(String rootEntityType, String sanitizedEntityName) {
                return new Properties();
            }
        };
        this.alterPartitionManager = TestUtils.createAlterIsrManager();
        this.replicaManager = new ReplicaManagerBuilder().
            setConfig(brokerProperties).
            setMetrics(metrics).
            setTime(time).
            setZkClient(zkClient).
            setScheduler(scheduler).
            setLogManager(logManager).
            setQuotaManagers(quotaManagers).
            setBrokerTopicStats(brokerTopicStats).
            setMetadataCache(MetadataCache.zkMetadataCache(this.brokerProperties.brokerId(),
                this.brokerProperties.interBrokerProtocolVersion(), BrokerFeatures.createEmpty(),
                null, false)).
            setLogDirFailureChannel(failureChannel).
            setAlterPartitionManager(alterPartitionManager).
            build();
        replicaManager.startup();
        replicaManager.checkpointHighWatermarks();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws Exception {
        this.replicaManager.shutdown(false);
        logManager.shutdown(-1L);
        this.metrics.close();
        this.scheduler.shutdown();
        this.quotaManagers.shutdown();
        for (File dir : JavaConverters.asJavaCollection(logManager.liveLogDirs())) {
            Utils.delete(dir);
        }
        this.zkClient.close();
    }

    private static LogConfig createLogConfig() {
        return new LogConfig(new Properties());
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void makeFollower() {
        topicPartitions = new ArrayList<>();
        for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
            topicPartitions.add(new TopicPartition(topicName, partitionNum));
        }

        List<Integer> replicas = new ArrayList<>();
        replicas.add(0);
        replicas.add(1);
        replicas.add(2);

        OffsetCheckpoints checkpoints = (logDir, topicPartition) -> Option.apply(0L);
        for (TopicPartition topicPartition : topicPartitions) {
            final Partition partition = this.replicaManager.createPartition(topicPartition);
            List<Integer> inSync = new ArrayList<>();
            inSync.add(0);
            inSync.add(1);
            inSync.add(2);

            LeaderAndIsrRequestData.LeaderAndIsrPartitionState partitionState = new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
                    .setControllerEpoch(0)
                    .setLeader(0)
                    .setLeaderEpoch(0)
                    .setIsr(inSync)
                    .setPartitionEpoch(1)
                    .setReplicas(replicas)
                    .setIsNew(true);

            partition.makeFollower(partitionState, checkpoints, topicId);
        }
    }
}
