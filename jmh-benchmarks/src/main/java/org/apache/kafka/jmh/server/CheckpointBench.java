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

import kafka.api.ApiVersion;
import kafka.cluster.Partition;
import kafka.log.CleanerConfig;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.server.AlterIsrManager;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.server.LogDirFailureChannel;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;
import kafka.server.builders.ReplicaManagerBuilder;
import kafka.server.checkpoints.OffsetCheckpoints;
import kafka.server.metadata.MockConfigRepository;
import kafka.utils.KafkaScheduler;
import kafka.utils.MockTime;
import kafka.utils.Scheduler;
import kafka.utils.TestUtils;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.Option;

@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(value = Scope.Benchmark)
public class CheckpointBench {

    @Param({"100", "1000", "2000"})
    public int numTopics;

    @Param({"3"})
    public int numPartitions;

    private final String topicName = "foo";

    private Scheduler scheduler;

    private Metrics metrics;

    private MockTime time;

    private KafkaConfig brokerProperties;

    private ReplicaManager replicaManager;
    private QuotaFactory.QuotaManagers quotaManagers;
    private LogDirFailureChannel failureChannel;
    private LogManager logManager;
    private AlterIsrManager alterIsrManager;


    @SuppressWarnings("deprecation")
    @Setup(Level.Trial)
    public void setup() {
        this.scheduler = new KafkaScheduler(1, "scheduler-thread", true);
        this.brokerProperties = KafkaConfig.fromProps(TestUtils.createBrokerConfig(
                0, TestUtils.MockZkConnect(), true, true, 9092, Option.empty(), Option.empty(),
                Option.empty(), true, false, 0, false, 0, false, 0, Option.empty(), 1, true, 1,
                (short) 1));
        this.metrics = new Metrics();
        this.time = new MockTime();
        this.failureChannel = new LogDirFailureChannel(brokerProperties.logDirs().size());
        final List<File> files =
            JavaConverters.seqAsJavaList(brokerProperties.logDirs()).stream().map(File::new).collect(Collectors.toList());
        this.logManager = TestUtils.createLogManager(JavaConverters.asScalaBuffer(files),
                LogConfig.apply(), new MockConfigRepository(), CleanerConfig.apply(1, 4 * 1024 * 1024L, 0.9d,
                        1024 * 1024, 32 * 1024 * 1024,
                        Double.MAX_VALUE, 15 * 1000, true, "MD5"), time, ApiVersion.latestVersion());
        scheduler.startup();
        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        final MetadataCache metadataCache =
                MetadataCache.zkMetadataCache(this.brokerProperties.brokerId());
        this.quotaManagers =
                QuotaFactory.instantiate(this.brokerProperties,
                        this.metrics,
                        this.time, "");

        this.alterIsrManager = TestUtils.createAlterIsrManager();
        this.replicaManager = new ReplicaManagerBuilder().
            setConfig(brokerProperties).
            setMetrics(metrics).
            setTime(time).
            setScheduler(scheduler).
            setLogManager(logManager).
            setQuotaManagers(quotaManagers).
            setBrokerTopicStats(brokerTopicStats).
            setMetadataCache(metadataCache).
            setLogDirFailureChannel(failureChannel).
            setAlterIsrManager(alterIsrManager).
            build();
        replicaManager.startup();

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int topicNum = 0; topicNum < numTopics; topicNum++) {
            final String topicName = this.topicName + "-" + topicNum;
            for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
                topicPartitions.add(new TopicPartition(topicName, partitionNum));
            }
        }

        OffsetCheckpoints checkpoints = (logDir, topicPartition) -> Option.apply(0L);
        for (TopicPartition topicPartition : topicPartitions) {
            final Partition partition = this.replicaManager.createPartition(topicPartition);
            partition.createLogIfNotExists(true, false, checkpoints, Option.apply(Uuid.randomUuid()));
        }

        replicaManager.checkpointHighWatermarks();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        this.replicaManager.shutdown(false);
        this.metrics.close();
        this.scheduler.shutdown();
        this.quotaManagers.shutdown();
        for (File dir : JavaConverters.asJavaCollection(logManager.liveLogDirs())) {
            Utils.delete(dir);
        }
    }


    @Benchmark
    @Threads(1)
    public void measureCheckpointHighWatermarks() {
        this.replicaManager.checkpointHighWatermarks();
    }

    @Benchmark
    @Threads(1)
    public void measureCheckpointLogStartOffsets() {
        this.logManager.checkpointLogStartOffsets();
    }
}
