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

package org.apache.kafka.jmh.partition;

import kafka.cluster.AlterPartitionListener;
import kafka.cluster.DelayedOperations;
import kafka.cluster.Partition;
import kafka.log.LogManager;
import kafka.server.AlterPartitionManager;
import kafka.server.MetadataCache;
import kafka.server.builders.LogManagerBuilder;
import kafka.server.metadata.MockConfigRepository;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints;
import org.apache.kafka.storage.internals.log.CleanerConfig;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.jdk.javaapi.OptionConverters;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PartitionMakeFollowerBenchmark {
    private final File logDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
    private final KafkaScheduler scheduler = new KafkaScheduler(1, true, "scheduler");
    private final List<Integer> replicas = Arrays.asList(0, 1, 2);
    private final OffsetCheckpoints offsetCheckpoints = Mockito.mock(OffsetCheckpoints.class);
    private final DelayedOperations delayedOperations  = Mockito.mock(DelayedOperations.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private Option<Uuid> topicId;
    private Partition partition;
    private LogManager logManager;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (!logDir.mkdir())
            throw new IOException("error creating test directory");

        scheduler.startup();
        LogConfig logConfig = new LogConfig(new Properties());

        BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
        LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);
        logManager = new LogManagerBuilder().
            setLogDirs(Collections.singletonList(logDir)).
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
            setTime(Time.SYSTEM).setKeepPartitionMetadataFile(true).
            build();

        TopicPartition tp = new TopicPartition("topic", 0);
        topicId = OptionConverters.toScala(Optional.of(Uuid.randomUuid()));

        Mockito.when(offsetCheckpoints.fetch(logDir.getAbsolutePath(), tp)).thenReturn(Optional.of(0L));
        AlterPartitionListener alterPartitionListener = Mockito.mock(AlterPartitionListener.class);
        AlterPartitionManager alterPartitionManager = Mockito.mock(AlterPartitionManager.class);
        partition = new Partition(tp, 100,
            MetadataVersion.latestTesting(), 0, () -> -1, Time.SYSTEM,
            alterPartitionListener, delayedOperations,
            Mockito.mock(MetadataCache.class), logManager, alterPartitionManager, topicId);
        partition.createLogIfNotExists(true, false, offsetCheckpoints, topicId, Option.empty());
        executorService.submit((Runnable) () -> {
            SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "foo".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(2L, "bar".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8))
            };
            int initialOffSet = 0;
            while (true) {
                MemoryRecords memoryRecords =  MemoryRecords.withRecords(initialOffSet, Compression.NONE, 0, simpleRecords);
                partition.appendRecordsToFollowerOrFutureReplica(memoryRecords, false);
                initialOffSet = initialOffSet + 2;
            }
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException, InterruptedException {
        executorService.shutdownNow();
        logManager.shutdown(-1L);
        scheduler.shutdown();
        Utils.delete(logDir);
    }

    @Benchmark
    public boolean testMakeFollower() {
        LeaderAndIsrRequestData.LeaderAndIsrPartitionState partitionState = new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(replicas)
            .setPartitionEpoch(1)
            .setReplicas(replicas)
            .setIsNew(true);
        return partition.makeFollower(partitionState, offsetCheckpoints, topicId, Option.empty());
    }
}
