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
import kafka.cluster.Replica;
import kafka.log.LogManager;
import kafka.server.AlterPartitionManager;
import kafka.server.MetadataCache;
import kafka.server.builders.LogManagerBuilder;
import kafka.server.metadata.MockConfigRepository;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.jdk.javaapi.OptionConverters;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class UpdateFollowerFetchStateBenchmark {
    private final TopicPartition topicPartition = new TopicPartition(UUID.randomUUID().toString(), 0);
    private final Option<Uuid> topicId = OptionConverters.toScala(Optional.of(Uuid.randomUuid()));
    private final File logDir = new File(System.getProperty("java.io.tmpdir"), topicPartition.toString());
    private final KafkaScheduler scheduler = new KafkaScheduler(1, true, "scheduler");
    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
    private final LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);
    private long nextOffset = 0;
    private LogManager logManager;
    private Partition partition;
    private Replica replica1;
    private Replica replica2;

    @Setup(Level.Trial)
    public void setUp() {
        scheduler.startup();
        LogConfig logConfig = createLogConfig();
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
            setTime(Time.SYSTEM).
            setKeepPartitionMetadataFile(true).
            build();
        OffsetCheckpoints offsetCheckpoints = Mockito.mock(OffsetCheckpoints.class);
        Mockito.when(offsetCheckpoints.fetch(logDir.getAbsolutePath(), topicPartition)).thenReturn(Optional.of(0L));
        DelayedOperations delayedOperations = new DelayedOperationsMock();

        // one leader, plus two followers
        List<Integer> replicas = new ArrayList<>();
        replicas.add(0);
        replicas.add(1);
        replicas.add(2);
        LeaderAndIsrPartitionState partitionState = new LeaderAndIsrPartitionState()
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(replicas)
            .setPartitionEpoch(1)
            .setReplicas(replicas)
            .setIsNew(true);
        AlterPartitionListener alterPartitionListener = Mockito.mock(AlterPartitionListener.class);
        AlterPartitionManager alterPartitionManager = Mockito.mock(AlterPartitionManager.class);
        partition = new Partition(topicPartition, 100,
                MetadataVersion.latestTesting(), 0, () -> -1, Time.SYSTEM,
                alterPartitionListener, delayedOperations,
                Mockito.mock(MetadataCache.class), logManager, alterPartitionManager, topicId);
        partition.makeLeader(partitionState, offsetCheckpoints, topicId, Option.empty());
        replica1 = partition.getReplica(1).get();
        replica2 = partition.getReplica(2).get();
    }

    // avoid mocked DelayedOperations to avoid mocked class affecting benchmark results
    private class DelayedOperationsMock extends DelayedOperations {
        DelayedOperationsMock() {
            super(topicId, topicPartition, null, null, null, null);
        }

        @Override
        public int numDelayedDelete() {
            return 0;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        logManager.shutdown(-1L);
        scheduler.shutdown();
    }

    private LogConfig createLogConfig() {
        return new LogConfig(new Properties());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void updateFollowerFetchStateBench() {
        // measure the impact of two follower fetches on the leader
        partition.updateFollowerFetchState(replica1, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, nextOffset, -1);
        partition.updateFollowerFetchState(replica2, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, nextOffset, -1);
        nextOffset++;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void updateFollowerFetchStateBenchNoChange() {
        // measure the impact of two follower fetches on the leader when the follower didn't
        // end up fetching anything
        partition.updateFollowerFetchState(replica1, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, 100, -1);
        partition.updateFollowerFetchState(replica2, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, 100, -1);
    }
}
