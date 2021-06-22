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

import kafka.api.ApiVersion$;
import kafka.cluster.DelayedOperations;
import kafka.cluster.IsrChangeListener;
import kafka.cluster.Partition;
import kafka.log.CleanerConfig;
import kafka.log.Defaults;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.server.AlterIsrManager;
import kafka.server.BrokerTopicStats;
import kafka.server.LogDirFailureChannel;
import kafka.server.MetadataCache;
import kafka.server.checkpoints.OffsetCheckpoints;
import kafka.server.metadata.CachedConfigRepository;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
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
import scala.Option;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class PartitionMakeFollowerBenchmark {
    private LogManager logManager;
    private File logDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
    private KafkaScheduler scheduler = new KafkaScheduler(1, "scheduler", true);
    private Partition partition;
    private List<Integer> replicas = Arrays.asList(0, 1, 2);
    private OffsetCheckpoints offsetCheckpoints = Mockito.mock(OffsetCheckpoints.class);
    private DelayedOperations delayedOperations  = Mockito.mock(DelayedOperations.class);
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private Option<Uuid> topicId;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (!logDir.mkdir())
            throw new IOException("error creating test directory");

        scheduler.startup();
        LogConfig logConfig = createLogConfig();

        List<File> logDirs = Collections.singletonList(logDir);
        BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);
        logManager = new LogManager(JavaConverters.asScalaIteratorConverter(logDirs.iterator()).asScala().toSeq(),
            JavaConverters.asScalaIteratorConverter(new ArrayList<File>().iterator()).asScala().toSeq(),
            new CachedConfigRepository(),
            logConfig,
            new CleanerConfig(0, 0, 0, 0, 0, 0.0, 0, false, "MD5"),
            1,
            1000L,
            10000L,
            10000L,
            1000L,
            60000,
            scheduler,
            brokerTopicStats,
            logDirFailureChannel,
            Time.SYSTEM,
            true);

        TopicPartition tp = new TopicPartition("topic", 0);
        topicId = OptionConverters.toScala(Optional.of(Uuid.randomUuid()));

        Mockito.when(offsetCheckpoints.fetch(logDir.getAbsolutePath(), tp)).thenReturn(Option.apply(0L));
        IsrChangeListener isrChangeListener = Mockito.mock(IsrChangeListener.class);
        AlterIsrManager alterIsrManager = Mockito.mock(AlterIsrManager.class);
        partition = new Partition(tp, 100,
            ApiVersion$.MODULE$.latestVersion(), 0, Time.SYSTEM,
            isrChangeListener, delayedOperations,
            Mockito.mock(MetadataCache.class), logManager, alterIsrManager);
        partition.createLogIfNotExists(true, false, offsetCheckpoints, topicId);
        executorService.submit((Runnable) () -> {
            SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "foo".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(2L, "bar".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8))
            };
            int initialOffSet = 0;
            while (true) {
                MemoryRecords memoryRecords =  MemoryRecords.withRecords(initialOffSet, CompressionType.NONE, 0, simpleRecords);
                partition.appendRecordsToFollowerOrFutureReplica(memoryRecords, false);
                initialOffSet = initialOffSet + 2;
            }
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        executorService.shutdownNow();
        logManager.shutdown();
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
            .setZkVersion(1)
            .setReplicas(replicas)
            .setIsNew(true);
        return partition.makeFollower(partitionState, offsetCheckpoints, topicId);
    }

    private static LogConfig createLogConfig() {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentMsProp(), Defaults.SegmentMs());
        logProps.put(LogConfig.SegmentBytesProp(), Defaults.SegmentSize());
        logProps.put(LogConfig.RetentionMsProp(), Defaults.RetentionMs());
        logProps.put(LogConfig.RetentionBytesProp(), Defaults.RetentionSize());
        logProps.put(LogConfig.SegmentJitterMsProp(), Defaults.SegmentJitterMs());
        logProps.put(LogConfig.CleanupPolicyProp(), Defaults.CleanupPolicy());
        logProps.put(LogConfig.MaxMessageBytesProp(), Defaults.MaxMessageSize());
        logProps.put(LogConfig.IndexIntervalBytesProp(), Defaults.IndexInterval());
        logProps.put(LogConfig.SegmentIndexBytesProp(), Defaults.MaxIndexSize());
        logProps.put(LogConfig.MessageFormatVersionProp(), Defaults.MessageFormatVersion());
        logProps.put(LogConfig.FileDeleteDelayMsProp(), Defaults.FileDeleteDelayMs());
        return LogConfig.apply(logProps, new scala.collection.immutable.HashSet<>());
    }
}
