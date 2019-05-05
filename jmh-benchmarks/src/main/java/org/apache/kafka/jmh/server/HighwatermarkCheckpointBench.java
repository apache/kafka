package org.apache.kafka.jmh.server;

import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.log.CleanerConfig;
import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.server.LogDirFailureChannel;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;
import kafka.utils.KafkaScheduler;
import kafka.utils.MockTime;
import kafka.utils.Scheduler;
import kafka.utils.TestUtils;
import kafka.zk.KafkaZkClient;
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
import scala.Option;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class HighwatermarkCheckpointBench {
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


    @Setup(Level.Trial)
    public void setup() throws Exception {
        this.scheduler = new KafkaScheduler(1, "scheduler-thread", true);
        this.brokerProperties = KafkaConfig.fromProps(TestUtils.createBrokerConfig(
                0, TestUtils.MockZkConnect(), true, true, 9092, Option.empty(), Option.empty(),
                Option.empty(), true, false, 0, false, 0, false, 0, Option.empty(), 1, true));
        this.metrics = new Metrics();
        this.time = new MockTime();
        this.failureChannel = new LogDirFailureChannel(brokerProperties.logDirs().size());
        final List<File> files =
                JavaConverters.seqAsJavaList(brokerProperties.logDirs()).stream().map(File::new).collect(Collectors.toList());
        this.logManager = TestUtils.createLogManager(JavaConverters.asScalaBuffer(files),
                LogConfig.apply(),
                CleanerConfig.apply(
                        1, 4*1024*1024L, 0.9d,
                        1024*1024, 32*1024*1024,
                        Double.MAX_VALUE, 15 * 1000, true, "MD5"),
                time);
        scheduler.startup();
        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        final MetadataCache metadataCache =
                new MetadataCache(this.brokerProperties.brokerId());
        this.quotaManagers =
                QuotaFactory.instantiate(this.brokerProperties,
                        this.metrics,
                        this.time, "");
        this.replicaManager = new ReplicaManager(
                this.brokerProperties,
                this.metrics,
                this.time,
                null,//this.zkClient,
                this.scheduler,
                this.logManager,
                new AtomicBoolean(false),
                this.quotaManagers,
                brokerTopicStats,
                metadataCache,
                this.failureChannel,
                Option.empty());
        this.replicaManager.startup();

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int topicNum = 0; topicNum < numTopics; topicNum++) {
            final String topicName = this.topicName + "-" + topicNum;
            for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
                topicPartitions.add(new TopicPartition(topicName, partitionNum));
            }
        }
        this.replicaManager.checkpointHighWatermarks();
        for (TopicPartition topicPartition : topicPartitions) {
            final Partition partition =
                    this.replicaManager.getOrCreatePartition(topicPartition);
            final Log log = this.logManager.getOrCreateLog(topicPartition,
                    LogConfig.apply(), true, false);
            final Replica replica = new Replica(this.brokerProperties.brokerId(),
                    topicPartition, this.time, 0, Option.apply(log));
            partition.addReplicaIfNotExists(replica);
        }
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
}

