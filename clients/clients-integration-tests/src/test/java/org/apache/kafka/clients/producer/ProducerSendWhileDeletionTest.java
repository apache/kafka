package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerSendWhileDeletionTest.BROKER_COUNT;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.NUM_PARTITIONS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = NUM_PARTITIONS_CONFIG, value = "2"),
        @ClusterConfigProperty(key = DEFAULT_REPLICATION_FACTOR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false")
    }
)
public class ProducerSendWhileDeletionTest {

    public static final int BROKER_COUNT = 2;
    private static final int DEFAULT_LINGER_MS = 5;
    private final ClusterInstance cluster;

    public ProducerSendWhileDeletionTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    /**
     * Tests that Producer gets self-recovered when a topic is deleted mid-way of produce.
     * <p>
     * Producer will attempt to send messages to the partition specified in each record, and should
     * succeed as long as the partition is included in the metadata.
     */
    @ClusterTest
    public void testSendWithTopicDeletionMidWay() throws Exception {
        int numRecords = 10;
        String topic = "topic";

        // Create topic with leader as 0 for the 2 partitions.
        Map<Integer, List<Integer>> topicAssignment = Map.of(
            0, List.of(0, 1),
            1, List.of(0, 1)
        );
        
        try (Admin admin = cluster.admin();
             Producer<byte[], byte[]> producer = createProducer()
        ) {
            
            admin.createTopics(List.of(new NewTopic(topic, topicAssignment)));

            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = Map.of(
                new TopicPartition(topic, 0), Optional.of(new NewPartitionReassignment(List.of(1, 0))),
                new TopicPartition(topic, 1), Optional.of(new NewPartitionReassignment(List.of(1, 0)))
            );

            // Change leader to 1 for both the partitions to increase leader epoch from 0 -> 1
            admin.alterPartitionReassignments(reassignment).all().get();
            
            for (int i = 1; i <= numRecords; i++) {
                RecordMetadata resp = producer.send(
                    new ProducerRecord<>(topic, null, ("value" + i).getBytes(StandardCharsets.UTF_8))
                ).get();
                assertEquals(topic, resp.topic());
            }

            // Start topic deletion
            admin.deleteTopics(List.of(topic)).all().get();

            // Verify that the topic is deleted when no metadata request comes in
            verifyTopicDeletion(topic, 2);

            // Producer should be able to send messages even after topic gets deleted and auto-created
            RecordMetadata finalResp = producer.send(
                new ProducerRecord<>(topic, null, "value".getBytes(StandardCharsets.UTF_8))
            ).get();
            assertEquals(topic, finalResp.topic());
        }
    }

    private Producer<byte[], byte[]> createProducer() {
        return cluster.producer(Map.of(
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000 + DEFAULT_LINGER_MS,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));
    }

    public void verifyTopicDeletion(String topic, int numPartitions) throws InterruptedException {

        List<TopicPartition> topicPartitions = IntStream.range(0, numPartitions)
                .mapToObj(i -> new TopicPartition(topic, i))
                .toList();

        // ensure that the topic-partition has been deleted from all brokers' replica managers
        TestUtils.waitForCondition(() -> 
            cluster.brokers().values().stream()
                .allMatch(broker -> topicPartitions.stream()
                        .allMatch(tp -> broker.replicaManager()
                                .onlinePartition(tp).isEmpty())
            ), "Replica manager's should have deleted all of this topic's partitions");

        // ensure that logs from all replicas are deleted
        TestUtils.waitForCondition(() ->
            cluster.brokers().values().stream()
                .allMatch(broker -> topicPartitions.stream()
                        .allMatch(tp -> broker.logManager().getLog(tp, false).isEmpty())
                ), "Replica logs not deleted after delete topic is complete");

        // ensure that topic is removed from all cleaner offsets
        TestUtils.waitForCondition(() -> {
            cluster.brokers().values().stream().allMatch(broker ->
                    topicPartitions.stream().allMatch(tp -> {
                        List<Map<TopicPartition, Long>> checkpoints = broker.logManager().liveLogDirs().stream()
                                .map(logDir -> {
                                    try {
                                        return new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null).read();
                                    } catch (Exception e) {
                                        return new HashMap<TopicPartition, Long>();
                                    }
                                })
                                .collect(Collectors.toList());
                        return checkpoints.stream().allMatch(checkpointsPerLogDir ->
                                !checkpointsPerLogDir.containsKey(tp));
        }, "Cleaner offset for deleted partition should have been removed");

        waitUntilTrue(() ->
                        cluster.brokers().values().stream().allMatch(broker ->
                                broker.config().logDirs().stream().allMatch(logDir ->
                                        topicPartitions.stream().allMatch(tp ->
                                                !new File(logDir, tp.topic() + "-" + tp.partition()).exists()))),
                "Failed to soft-delete the data to a delete directory");

        waitUntilTrue(() ->
                        cluster.brokers().values().stream().allMatch(broker ->
                                broker.config().logDirs().stream().allMatch(logDir -> {
                                    String[] directoryNames = new File(logDir).list();
                                    if (directoryNames == null) {
                                        return true;
                                    }
                                    return topicPartitions.stream().allMatch(tp ->
                                            Arrays.stream(directoryNames).noneMatch(directoryName ->
                                                    directoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                                                            directoryName.endsWith(UnifiedLog.DELETE_DIR_SUFFIX)));
                                })),
                "Failed to hard-delete the delete directory");
    }
}

