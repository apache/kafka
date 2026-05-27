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

package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Client to interact with MirrorMaker internal topics (checkpoints, heartbeats) on a given cluster.
 * Whenever possible use the methods from {@link RemoteClusterUtils} instead of directly using MirrorClient.
 */
public class MirrorClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MirrorClient.class);

    private final Admin adminClient;
    private final ReplicationPolicy replicationPolicy;
    private final Map<String, Object> consumerConfig;

    public MirrorClient(Map<String, Object> props) {
        this(new MirrorClientConfig(props));
    }

    public MirrorClient(MirrorClientConfig config) {
        adminClient = config.forwardingAdmin(config.adminConfig());
        consumerConfig = config.consumerConfig();
        replicationPolicy = config.replicationPolicy();
    }

    // for testing
    MirrorClient(Admin adminClient, ReplicationPolicy replicationPolicy,
            Map<String, Object> consumerConfig) {
        this.adminClient = adminClient;
        this.replicationPolicy = replicationPolicy;
        this.consumerConfig = consumerConfig;
    }

    // for testing
    Consumer<byte[], byte[]> consumer() {
        return new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Closes internal clients.
     */
    public void close() {
        adminClient.close();
    }

    /**
     * Gets the {@link ReplicationPolicy} instance used to interpret remote topics. This instance is constructed based on
     * relevant configuration properties, including {@code replication.policy.class}.
     */
    public ReplicationPolicy replicationPolicy() {
        return replicationPolicy;
    }

    /**
     * Computes the shortest number of hops from an upstream source cluster.
     * For example, given replication flow A-&gt;B-&gt;C, there are two hops from A to C.
     * Returns -1 if the upstream cluster is unreachable.
     */
    public int replicationHops(String upstreamClusterAlias) throws InterruptedException {
        return heartbeatTopics().stream()
            .map(x -> countHopsForTopic(x, upstreamClusterAlias))
            .filter(x -> x != -1)
            .mapToInt(x -> x)
            .min()
            .orElse(-1);
    }

    /**
     * Finds all heartbeats topics on this cluster. Heartbeats topics are replicated from other clusters.
     */
    public Set<String> heartbeatTopics() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isHeartbeatTopic)
            .collect(Collectors.toSet());
    }

    /**
     * Finds all checkpoints topics on this cluster.
     */
    public Set<String> checkpointTopics() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isCheckpointTopic)
            .collect(Collectors.toSet());
    }

    /**
     * Finds upstream clusters, which may be multiple hops away, based on incoming heartbeats.
     */
    public Set<String> upstreamClusters() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isHeartbeatTopic)
            .flatMap(x -> allSources(x).stream())
            .collect(Collectors.toSet());
    }

    /**
     * Finds all remote topics on this cluster. This does not include internal topics (heartbeats, checkpoints).
     */
    public Set<String> remoteTopics() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isRemoteTopic)
            .collect(Collectors.toSet());
    }

    /**
     * Finds all remote topics that have been replicated directly from the given source cluster.
     */
    public Set<String> remoteTopics(String source) throws InterruptedException {
        return listTopics().stream()
            .filter(this::isRemoteTopic)
            .filter(x -> source.equals(replicationPolicy.topicSource(x)))
            .collect(Collectors.toSet());
    }

    /**
     * Translates remote consumer groups' offsets into corresponding local offsets. Topics are automatically
     * renamed according to the ReplicationPolicy.
     * @param consumerGroupPattern The regex pattern specifying the consumer groups to translate offsets for
     * @param remoteClusterAlias The alias of remote cluster
     * @param timeout The maximum time to block when consuming from the checkpoints topic
     * @throws IllegalArgumentException If any of the arguments are null
     */
    public Map<String, Map<TopicPartition, OffsetAndMetadata>> remoteConsumerOffsets(Pattern consumerGroupPattern,
             String remoteClusterAlias, Duration timeout) {
        if (consumerGroupPattern == null) {
            throw new IllegalArgumentException("`consumerGroupPattern` must not be null");
        }
        if (remoteClusterAlias == null) {
            throw new IllegalArgumentException("`remoteClusterAlias` must not be null");
        }
        if (timeout == null) {
            throw new IllegalArgumentException("`timeout` must not be null");
        }
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets = new HashMap<>();

        try (Consumer<byte[], byte[]> consumer = consumer()) {
            // checkpoint topics are not "remote topics", as they are not replicated. So we don't need
            // to use ReplicationPolicy to create the checkpoint topic here.
            String checkpointTopic = replicationPolicy.checkpointsTopic(remoteClusterAlias);
            List<TopicPartition> checkpointAssignment = List.of(new TopicPartition(checkpointTopic, 0));
            consumer.assign(checkpointAssignment);
            consumer.seekToBeginning(checkpointAssignment);
            while (System.currentTimeMillis() < deadline && !endOfStream(consumer, checkpointAssignment)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                        String consumerGroupId = checkpoint.consumerGroupId();
                        if (consumerGroupPattern.matcher(consumerGroupId).matches()) {
                            offsets.computeIfAbsent(consumerGroupId, k -> new HashMap<>())
                                    .put(checkpoint.topicPartition(), checkpoint.offsetAndMetadata());
                        }
                    } catch (SchemaException e) {
                        log.info("Could not deserialize record. Skipping.", e);
                    }
                }
            }
            log.info("Consumed {} checkpoint records from {}.", offsets.size(), checkpointTopic);
        }
        return offsets;
    }

    /**
     * Translates a remote consumer group's offsets into corresponding local offsets. Topics are automatically
     * renamed according to the ReplicationPolicy.
     * @param consumerGroupId The group ID of remote consumer group
     * @param remoteClusterAlias The alias of remote cluster
     * @param timeout The maximum time to block when consuming from the checkpoints topic
     */
    public Map<TopicPartition, OffsetAndMetadata> remoteConsumerOffsets(String consumerGroupId,
            String remoteClusterAlias, Duration timeout) {
        Pattern consumerGroupPattern = Pattern.compile(Pattern.quote(consumerGroupId));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets = remoteConsumerOffsets(consumerGroupPattern, remoteClusterAlias, timeout);
        return offsets.getOrDefault(consumerGroupId, new HashMap<>());
    }

    Set<String> listTopics() throws InterruptedException {
        try {
            return adminClient.listTopics().names().get();
        } catch (ExecutionException e) {
            throw new KafkaException(e.getCause());
        }
    }

    int countHopsForTopic(String topic, String sourceClusterAlias) {
        int hops = 0;
        Set<String> visited = new HashSet<>();
        while (true) {
            hops++;
            String source = replicationPolicy.topicSource(topic);
            if (source == null) {
                return -1;
            }
            if (source.equals(sourceClusterAlias)) {
                return hops;
            }
            if (visited.contains(source)) {
                // Extra check for IdentityReplicationPolicy and similar impls that cannot prevent cycles.
                // We assume we're stuck in a cycle and will never find sourceClusterAlias.
                return -1;
            }
            visited.add(source);
            topic = replicationPolicy.upstreamTopic(topic);
        }
    }

    boolean isHeartbeatTopic(String topic) {
        return replicationPolicy.isHeartbeatsTopic(topic);
    }

    boolean isCheckpointTopic(String topic) {
        return replicationPolicy.isCheckpointsTopic(topic);
    }

    boolean isRemoteTopic(String topic) {
        return !replicationPolicy.isInternalTopic(topic)
            && replicationPolicy.topicSource(topic) != null;
    }

    Set<String> allSources(String topic) {
        Set<String> sources = new HashSet<>();
        String source = replicationPolicy.topicSource(topic);
        while (source != null && !sources.contains(source)) {
            // The extra Set.contains above is for ReplicationPolicies that cannot prevent cycles.
            sources.add(source);
            topic = replicationPolicy.upstreamTopic(topic);
            source = replicationPolicy.topicSource(topic);
        }
        return sources;
    }

    private static boolean endOfStream(Consumer<?, ?> consumer, Collection<TopicPartition> assignments) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignments);
        for (TopicPartition topicPartition : assignments) {
            if (consumer.position(topicPartition) < endOffsets.get(topicPartition)) {
                return false;
            }
        }
        return true;
    }
}
