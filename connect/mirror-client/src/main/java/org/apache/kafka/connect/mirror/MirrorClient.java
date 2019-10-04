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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.SchemaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.time.Duration;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;

/** Interprets MM2's internal topics (checkpoints, heartbeats) on a given cluster.
 *  <p> 
 *  Given a top-level "mm2.properties" configuration file, MirrorClients can be constructed
 *  for individual clusters as follows:
 *  </p> 
 *  <pre>
 *    MirrorMakerConfig mmConfig = new MirrorMakerConfig(props);
 *    MirrorClientConfig mmClientConfig = mmConfig.clientConfig("some-cluster");
 *    MirrorClient mmClient = new Mirrorclient(mmClientConfig);
 *  </pre>
 */
public class MirrorClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MirrorClient.class);

    private AdminClient adminClient;
    private ReplicationPolicy replicationPolicy;
    private Map<String, Object> consumerConfig;

    public MirrorClient(Map<String, Object> props) {
        this(new MirrorClientConfig(props));
    }

    public MirrorClient(MirrorClientConfig config) {
        adminClient = AdminClient.create(config.adminConfig());
        consumerConfig = config.consumerConfig();
        replicationPolicy = config.replicationPolicy();
    }

    // for testing
    MirrorClient(AdminClient adminClient, ReplicationPolicy replicationPolicy,
            Map<String, Object> consumerConfig) {
        this.adminClient = adminClient;
        this.replicationPolicy = replicationPolicy;
        this.consumerConfig = consumerConfig;
    }

    /** Close internal clients. */
    public void close() {
        adminClient.close();
    }

    /** Get the ReplicationPolicy instance used to interpret remote topics. This instance is constructed based on
     *  relevant configuration properties, including {@code replication.policy.class}. */
    public ReplicationPolicy replicationPolicy() {
        return replicationPolicy;
    }

    /** Compute shortest number of hops from an upstream source cluster.
     *  For example, given replication flow A-&gt;B-&gt;C, there are two hops from A to C.
     *  Returns -1 if upstream cluster is unreachable.
     */
    public int replicationHops(String upstreamClusterAlias) throws InterruptedException {
        return heartbeatTopics().stream()
            .map(x -> countHopsForTopic(x, upstreamClusterAlias))
            .filter(x -> x != -1)
            .mapToInt(x -> x)
            .min()
            .orElse(-1);
    }

    /** Find all heartbeat topics on this cluster. Heartbeat topics are replicated from other clusters. */
    public Set<String> heartbeatTopics() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isHeartbeatTopic)
            .collect(Collectors.toSet());
    }

    /** Find all checkpoint topics on this cluster. */
    public Set<String> checkpointTopics() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isCheckpointTopic)
            .collect(Collectors.toSet());
    }

    /** Find upstream clusters, which may be multiple hops away, based on incoming heartbeats. */
    public Set<String> upstreamClusters() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isHeartbeatTopic)
            .flatMap(x -> allSources(x).stream())
            .distinct()
            .collect(Collectors.toSet());
    }

    /** Find all remote topics on this cluster. This does not include internal topics (heartbeats, checkpoints). */
    public Set<String> remoteTopics() throws InterruptedException {
        return listTopics().stream()
            .filter(this::isRemoteTopic)
            .collect(Collectors.toSet());
    }

    /** Find all remote topics that have been replicated directly from the given source cluster. */
    public Set<String> remoteTopics(String source) throws InterruptedException {
        return listTopics().stream()
            .filter(this::isRemoteTopic)
            .filter(x -> source.equals(replicationPolicy.topicSource(x)))
            .distinct()
            .collect(Collectors.toSet());
    }

    /** Translate a remote consumer group's offsets into corresponding local offsets. Topics are automatically
     *  renamed according to the ReplicationPolicy.
     *  @param consumerGroupId group ID of remote consumer group
     *  @param remoteClusterAlias alias of remote cluster
     *  @param timeout timeout
     */
    public Map<TopicPartition, OffsetAndMetadata> remoteConsumerOffsets(String consumerGroupId,
            String remoteClusterAlias, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig,
            new ByteArrayDeserializer(), new ByteArrayDeserializer());
        try {
            // checkpoint topics are not "remote topics", as they are not replicated. So we don't need
            // to use ReplicationPolicy to create the checkpoint topic here.
            String checkpointTopic = remoteClusterAlias + MirrorClientConfig.CHECKPOINTS_TOPIC_SUFFIX;
            List<TopicPartition> checkpointAssignment =
                Collections.singletonList(new TopicPartition(checkpointTopic, 0));
            consumer.assign(checkpointAssignment);
            consumer.seekToBeginning(checkpointAssignment);
            while (System.currentTimeMillis() < deadline && !endOfStream(consumer, checkpointAssignment)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                        if (checkpoint.consumerGroupId().equals(consumerGroupId)) {
                            offsets.put(checkpoint.topicPartition(), checkpoint.offsetAndMetadata());
                        }
                    } catch (SchemaException e) {
                        log.info("Could not deserialize record. Skipping.", e);
                    }
                }
            }
            log.info("Consumed {} checkpoint records for {} from {}.", offsets.size(),
                consumerGroupId, checkpointTopic);
        } finally {
            consumer.close();
        }
        return offsets;
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
        while (true) {
            hops++;
            String source = replicationPolicy.topicSource(topic);
            if (source == null) {
                return -1;
            }
            if (source.equals(sourceClusterAlias)) {
                return hops;
            }
            topic = replicationPolicy.upstreamTopic(topic);
        } 
    }

    boolean isHeartbeatTopic(String topic) {
        // heartbeats are replicated, so we must use ReplicationPolicy here
        return MirrorClientConfig.HEARTBEATS_TOPIC.equals(replicationPolicy.originalTopic(topic));
    }

    boolean isCheckpointTopic(String topic) {
        // checkpoints are not replicated, so we don't need to use ReplicationPolicy here
        return topic.endsWith(MirrorClientConfig.CHECKPOINTS_TOPIC_SUFFIX);
    }

    boolean isRemoteTopic(String topic) {
        return !replicationPolicy.isInternalTopic(topic)
            && replicationPolicy.topicSource(topic) != null;
    }

    Set<String> allSources(String topic) {
        Set<String> sources = new HashSet<>();
        String source = replicationPolicy.topicSource(topic);
        while (source != null) {
            sources.add(source);
            topic = replicationPolicy.upstreamTopic(topic);
            source = replicationPolicy.topicSource(topic);
        }
        return sources;
    }

    static private boolean endOfStream(Consumer<?, ?> consumer, Collection<TopicPartition> assignments) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignments);
        for (TopicPartition topicPartition : assignments) {
            if (consumer.position(topicPartition) < endOffsets.get(topicPartition)) {
                return false;
            }
        }
        return true;
    }
}
