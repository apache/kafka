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
package org.apache.kafka.common.requests;

import java.util.HashMap;
import java.util.Set;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RequestTestUtils {

    public static boolean hasIdempotentRecords(ProduceRequest request) {
        return RequestUtils.flag(request, RecordBatch::hasProducerId);
    }

    public static ByteBuffer serializeRequestHeader(RequestHeader header) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(header.size(serializationCache));
        header.write(buffer, serializationCache);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeResponseWithHeader(AbstractResponse response, short version, int correlationId) {
        return response.serializeWithHeader(new ResponseHeader(correlationId,
            response.apiKey().responseHeaderVersion(version)), version);
    }

    public static MetadataResponse metadataResponse(Collection<Node> brokers,
                                                    String clusterId, int controllerId,
                                                    List<MetadataResponse.TopicMetadata> topicMetadataList) {
        return metadataResponse(brokers, clusterId, controllerId, topicMetadataList, ApiKeys.METADATA.latestVersion());
    }

    public static MetadataResponse metadataResponse(Collection<Node> brokers,
                                                    String clusterId, int controllerId,
                                                    List<MetadataResponse.TopicMetadata> topicMetadataList,
                                                    short responseVersion) {
        return metadataResponse(MetadataResponse.DEFAULT_THROTTLE_TIME, brokers, clusterId, controllerId,
                topicMetadataList, MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED, responseVersion);
    }

    public static MetadataResponse metadataResponse(int throttleTimeMs, Collection<Node> brokers,
                                                    String clusterId, int controllerId,
                                                    List<MetadataResponse.TopicMetadata> topicMetadatas,
                                                    int clusterAuthorizedOperations,
                                                    short responseVersion) {
        List<MetadataResponseData.MetadataResponseTopic> topics = new ArrayList<>();
        topicMetadatas.forEach(topicMetadata -> {
            MetadataResponseData.MetadataResponseTopic metadataResponseTopic = new MetadataResponseData.MetadataResponseTopic();
            metadataResponseTopic
                    .setErrorCode(topicMetadata.error().code())
                    .setName(topicMetadata.topic())
                    .setTopicId(topicMetadata.topicId())
                    .setIsInternal(topicMetadata.isInternal())
                    .setTopicAuthorizedOperations(topicMetadata.authorizedOperations());

            for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
                metadataResponseTopic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                        .setErrorCode(partitionMetadata.error.code())
                        .setPartitionIndex(partitionMetadata.partition())
                        .setLeaderId(partitionMetadata.leaderId.orElse(MetadataResponse.NO_LEADER_ID))
                        .setLeaderEpoch(partitionMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                        .setReplicaNodes(partitionMetadata.replicaIds)
                        .setIsrNodes(partitionMetadata.inSyncReplicaIds)
                        .setOfflineReplicas(partitionMetadata.offlineReplicaIds));
            }
            topics.add(metadataResponseTopic);
        });
        return MetadataResponse.prepareResponse(responseVersion, throttleTimeMs, brokers, clusterId, controllerId,
                topics, clusterAuthorizedOperations); }

    public static MetadataResponse metadataUpdateWith(final int numNodes,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith("kafka-cluster", numNodes, topicPartitionCounts);
    }

    public static MetadataResponse metadataUpdateWith(final int numNodes,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final Function<TopicPartition, Integer> epochSupplier) {
        return metadataUpdateWith("kafka-cluster", numNodes, Collections.emptyMap(),
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith(clusterId, numNodes, Collections.emptyMap(),
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final short responseVersion) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, responseVersion, Collections.emptyMap());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final Function<TopicPartition, Integer> epochSupplier) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
    }

    public static MetadataResponse metadataUpdateWithIds(final int numNodes,
                                                         final Map<String, Integer> topicPartitionCounts,
                                                         final Map<String, Uuid> topicIds) {
        return metadataUpdateWith("kafka-cluster", numNodes, Collections.emptyMap(),
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(),
                topicIds);
    }

    public static MetadataResponse metadataUpdateWithIds(final int numNodes,
                                                         final Set<TopicIdPartition> partitions,
                                                         final Function<TopicPartition, Integer> epochSupplier) {
        final Map<String, Integer> topicPartitionCounts = new HashMap<>();
        final Map<String, Uuid> topicIds = new HashMap<>();

        partitions.forEach(partition -> {
            topicPartitionCounts.compute(partition.topic(), (key, value) -> value == null ? 1 : value + 1);
            topicIds.putIfAbsent(partition.topic(), partition.topicId());
        });

        return metadataUpdateWithIds(numNodes, topicPartitionCounts, epochSupplier, topicIds);
    }

    public static MetadataResponse metadataUpdateWithIds(final int numNodes,
                                                         final Map<String, Integer> topicPartitionCounts,
                                                         final Function<TopicPartition, Integer> epochSupplier,
                                                         final Map<String, Uuid> topicIds) {
        return metadataUpdateWith("kafka-cluster", numNodes, Collections.emptyMap(),
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(),
                topicIds);
    }

    public static MetadataResponse metadataUpdateWithIds(final int numNodes,
                                                         final Map<String, Integer> topicPartitionCounts,
                                                         final Function<TopicPartition, Integer> epochSupplier,
                                                         final Map<String, Uuid> topicIds,
                                                         final Boolean leaderOnly) {
        return metadataUpdateWith("kafka-cluster", numNodes, Collections.emptyMap(),
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(),
                topicIds, leaderOnly);
    }

    public static MetadataResponse metadataUpdateWithIds(final String clusterId,
                                                         final int numNodes,
                                                         final Map<String, Errors> topicErrors,
                                                         final Map<String, Integer> topicPartitionCounts,
                                                         final Function<TopicPartition, Integer> epochSupplier,
                                                         final Map<String, Uuid> topicIds) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion(), topicIds);
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final Function<TopicPartition, Integer> epochSupplier,
                                                      final PartitionMetadataSupplier partitionSupplier,
                                                      final short responseVersion,
                                                      final Map<String, Uuid> topicIds) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, epochSupplier, partitionSupplier,
                responseVersion, topicIds, true);
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final Function<TopicPartition, Integer> epochSupplier,
                                                      final PartitionMetadataSupplier partitionSupplier,
                                                      final short responseVersion,
                                                      final Map<String, Uuid> topicIds,
                                                      final Boolean leaderOnly) {
        final List<Node> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++)
            nodes.add(new Node(i, "localhost", 1969 + i));

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        for (Map.Entry<String, Integer> topicPartitionCountEntry : topicPartitionCounts.entrySet()) {
            String topic = topicPartitionCountEntry.getKey();
            int numPartitions = topicPartitionCountEntry.getValue();

            List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                TopicPartition tp = new TopicPartition(topic, i);
                Node leader = nodes.get(i % nodes.size());
                List<Integer> replicaIds = leaderOnly ? Collections.singletonList(leader.id()) : nodes.stream().map(Node::id).collect(Collectors.toList());
                partitionMetadata.add(partitionSupplier.supply(
                        Errors.NONE, tp, Optional.of(leader.id()), Optional.ofNullable(epochSupplier.apply(tp)),
                        replicaIds, replicaIds, Collections.emptyList()));
            }

            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic, topicIds.getOrDefault(topic, Uuid.ZERO_UUID),
                    Topic.isInternal(topic), partitionMetadata, MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));
        }

        for (Map.Entry<String, Errors> topicErrorEntry : topicErrors.entrySet()) {
            String topic = topicErrorEntry.getKey();
            topicMetadata.add(new MetadataResponse.TopicMetadata(topicErrorEntry.getValue(), topic,
                    Topic.isInternal(topic), Collections.emptyList()));
        }

        return metadataResponse(nodes, clusterId, 0, topicMetadata, responseVersion);
    }

    @FunctionalInterface
    public interface PartitionMetadataSupplier {
        MetadataResponse.PartitionMetadata supply(Errors error,
                                                  TopicPartition partition,
                                                  Optional<Integer> leaderId,
                                                  Optional<Integer> leaderEpoch,
                                                  List<Integer> replicas,
                                                  List<Integer> isr,
                                                  List<Integer> offlineReplicas);
    }
}
