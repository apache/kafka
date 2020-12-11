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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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

public class RequestTestUtils {

    public static ByteBuffer serializeRequestHeader(RequestHeader header) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(header.size(serializationCache));
        header.write(buffer, serializationCache);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeRequestWithHeader(RequestHeader header, AbstractRequest request) {
        return RequestUtils.serialize(header.data(), header.headerVersion(), request.data(), request.version());
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
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith(clusterId, numNodes, Collections.emptyMap(),
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final short responseVersion) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, responseVersion);
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final Function<TopicPartition, Integer> epochSupplier) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors,
                topicPartitionCounts, epochSupplier, MetadataResponse.PartitionMetadata::new, ApiKeys.METADATA.latestVersion());
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final Function<TopicPartition, Integer> epochSupplier,
                                                      final PartitionMetadataSupplier partitionSupplier,
                                                      final short responseVersion) {
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
                List<Integer> replicaIds = Collections.singletonList(leader.id());
                partitionMetadata.add(partitionSupplier.supply(
                        Errors.NONE, tp, Optional.of(leader.id()), Optional.ofNullable(epochSupplier.apply(tp)),
                        replicaIds, replicaIds, replicaIds));
            }

            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic,
                    Topic.isInternal(topic), partitionMetadata));
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
