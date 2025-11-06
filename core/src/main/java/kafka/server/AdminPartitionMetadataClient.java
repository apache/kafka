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
package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.coordinator.group.metrics.PartitionMetadataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Admin-based implementation of {@link PartitionMetadataClient} that uses
 * {@link Admin#listOffsets(Map)} to retrieve the latest offsets for topic partitions.
 */
public class AdminPartitionMetadataClient implements PartitionMetadataClient {
    private static final Logger log = LoggerFactory.getLogger(AdminPartitionMetadataClient.class);
    private final Admin adminClient;

    /**
     * Creates a new AdminPartitionMetadataClient with the provided Admin instance.
     *
     * @param adminProps  The Admin client props to use for creating the adminClient.
     */
    public AdminPartitionMetadataClient(Map<String, Object> adminProps) {
        this.adminClient = Admin.create(adminProps);
    }

    @Override
    public Map<TopicPartition, CompletableFuture<Long>> listLatestOffsets(Set<TopicPartition> topicPartitions) {
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            return new HashMap<>();
        }

        // Build the map of topic partitions to OffsetSpec.latest()
        Map<TopicPartition, OffsetSpec> offsetSpecMap = new HashMap<>();
        for (TopicPartition partition : topicPartitions) {
            offsetSpecMap.put(partition, OffsetSpec.latest());
        }

        try {
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(offsetSpecMap);
            Map<TopicPartition, CompletableFuture<Long>> offsets = new HashMap<>();

            // Retrieve the offset for each partition
            for (TopicPartition partition : topicPartitions) {
                CompletableFuture<ListOffsetsResult.ListOffsetsResultInfo> listOffsetsResultInfo =
                    toCompletableFuture(listOffsetsResult.partitionResult(partition));
                offsets.put(partition, listOffsetsResultInfo.thenApply(ListOffsetsResult.ListOffsetsResultInfo::offset));
            }

            return offsets;
        } catch (Exception e) {
            log.error("Failed to list latest offsets for partitions: {}", topicPartitions, e);
            throw e;
        }
    }

    public static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        kafkaFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                completableFuture.completeExceptionally(ex);
            } else {
                completableFuture.complete(result);
            }
        });
        return completableFuture;
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
