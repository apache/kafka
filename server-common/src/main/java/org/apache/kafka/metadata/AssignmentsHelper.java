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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class AssignmentsHelper {

    /**
     * Build a AssignReplicasToDirsRequestData from a map of TopicIdPartition to Uuid.
     */
    public static AssignReplicasToDirsRequestData buildRequestData(int brokerId, long brokerEpoch, Map<TopicIdPartition, Uuid> assignment) {
        Map<Uuid, AssignReplicasToDirsRequestData.DirectoryData> directoryMap = new HashMap<>();
        Map<Uuid, Map<Uuid, AssignReplicasToDirsRequestData.TopicData>> topicMap = new HashMap<>();
        for (Map.Entry<TopicIdPartition, Uuid> entry : assignment.entrySet()) {
            TopicIdPartition topicPartition = entry.getKey();
            Uuid directoryId = entry.getValue();
            AssignReplicasToDirsRequestData.DirectoryData directory = directoryMap.computeIfAbsent(directoryId, d -> new AssignReplicasToDirsRequestData.DirectoryData().setId(directoryId));
            AssignReplicasToDirsRequestData.TopicData topic = topicMap.computeIfAbsent(directoryId, d -> new HashMap<>())
                    .computeIfAbsent(topicPartition.topicId(), topicId -> {
                        AssignReplicasToDirsRequestData.TopicData data = new AssignReplicasToDirsRequestData.TopicData().setTopicId(topicId);
                        directory.topics().add(data);
                        return data;
                    });
            AssignReplicasToDirsRequestData.PartitionData partition = new AssignReplicasToDirsRequestData.PartitionData().setPartitionIndex(topicPartition.partitionId());
            topic.partitions().add(partition);
        }
        return new AssignReplicasToDirsRequestData()
                .setBrokerId(brokerId)
                .setBrokerEpoch(brokerEpoch)
                .setDirectories(new ArrayList<>(directoryMap.values()));
    }

    /**
     * Build a AssignReplicasToDirsRequestData from a map of TopicIdPartition to Uuid.
     */
    public static AssignReplicasToDirsResponseData buildResponseData(short errorCode, int throttleTimeMs, Map<Uuid, Map<TopicIdPartition, Errors>> errors) {
        Map<Uuid, AssignReplicasToDirsResponseData.DirectoryData> directoryMap = new HashMap<>();
        Map<Uuid, Map<Uuid, AssignReplicasToDirsResponseData.TopicData>> topicMap = new HashMap<>();
        for (Map.Entry<Uuid, Map<TopicIdPartition, Errors>> dirEntry : errors.entrySet()) {
            Uuid directoryId = dirEntry.getKey();
            AssignReplicasToDirsResponseData.DirectoryData directory = directoryMap.computeIfAbsent(directoryId, d -> new AssignReplicasToDirsResponseData.DirectoryData().setId(directoryId));
            for (Map.Entry<TopicIdPartition, Errors> partitionEntry : dirEntry.getValue().entrySet()) {
                TopicIdPartition topicPartition = partitionEntry.getKey();
                Errors error = partitionEntry.getValue();
                AssignReplicasToDirsResponseData.TopicData topic = topicMap.computeIfAbsent(directoryId, d -> new HashMap<>())
                        .computeIfAbsent(topicPartition.topicId(), topicId -> {
                            AssignReplicasToDirsResponseData.TopicData data = new AssignReplicasToDirsResponseData.TopicData().setTopicId(topicId);
                            directory.topics().add(data);
                            return data;
                        });
                AssignReplicasToDirsResponseData.PartitionData partition = new AssignReplicasToDirsResponseData.PartitionData()
                        .setPartitionIndex(topicPartition.partitionId()).setErrorCode(error.code());
                topic.partitions().add(partition);
            }
        }
        return new AssignReplicasToDirsResponseData()
                .setErrorCode(errorCode)
                .setThrottleTimeMs(throttleTimeMs)
                .setDirectories(new ArrayList<>(directoryMap.values()));
    }

    /**
     * Normalize the request data by sorting the directories, topics and partitions.
     * This is useful for comparing two semantically equivalent requests.
     */
    public static AssignReplicasToDirsRequestData normalize(AssignReplicasToDirsRequestData request) {
        request = request.duplicate();
        request.directories().sort(Comparator.comparing(AssignReplicasToDirsRequestData.DirectoryData::id));
        for (AssignReplicasToDirsRequestData.DirectoryData directory : request.directories()) {
            directory.topics().sort(Comparator.comparing(AssignReplicasToDirsRequestData.TopicData::topicId));
            for (AssignReplicasToDirsRequestData.TopicData topic : directory.topics()) {
                topic.partitions().sort(Comparator.comparing(AssignReplicasToDirsRequestData.PartitionData::partitionIndex));
            }
        }
        return request;
    }

    /**
     * Normalize the response data by sorting the directories, topics and partitions.
     * This is useful for comparing two semantically equivalent requests.
     */
    public static AssignReplicasToDirsResponseData normalize(AssignReplicasToDirsResponseData response) {
        response = response.duplicate();
        response.directories().sort(Comparator.comparing(AssignReplicasToDirsResponseData.DirectoryData::id));
        for (AssignReplicasToDirsResponseData.DirectoryData directory : response.directories()) {
            directory.topics().sort(Comparator.comparing(AssignReplicasToDirsResponseData.TopicData::topicId));
            for (AssignReplicasToDirsResponseData.TopicData topic : directory.topics()) {
                topic.partitions().sort(Comparator.comparing(AssignReplicasToDirsResponseData.PartitionData::partitionIndex));
            }
        }
        return response;
    }
}
