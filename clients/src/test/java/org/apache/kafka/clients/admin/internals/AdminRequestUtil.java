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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.utils.CollectionUtils;

import java.util.Map;

public class AdminRequestUtil {

    private AdminRequestUtil() {}

    public static MetadataResponseData metadataResponse(
        Map<TopicPartition, MetadataResponsePartition> partitionResponses
    ) {
        MetadataResponseData response = new MetadataResponseData();

        Map<String, Map<Integer, MetadataResponsePartition>> partitionResponsesByTopic =
            CollectionUtils.groupPartitionDataByTopic(partitionResponses);

        for (Map.Entry<String, Map<Integer, MetadataResponsePartition>> entry : partitionResponsesByTopic.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, MetadataResponsePartition> partitionResponseMap = entry.getValue();

            MetadataResponseTopic topicResponse = new MetadataResponseTopic();
            response.topics().add(topicResponse.setName(topic));

            for (Map.Entry<Integer, MetadataResponsePartition> partitionEntry : partitionResponseMap.entrySet()) {
                Integer partitionId = partitionEntry.getKey();
                MetadataResponsePartition partitionResponse = partitionEntry.getValue();
                topicResponse.partitions().add(partitionResponse.setPartitionIndex(partitionId));
            }
        }

        return response;
    }
}
