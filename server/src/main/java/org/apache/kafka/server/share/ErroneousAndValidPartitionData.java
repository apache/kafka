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

package org.apache.kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to return the erroneous partitions and valid partition data
 */
public class ErroneousAndValidPartitionData {
    private final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> erroneous;
    private final List<TopicIdPartition> validTopicIdPartitions;

    public ErroneousAndValidPartitionData(Map<TopicIdPartition, ShareFetchResponseData.PartitionData> erroneous,
                                          List<TopicIdPartition> validTopicIdPartitions) {
        this.erroneous = erroneous;
        this.validTopicIdPartitions = validTopicIdPartitions;
    }

    public ErroneousAndValidPartitionData(List<TopicIdPartition> shareFetchData) {
        erroneous = new HashMap<>();
        validTopicIdPartitions = new ArrayList<>();
        shareFetchData.forEach(topicIdPartition -> {
            if (topicIdPartition.topic() == null) {
                erroneous.put(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID));
            } else {
                validTopicIdPartitions.add(topicIdPartition);
            }
        });
    }

    public ErroneousAndValidPartitionData() {
        this.erroneous = Map.of();
        this.validTopicIdPartitions = List.of();
    }

    public Map<TopicIdPartition, ShareFetchResponseData.PartitionData> erroneous() {
        return erroneous;
    }

    public List<TopicIdPartition> validTopicIdPartitions() {
        return validTopicIdPartitions;
    }
}
