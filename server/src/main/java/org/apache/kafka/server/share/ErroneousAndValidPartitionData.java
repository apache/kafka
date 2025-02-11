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
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to return the erroneous partitions and valid partition data
 */
public class ErroneousAndValidPartitionData {
    private final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> erroneous;
    private final Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> validTopicIdPartitions;

    public ErroneousAndValidPartitionData(Map<TopicIdPartition, ShareFetchResponseData.PartitionData> erroneous,
                                          Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> validTopicIdPartitions) {
        this.erroneous = erroneous;
        this.validTopicIdPartitions = validTopicIdPartitions;
    }

    public ErroneousAndValidPartitionData(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
        erroneous = new HashMap<>();
        validTopicIdPartitions = new HashMap<>();
        shareFetchData.forEach((topicIdPartition, sharePartitionData) -> {
            if (topicIdPartition.topic() == null) {
                erroneous.put(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID));
            } else {
                validTopicIdPartitions.put(topicIdPartition, sharePartitionData);
            }
        });
    }

    public ErroneousAndValidPartitionData() {
        this.erroneous = new HashMap<>();
        this.validTopicIdPartitions = new HashMap<>();
    }

    public Map<TopicIdPartition, ShareFetchResponseData.PartitionData> erroneous() {
        return erroneous;
    }

    public Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> validTopicIdPartitions() {
        return validTopicIdPartitions;
    }
}
