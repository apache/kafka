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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShareAcknowledgeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ShareAcknowledgeRequest> {

        private final ShareAcknowledgeRequestData data;

        public Builder(ShareAcknowledgeRequestData data) {
            this(data, false);
        }

        public Builder(ShareAcknowledgeRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.SHARE_ACKNOWLEDGE, enableUnstableLastVersion);
            this.data = data;
        }

        public static ShareAcknowledgeRequest.Builder forConsumer(String groupId, ShareRequestMetadata metadata,
                                                                  Map<TopicIdPartition, List<ShareAcknowledgeRequestData.AcknowledgementBatch>> acknowledgementsMap) {
            ShareAcknowledgeRequestData data = new ShareAcknowledgeRequestData();
            data.setGroupId(groupId);
            if (metadata != null) {
                data.setMemberId(metadata.memberId().toString());
                data.setShareSessionEpoch(metadata.epoch());
            }

            // Build a map of topics to acknowledge keyed by topic ID, and within each a map of partitions keyed by index
            Map<Uuid, Map<Integer, ShareAcknowledgeRequestData.AcknowledgePartition>> ackMap = new HashMap<>();

            for (Map.Entry<TopicIdPartition, List<ShareAcknowledgeRequestData.AcknowledgementBatch>> acknowledgeEntry : acknowledgementsMap.entrySet()) {
                TopicIdPartition tip = acknowledgeEntry.getKey();
                Map<Integer, ShareAcknowledgeRequestData.AcknowledgePartition> partMap = ackMap.computeIfAbsent(tip.topicId(), k -> new HashMap<>());
                ShareAcknowledgeRequestData.AcknowledgePartition ackPartition = partMap.get(tip.partition());
                if (ackPartition == null) {
                    ackPartition = new ShareAcknowledgeRequestData.AcknowledgePartition()
                            .setPartitionIndex(tip.partition());
                    partMap.put(tip.partition(), ackPartition);
                }
                ackPartition.setAcknowledgementBatches(acknowledgeEntry.getValue());
            }

            // Finally, build up the data to fetch
            data.setTopics(new ArrayList<>());
            ackMap.forEach((topicId, partMap) -> {
                ShareAcknowledgeRequestData.AcknowledgeTopic ackTopic = new ShareAcknowledgeRequestData.AcknowledgeTopic()
                        .setTopicId(topicId)
                        .setPartitions(new ArrayList<>());
                data.topics().add(ackTopic);

                partMap.forEach((index, ackPartition) -> ackTopic.partitions().add(ackPartition));
            });

            return new ShareAcknowledgeRequest.Builder(data, true);
        }

        public ShareAcknowledgeRequestData data() {
            return data;
        }

        @Override
        public ShareAcknowledgeRequest build(short version) {
            return new ShareAcknowledgeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ShareAcknowledgeRequestData data;

    public ShareAcknowledgeRequest(ShareAcknowledgeRequestData data, short version) {
        super(ApiKeys.SHARE_ACKNOWLEDGE, version);
        this.data = data;
    }

    @Override
    public ShareAcknowledgeRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ShareAcknowledgeResponse(new ShareAcknowledgeResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static ShareAcknowledgeRequest parse(Readable readable, short version) {
        return new ShareAcknowledgeRequest(
                new ShareAcknowledgeRequestData(readable, version),
                version
        );
    }
}