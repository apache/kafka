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

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SharePartitionManager {
    private final ReplicaManager replicaManager;
    private HashMap<SharePartitionKey, SharePartition> sharePartitions;

    public SharePartitionManager(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
        sharePartitions = new HashMap<>();
    }

    public CompletableFuture<ShareFetchResponseData> fetchMessages(ShareSession session, PartitionInfo partitionInfo) {
      assert replicaManager != null;
      throw new UnsupportedOperationException("Not implemented yet");
    }

    public CompletableFuture<ShareAcknowledgeResponseData> acknowledge(ShareSession session, PartitionInfo partitionInfo) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private SharePartition getSharePartition(String groupId, Uuid topicId, int partition) {
        SharePartitionKey sharePartitionKey = new SharePartitionKey(groupId, topicId, partition);
        if (!sharePartitions.containsKey(sharePartitionKey))
            //TODO: This line needs to be changed for constructor of SharePartition
            sharePartitions.put(sharePartitionKey, new SharePartition());
        return sharePartitions.get(sharePartitionKey);
    }

    public ShareSession getSession() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public ShareFetchContext newContext(Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> forgottenTopics,
                                        Map<Uuid, String> topicNames) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    // TODO: Define share session class.
    public static class ShareSession {

    }
    /**
     * The share fetch context for a sessionless share fetch request.
     */
    public static class SessionlessShareFetchContext extends ShareFetchContext {
        private final Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        public SessionlessShareFetchContext(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.shareFetchData = shareFetchData;
        }

        @Override
        int getResponseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates, short version) {
            return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Sessionless fetch context returning" + partitionsToLogString(updates.keySet()));
            return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> getCachedPartitions() {
            return shareFetchData;
        }
    }

    /**
     * The fetch context for a full share fetch request.
     */
    // TODO: Implement FullShareFetchContext when you have share sessions available
    public static class FullShareFetchContext extends ShareFetchContext {

        @Override
        int getResponseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> getCachedPartitions() {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    /**
     * The share fetch context for a share fetch request that had a session error.
     */
    // TODO: Implement ShareSessionErrorContext when you have share sessions available
    public static class ShareSessionErrorContext extends ShareFetchContext {

        @Override
        int getResponseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> getCachedPartitions() {
            return new HashMap<>();
        }
    }

    /**
     * The share fetch context for an incremental share fetch request.
     */
    // TODO: Implement IncrementalFetchContext when you have share sessions available
    public static class IncrementalFetchContext extends ShareFetchContext {

        @Override
        int getResponseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> getCachedPartitions() {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }
}
