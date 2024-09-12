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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;

import java.util.Objects;
import java.util.Optional;

/**
 * A cached share partition. The broker maintains a set of these objects for each share session.
 * <p>
 * We store many of these objects, so it is important for them to be memory-efficient.
 * That is why we store topic and partition separately rather than storing a TopicPartition
 * object. The TP object takes up more memory because it is a separate JVM object, and
 * because it stores the cached hash code in memory.
 */
public class CachedSharePartition implements ImplicitLinkedHashCollection.Element {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final Optional<Integer> leaderEpoch;
    private int maxBytes;
    private boolean requiresUpdateInResponse;

    private int cachedNext = ImplicitLinkedHashCollection.INVALID_INDEX;
    private int cachedPrev = ImplicitLinkedHashCollection.INVALID_INDEX;

    private CachedSharePartition(String topic, Uuid topicId, int partition, int maxBytes, Optional<Integer> leaderEpoch,
                                 boolean requiresUpdateInResponse) {
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.maxBytes = maxBytes;
        this.leaderEpoch = leaderEpoch;
        this.requiresUpdateInResponse = requiresUpdateInResponse;
    }

    public CachedSharePartition(String topic, Uuid topicId, int partition, boolean requiresUpdateInResponse) {
        this(topic, topicId, partition, -1, Optional.empty(), requiresUpdateInResponse);
    }

    public CachedSharePartition(TopicIdPartition topicIdPartition) {
        this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition(), false);
    }

    public CachedSharePartition(TopicIdPartition topicIdPartition, ShareFetchRequest.SharePartitionData reqData,
                                boolean requiresUpdateInResponse) {
        this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition(), reqData.maxBytes,
                Optional.empty(), requiresUpdateInResponse);
    }

    public Uuid topicId() {
        return topicId;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public ShareFetchRequest.SharePartitionData reqData() {
        return new ShareFetchRequest.SharePartitionData(topicId, maxBytes);
    }

    public void updateRequestParams(ShareFetchRequest.SharePartitionData reqData) {
        // Update our cached request parameters.
        maxBytes = reqData.maxBytes;
    }

    /**
     * Determine whether the specified cached partition should be included in the ShareFetchResponse we send back to
     * the fetcher and update it if requested.
     * This function should be called while holding the appropriate session lock.
     *
     * @param respData partition data
     * @param updateResponseData if set to true, update this CachedSharePartition with new request and response data.
     * @return True if this partition should be included in the response; false if it can be omitted.
     */
    public boolean maybeUpdateResponseData(ShareFetchResponseData.PartitionData respData, boolean updateResponseData) {
        boolean mustRespond = false;
        // Check the response data
        // Partitions with new data are always included in the response.
        if (ShareFetchResponse.recordsSize(respData) > 0)
            mustRespond = true;
        if (requiresUpdateInResponse) {
            mustRespond = true;
            if (updateResponseData)
                requiresUpdateInResponse = false;
        }
        if (respData.errorCode() != Errors.NONE.code()) {
            // Partitions with errors are always included in the response.
            // We also set the cached requiresUpdateInResponse to false.
            // This ensures that when the error goes away, we re-send the partition.
            if (updateResponseData)
                requiresUpdateInResponse = true;
            mustRespond = true;
        }
        return mustRespond;
    }

    public String toString() {
        return  "CachedSharePartition(topic=" + topic +
                ", topicId=" + topicId +
                ", partition=" + partition +
                ", maxBytes=" + maxBytes +
                ", leaderEpoch=" + leaderEpoch +
                ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, topicId);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        else if (obj == null || getClass() != obj.getClass())
            return false;
        else {
            CachedSharePartition that = (CachedSharePartition) obj;
            return partition == that.partition && Objects.equals(topicId, that.topicId);
        }
    }

    @Override
    public int prev() {
        return cachedPrev;
    }

    @Override
    public void setPrev(int prev) {
        cachedPrev = prev;
    }

    @Override
    public int next() {
        return cachedNext;
    }

    @Override
    public void setNext(int next) {
        cachedNext = next;
    }
}
