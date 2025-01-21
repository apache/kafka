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
package org.apache.kafka.server.share.context;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.server.share.ErroneousAndValidPartitionData;
import org.apache.kafka.server.share.session.ShareSession;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * The context for every share fetch request. The context is responsible for tracking the topic partitions present in
 * the share fetch request and generating the response data.
 */
public abstract class ShareFetchContext {

    /**
     *
     * @param partitions - The partitions requested in the fetch request.
     * @return - A string representation of the partitions requested.
     */
    String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return ShareSession.partitionsToLogString(partitions, isTraceEnabled());
    }

    /**
     * Return an empty throttled response due to quota violation.
     * @param throttleTimeMs - The time to throttle the response.
     * @return - An empty throttled response.
     */
    public ShareFetchResponse throttleResponse(int throttleTimeMs) {
        return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, throttleTimeMs,
                Collections.emptyIterator(), Collections.emptyList()));
    }

    /**
     * @return - Whether trace logging is enabled.
     */
    abstract boolean isTraceEnabled();

    /**
     * Get the response size to be used for quota computation. Since we are returning an empty response in case of
     * throttling, we are not supposed to update the context until we know that we are not going to throttle.
     * @param updates - The updates to be sent in the response.
     * @param version - The version of the share fetch request.
     * @return - The size of the response.
     */
    public abstract int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                              short version);

    /**
     * Updates the share fetch context with new partition information. Generates response data.
     * The response data may require subsequent down-conversion.
     * @param groupId - The group id.
     * @param memberId - The member id.
     * @param updates - The updates to be sent in the response.
     * @return - The share fetch response.
     */
    public abstract ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId, LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates);

    /**
     * @return - The error-prone and valid topic id partitions in the share fetch request.
     */
    public abstract ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions();

}
