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

package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.server.storage.log.FetchParams;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The ShareFetchData class is used to store the fetch parameters for a share fetch request.
 */
public class ShareFetchData {

    private final FetchParams fetchParams;
    private final String groupId;
    private final String memberId;
    private final CompletableFuture<Map<TopicIdPartition, PartitionData>> future;
    private final Map<TopicIdPartition, Integer> partitionMaxBytes;

    public ShareFetchData(
        FetchParams fetchParams,
        String groupId,
        String memberId,
        CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
        Map<TopicIdPartition, Integer> partitionMaxBytes
    ) {
        this.fetchParams = fetchParams;
        this.groupId = groupId;
        this.memberId = memberId;
        this.future = future;
        this.partitionMaxBytes = partitionMaxBytes;
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public CompletableFuture<Map<TopicIdPartition, PartitionData>> future() {
        return future;
    }

    public Map<TopicIdPartition, Integer> partitionMaxBytes() {
        return partitionMaxBytes;
    }

    public FetchParams fetchParams() {
        return fetchParams;
    }
}
