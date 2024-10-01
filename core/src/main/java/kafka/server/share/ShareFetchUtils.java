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
package kafka.server.share;

import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.ShareFetchData;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import scala.Option;

/**
 * Utility class for post-processing of share fetch operations.
 */
public class ShareFetchUtils {
    private static final Logger log = LoggerFactory.getLogger(ShareFetchUtils.class);

    // Process the replica manager fetch response to update share partitions and futures. We acquire the fetched data
    // from share partitions.
    static CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> processFetchResponse(
            ShareFetchData shareFetchData,
            Map<TopicIdPartition, FetchPartitionData> responseData,
            Map<SharePartitionKey, SharePartition> partitionCacheMap,
            ReplicaManager replicaManager
    ) {
        Map<TopicIdPartition, CompletableFuture<ShareFetchResponseData.PartitionData>> futures = new HashMap<>();
        responseData.forEach((topicIdPartition, fetchPartitionData) -> {

            SharePartition sharePartition = partitionCacheMap.get(new SharePartitionKey(
                shareFetchData.groupId(), topicIdPartition));
            futures.put(topicIdPartition, sharePartition.acquire(shareFetchData.memberId(), fetchPartitionData)
                    .handle((acquiredRecords, throwable) -> {
                        log.trace("Acquired records for topicIdPartition: {} with share fetch data: {}, records: {}",
                                topicIdPartition, shareFetchData, acquiredRecords);
                        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                                .setPartitionIndex(topicIdPartition.partition());

                        if (throwable != null) {
                            partitionData.setErrorCode(Errors.forException(throwable).code());
                            return partitionData;
                        }

                        if (fetchPartitionData.error.code() == Errors.OFFSET_OUT_OF_RANGE.code()) {
                            // In case we get OFFSET_OUT_OF_RANGE error, that's because the Log Start Offset is later than the fetch offset.
                            // So, we would update the start and end offset of the share partition and still return an empty
                            // response and let the client retry the fetch. This way we do not lose out on the data that
                            // would be returned for other share partitions in the fetch request.
                            sharePartition.updateCacheAndOffsets(offsetForEarliestTimestamp(topicIdPartition, replicaManager));
                            partitionData.setPartitionIndex(topicIdPartition.partition())
                                    .setRecords(null)
                                    .setErrorCode(Errors.NONE.code())
                                    .setAcquiredRecords(Collections.emptyList())
                                    .setAcknowledgeErrorCode(Errors.NONE.code());
                            return partitionData;
                        }

                        // Maybe, in the future, check if no records are acquired, and we want to retry
                        // replica manager fetch. Depends on the share partition manager implementation,
                        // if we want parallel requests for the same share partition or not.
                        partitionData.setPartitionIndex(topicIdPartition.partition())
                                .setRecords(fetchPartitionData.records)
                                .setErrorCode(fetchPartitionData.error.code())
                                .setAcquiredRecords(acquiredRecords)
                                .setAcknowledgeErrorCode(Errors.NONE.code());
                        return partitionData;
                    }));
        });
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).thenApply(v -> {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> processedResult = new HashMap<>();
            futures.forEach((topicIdPartition, future) -> processedResult.put(topicIdPartition, future.join()));
            return processedResult;
        });
    }

    /**
     * The method is used to get the offset for the earliest timestamp for the topic-partition.
     *
     * @return The offset for the earliest timestamp.
     */
    static long offsetForEarliestTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager) {
        // Isolation level is only required when reading from the latest offset hence use Option.empty() for now.
        Option<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
                topicIdPartition.topicPartition(), ListOffsetsRequest.EARLIEST_TIMESTAMP, Option.empty(),
                Optional.empty(), true).timestampAndOffsetOpt();
        return timestampAndOffset.isEmpty() ? (long) 0 : timestampAndOffset.get().offset;
    }
}
