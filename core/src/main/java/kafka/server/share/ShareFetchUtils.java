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
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetchData;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import scala.Option;

/**
 * Utility class for post-processing of share fetch operations.
 */
public class ShareFetchUtils {
    private static final Logger log = LoggerFactory.getLogger(ShareFetchUtils.class);

    /**
     * Process the replica manager fetch response to create share fetch response. The response is created
     * by acquiring records from the share partition.
     */
    static Map<TopicIdPartition, ShareFetchResponseData.PartitionData> processFetchResponse(
            ShareFetchData shareFetchData,
            Map<TopicIdPartition, FetchPartitionData> responseData,
            SharePartitionManager sharePartitionManager,
            ReplicaManager replicaManager
    ) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> response = new HashMap<>();

        // Acquired records count for the share fetch request.
        int acquiredRecordsCount = 0;
        for (Map.Entry<TopicIdPartition, FetchPartitionData> entry : responseData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            FetchPartitionData fetchPartitionData = entry.getValue();

            SharePartition sharePartition = sharePartitionManager.sharePartition(shareFetchData.groupId(), topicIdPartition);
            if (sharePartition == null) {
                log.error("Encountered null share partition for groupId={}, topicIdPartition={}. Skipping it.", shareFetchData.groupId(), topicIdPartition);
                continue;
            }
            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(topicIdPartition.partition());

            if (fetchPartitionData.error.code() != Errors.NONE.code()) {
                partitionData
                    .setRecords(null)
                    .setErrorCode(fetchPartitionData.error.code())
                    .setErrorMessage(fetchPartitionData.error.message())
                    .setAcquiredRecords(Collections.emptyList());

                // In case we get OFFSET_OUT_OF_RANGE error, that's because the Log Start Offset is later than the fetch offset.
                // So, we would update the start and end offset of the share partition and still return an empty
                // response and let the client retry the fetch. This way we do not lose out on the data that
                // would be returned for other share partitions in the fetch request.
                if (fetchPartitionData.error.code() == Errors.OFFSET_OUT_OF_RANGE.code()) {
                    sharePartition.updateCacheAndOffsets(offsetForEarliestTimestamp(topicIdPartition, replicaManager));
                    // We set the error code to NONE, as we have updated the start offset of the share partition
                    // and the client can retry the fetch.
                    partitionData.setErrorCode(Errors.NONE.code());
                    partitionData.setErrorMessage(Errors.NONE.message());
                }
            } else {
                ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(shareFetchData.memberId(), shareFetchData.maxFetchRecords() - acquiredRecordsCount, fetchPartitionData);
                log.trace("Acquired records for topicIdPartition: {} with share fetch data: {}, records: {}",
                    topicIdPartition, shareFetchData, shareAcquiredRecords);
                // Maybe, in the future, check if no records are acquired, and we want to retry
                // replica manager fetch. Depends on the share partition manager implementation,
                // if we want parallel requests for the same share partition or not.
                if (shareAcquiredRecords.acquiredRecords().isEmpty()) {
                    partitionData
                        .setRecords(null)
                        .setAcquiredRecords(Collections.emptyList());
                } else {
                    partitionData
                        // We set the records to the fetchPartitionData records. We do not alter the records
                        // fetched from the replica manager as they follow zero copy buffer. The acquired records
                        // might be a subset of the records fetched from the replica manager, depending
                        // on the max fetch records or available records in the share partition. The client
                        // sends the max bytes in request which should limit the bytes sent to the client
                        // in the response.
                        .setRecords(fetchPartitionData.records)
                        .setAcquiredRecords(shareAcquiredRecords.acquiredRecords());
                    acquiredRecordsCount += shareAcquiredRecords.count();
                }
            }
            response.put(topicIdPartition, partitionData);
        }
        return response;
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
