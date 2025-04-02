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

import kafka.cluster.Partition;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.OffsetNotAvailableException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.fetch.ShareFetchPartitionData;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import scala.Option;
import scala.Some;

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
            ShareFetch shareFetch,
            List<ShareFetchPartitionData> shareFetchPartitionDataList,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions,
            ReplicaManager replicaManager,
            BiConsumer<SharePartitionKey, Throwable> exceptionHandler
    ) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> response = new HashMap<>();

        // Acquired records count for the share fetch request.
        int acquiredRecordsCount = 0;
        for (ShareFetchPartitionData shareFetchPartitionData : shareFetchPartitionDataList) {
            TopicIdPartition topicIdPartition = shareFetchPartitionData.topicIdPartition();
            FetchPartitionData fetchPartitionData = shareFetchPartitionData.fetchPartitionData();

            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
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
                    try {
                        sharePartition.updateCacheAndOffsets(offsetForEarliestTimestamp(topicIdPartition,
                            replicaManager, sharePartition.leaderEpoch()));
                    } catch (Exception e) {
                        log.error("Error while fetching offset for earliest timestamp for topicIdPartition: {}", topicIdPartition, e);
                        shareFetch.addErroneous(topicIdPartition, e);
                        exceptionHandler.accept(new SharePartitionKey(shareFetch.groupId(), topicIdPartition), e);
                        // Do not fill the response for this partition and continue.
                        continue;
                    }
                    // We set the error code to NONE, as we have updated the start offset of the share partition
                    // and the client can retry the fetch.
                    partitionData.setErrorCode(Errors.NONE.code());
                    partitionData.setErrorMessage(Errors.NONE.message());
                }
            } else {
                ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(
                    shareFetch.memberId(),
                    shareFetch.batchSize(),
                    shareFetch.maxFetchRecords() - acquiredRecordsCount,
                    shareFetchPartitionData.fetchOffset(),
                    fetchPartitionData
                );
                log.trace("Acquired records: {} for topicIdPartition: {}", shareAcquiredRecords, topicIdPartition);
                // Maybe, in the future, check if no records are acquired, and we want to retry
                // replica manager fetch. Depends on the share partition manager implementation,
                // if we want parallel requests for the same share partition or not.
                if (shareAcquiredRecords.acquiredRecords().isEmpty()) {
                    partitionData
                        .setRecords(null)
                        .setAcquiredRecords(Collections.emptyList());
                } else {
                    partitionData
                        .setRecords(maybeSliceFetchRecords(fetchPartitionData.records, shareAcquiredRecords))
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
    static long offsetForEarliestTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager, int leaderEpoch) {
        // Isolation level is only required when reading from the latest offset hence use Option.empty() for now.
        Optional<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
                topicIdPartition.topicPartition(), ListOffsetsRequest.EARLIEST_TIMESTAMP, Option.empty(),
                Optional.of(leaderEpoch), true).timestampAndOffsetOpt();
        if (timestampAndOffset.isEmpty()) {
            throw new OffsetNotAvailableException("Offset for earliest timestamp not found for topic partition: " + topicIdPartition);
        }
        return timestampAndOffset.get().offset;
    }

    /**
     * The method is used to get the offset for the latest timestamp for the topic-partition.
     *
     * @return The offset for the latest timestamp.
     */
    static long offsetForLatestTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager, int leaderEpoch) {
        // Isolation level is set to READ_UNCOMMITTED, matching with that used in share fetch requests
        Optional<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
            topicIdPartition.topicPartition(), ListOffsetsRequest.LATEST_TIMESTAMP, new Some<>(IsolationLevel.READ_UNCOMMITTED),
            Optional.of(leaderEpoch), true).timestampAndOffsetOpt();
        if (timestampAndOffset.isEmpty()) {
            throw new OffsetNotAvailableException("Offset for latest timestamp not found for topic partition: " + topicIdPartition);
        }
        return timestampAndOffset.get().offset;
    }

    /**
     * The method is used to get the offset for the given timestamp for the topic-partition.
     *
     * @return The offset for the given timestamp.
     */
    static long offsetForTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager, long timestampToSearch, int leaderEpoch) {
        Optional<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
            topicIdPartition.topicPartition(), timestampToSearch, new Some<>(IsolationLevel.READ_UNCOMMITTED), Optional.of(leaderEpoch), true).timestampAndOffsetOpt();
        if (timestampAndOffset.isEmpty()) {
            throw new OffsetNotAvailableException("Offset for timestamp " + timestampToSearch + " not found for topic partition: " + topicIdPartition);
        }
        return timestampAndOffset.get().offset;
    }

    static int leaderEpoch(ReplicaManager replicaManager, TopicPartition tp) {
        return partition(replicaManager, tp).getLeaderEpoch();
    }

    static Partition partition(ReplicaManager replicaManager, TopicPartition tp) {
        Partition partition = replicaManager.getPartitionOrException(tp);
        if (!partition.isLeader()) {
            log.debug("The broker is not the leader for topic partition: {}-{}", tp.topic(), tp.partition());
            throw new NotLeaderOrFollowerException();
        }
        return partition;
    }

    /**
     * Slice the fetch records based on the acquired records. The slicing is done based on the first
     * and last offset of the acquired records from the list. The slicing doesn't consider individual
     * acquired batches rather the boundaries of the acquired list. The method expects the acquired
     * records list to be within the fetch records bounds.
     *
     * @param records The records to be sliced.
     * @param shareAcquiredRecords The share acquired records containing the non-empty acquired records.
     * @return The sliced records, if the records are of type FileRecords and the acquired records are a subset
     *        of the fetched records. Otherwise, the original records are returned.
     */
    static Records maybeSliceFetchRecords(Records records, ShareAcquiredRecords shareAcquiredRecords) {
        if (!(records instanceof FileRecords fileRecords)) {
            return records;
        }
        // The acquired records should be non-empty, do not check as the method is called only when the
        // acquired records are non-empty.
        List<AcquiredRecords> acquiredRecords = shareAcquiredRecords.acquiredRecords();
        try {
            final Iterator<FileChannelRecordBatch> iterator = fileRecords.batchIterator();
            // Track the first overlapping batch with the first acquired offset.
            FileChannelRecordBatch firstOverlapBatch = iterator.next();
            // If there exists single fetch batch, then return the original records.
            if (!iterator.hasNext()) {
                return records;
            }
            // Find the first and last acquired offset to slice the records.
            final long firstAcquiredOffset = acquiredRecords.get(0).firstOffset();
            final long lastAcquiredOffset = acquiredRecords.get(acquiredRecords.size() - 1).lastOffset();
            int startPosition = 0;
            int size = 0;
            // Start iterating from the second batch.
            while (iterator.hasNext()) {
                FileChannelRecordBatch batch = iterator.next();
                // Iterate until finds the first overlap batch with the first acquired offset. All the
                // batches before this first overlap batch should be sliced hence increment the start
                // position.
                if (batch.baseOffset() <= firstAcquiredOffset) {
                    startPosition += firstOverlapBatch.sizeInBytes();
                    firstOverlapBatch = batch;
                    continue;
                }
                // Break if traversed all the batches till the last acquired offset.
                if (batch.baseOffset() > lastAcquiredOffset) {
                    break;
                }
                size += batch.sizeInBytes();
            }
            // Include the first overlap batch as it's the last batch traversed which overlapped the first
            // acquired offset.
            size += firstOverlapBatch.sizeInBytes();
            // Check if we do not need slicing i.e. neither start position nor size changed.
            if (startPosition == 0 && size == fileRecords.sizeInBytes()) {
                return records;
            }
            return fileRecords.slice(startPosition, size);
        } catch (Exception e) {
            log.error("Error while checking batches for acquired records: {}, skipping slicing.", acquiredRecords, e);
            // If there is an exception while slicing, return the original records so that the fetch
            // can continue with the original records.
            return records;
        }
    }
}
