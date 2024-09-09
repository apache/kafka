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

import kafka.server.DelayedOperation;
import kafka.server.DelayedOperationPurgatory;
import kafka.server.LogReadResult;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.storage.internals.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

/**
 * A delayed share fetch operation has been introduced in case there is no share partition for which we can acquire records. We will try to wait
 * for MaxWaitMs for records to be released else complete the share fetch request.
 */
public class DelayedShareFetch extends DelayedOperation {
    private final SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData;
    private final ReplicaManager replicaManager;
    private final Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap;
    private Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionDataFromTryComplete = new LinkedHashMap<>();
    private final DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory;

    private static final Logger log = LoggerFactory.getLogger(DelayedShareFetch.class);

    DelayedShareFetch(
            SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData,
            ReplicaManager replicaManager,
            Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap,
            DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory) {
        super(shareFetchPartitionData.fetchParams().maxWaitMs, Option.empty());
        this.shareFetchPartitionData = shareFetchPartitionData;
        this.replicaManager = replicaManager;
        this.partitionCacheMap = partitionCacheMap;
        this.delayedShareFetchPurgatory = delayedShareFetchPurgatory;
    }

    @Override
    public void onExpiration() {
    }

    /**
     * Complete the share fetch operation by fetching records for all partitions in the share fetch request irrespective
     * of whether they have any acquired records. This is called when the fetch operation is forced to complete either
     * because records can be acquired for some partitions or due to MaxWaitMs timeout.
     */
    @Override
    public void onComplete() {
        log.trace("Completing the delayed share fetch request for group {}, member {}, " +
                        "topic partitions {}", shareFetchPartitionData.groupId(),
                shareFetchPartitionData.memberId(), shareFetchPartitionData.partitionMaxBytes().keySet());

        if (shareFetchPartitionData.future().isDone())
            return;

        Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData;
        // tryComplete did not invoke forceComplete, so we need to check if we have any partitions to fetch.
        if (topicPartitionDataFromTryComplete.isEmpty())
            topicPartitionData = acquirablePartitions();
        // tryComplete invoked forceComplete, so we can use the data from tryComplete.
        else
            topicPartitionData = topicPartitionDataFromTryComplete;
        try {
            if (topicPartitionData.isEmpty()) {
                // No locks for share partitions could be acquired, so we complete the request with an empty response.
                shareFetchPartitionData.future().complete(Collections.emptyMap());
                return;
            }
            log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
                    topicPartitionData, shareFetchPartitionData.groupId(), shareFetchPartitionData.fetchParams());

            Seq<Tuple2<TopicIdPartition, LogReadResult>> responseLogResult = replicaManager.readFromLog(
                shareFetchPartitionData.fetchParams(),
                CollectionConverters.asScala(
                    topicPartitionData.entrySet().stream().map(entry ->
                        new Tuple2<>(entry.getKey(), entry.getValue())).collect(Collectors.toList())
                ),
                QuotaFactory.UnboundedQuota$.MODULE$,
                true);

            List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
            responseLogResult.foreach(tpLogResult -> {
                TopicIdPartition topicIdPartition = tpLogResult._1();
                LogReadResult logResult = tpLogResult._2();
                FetchPartitionData fetchPartitionData = logResult.toFetchPartitionData(false);
                responseData.add(new Tuple2<>(topicIdPartition, fetchPartitionData));
                return BoxedUnit.UNIT;
            });

            log.trace("Data successfully retrieved by replica manager: {}", responseData);
            ShareFetchUtils.processFetchResponse(shareFetchPartitionData, responseData, partitionCacheMap, replicaManager)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Error processing fetch response for share partitions", throwable);
                        shareFetchPartitionData.future().completeExceptionally(throwable);
                    } else {
                        shareFetchPartitionData.future().complete(result);
                    }
                    // Releasing the lock to move ahead with the next request in queue.
                    releasePartitionsLock(shareFetchPartitionData.groupId(), topicPartitionData.keySet());
                    // If we have a fetch request completed for a topic-partition, it means  the HWM has advanced,
                    // then we should check if there is a pending share fetch request for the topic-partition and complete it.
                    result.keySet().forEach(topicIdPartition -> delayedShareFetchPurgatory.checkAndComplete(
                            new DelayedShareFetchKey(topicIdPartition, shareFetchPartitionData.groupId())));
                });

        } catch (Exception e) {
            // Release the locks acquired for the partitions in the share fetch request in case there is an exception
            log.error("Error processing delayed share fetch request", e);
            shareFetchPartitionData.future().completeExceptionally(e);
            releasePartitionsLock(shareFetchPartitionData.groupId(), topicPartitionData.keySet());
        }
    }

    /**
     * Try to complete the fetch operation if we can acquire records for any partition in the share fetch request.
     */
    @Override
    public boolean tryComplete() {
        log.trace("Try to complete the delayed share fetch request for group {}, member {}, topic partitions {}",
                shareFetchPartitionData.groupId(), shareFetchPartitionData.memberId(),
                shareFetchPartitionData.partitionMaxBytes().keySet());

        topicPartitionDataFromTryComplete = acquirablePartitions();

        if (!topicPartitionDataFromTryComplete.isEmpty())
            return forceComplete();
        log.info("Can't acquire records for any partition in the share fetch request for group {}, member {}, " +
                "topic partitions {}", shareFetchPartitionData.groupId(),
                shareFetchPartitionData.memberId(), shareFetchPartitionData.partitionMaxBytes().keySet());
        return false;
    }

    /**
     * Prepare fetch request structure for partitions in the share fetch request for which we can acquire records.
     */
    // Visible for testing
    Map<TopicIdPartition, FetchRequest.PartitionData> acquirablePartitions() {
        // Initialize the topic partitions for which the fetch should be attempted.
        Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();

        shareFetchPartitionData.partitionMaxBytes().keySet().forEach(topicIdPartition -> {
            SharePartition sharePartition = partitionCacheMap.get(new SharePartitionManager.SharePartitionKey(
                    shareFetchPartitionData.groupId(), topicIdPartition));

            int partitionMaxBytes = shareFetchPartitionData.partitionMaxBytes().getOrDefault(topicIdPartition, 0);
            // Add the share partition to the list of partitions to be fetched only if we can
            // acquire the fetch lock on it.
            if (sharePartition.maybeAcquireFetchLock()) {
                // If the share partition is already at capacity, we should not attempt to fetch.
                if (sharePartition.canAcquireRecords()) {
                    topicPartitionData.put(
                            topicIdPartition,
                            new FetchRequest.PartitionData(
                                    topicIdPartition.topicId(),
                                    sharePartition.nextFetchOffset(),
                                    0,
                                    partitionMaxBytes,
                                    Optional.empty()
                            )
                    );
                } else {
                    sharePartition.releaseFetchLock();
                    log.trace("Record lock partition limit exceeded for SharePartition {}, " +
                            "cannot acquire more records", sharePartition);
                }
            }
        });
        return topicPartitionData;
    }

    private void releasePartitionsLock(String groupId, Set<TopicIdPartition> topicIdPartitions) {
        topicIdPartitions.forEach(tp -> partitionCacheMap.get(new
                SharePartitionManager.SharePartitionKey(groupId, tp)).releaseFetchLock());
    }
}
