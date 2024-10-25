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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class AbortTransactionHandler extends AdminApiHandler.Batched<TopicPartition, Void> {
    private final Logger log;
    private final AbortTransactionSpec abortSpec;
    private final PartitionLeaderStrategy lookupStrategy;

    public AbortTransactionHandler(
        AbortTransactionSpec abortSpec,
        LogContext logContext
    ) {
        this.abortSpec = abortSpec;
        this.log = logContext.logger(AbortTransactionHandler.class);
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
    }

    public static PartitionLeaderStrategy.PartitionLeaderFuture<Void> newFuture(
        Set<TopicPartition> topicPartitions,
        Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(topicPartitions, partitionLeaderCache);
    }

    @Override
    public String apiName() {
        return "abortTransaction";
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public WriteTxnMarkersRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<TopicPartition> topicPartitions
    ) {
        validateTopicPartitions(topicPartitions);

        WriteTxnMarkersRequestData.WritableTxnMarker marker = new WriteTxnMarkersRequestData.WritableTxnMarker()
            .setCoordinatorEpoch(abortSpec.coordinatorEpoch())
            .setProducerEpoch(abortSpec.producerEpoch())
            .setProducerId(abortSpec.producerId())
            .setTransactionResult(false);

        marker.topics().add(new WriteTxnMarkersRequestData.WritableTxnMarkerTopic()
            .setName(abortSpec.topicPartition().topic())
            .setPartitionIndexes(singletonList(abortSpec.topicPartition().partition()))
        );

        WriteTxnMarkersRequestData request = new WriteTxnMarkersRequestData();
        request.markers().add(marker);

        return new WriteTxnMarkersRequest.Builder(request);
    }

    @Override
    public ApiResult<TopicPartition, Void> handleResponse(
        Node broker,
        Set<TopicPartition> topicPartitions,
        AbstractResponse abstractResponse
    ) {
        validateTopicPartitions(topicPartitions);

        WriteTxnMarkersResponse response = (WriteTxnMarkersResponse) abstractResponse;
        List<WriteTxnMarkersResponseData.WritableTxnMarkerResult> markerResponses = response.data().markers();

        if (markerResponses.size() != 1 || markerResponses.get(0).producerId() != abortSpec.producerId()) {
            return ApiResult.failed(abortSpec.topicPartition(), new KafkaException("WriteTxnMarkers response " +
                "included unexpected marker entries: " + markerResponses + "(expected to find exactly one " +
                "entry with producerId " + abortSpec.producerId() + ")"));
        }

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse = markerResponses.get(0);
        List<WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult> topicResponses = markerResponse.topics();

        if (topicResponses.size() != 1 || !topicResponses.get(0).name().equals(abortSpec.topicPartition().topic())) {
            return ApiResult.failed(abortSpec.topicPartition(), new KafkaException("WriteTxnMarkers response " +
                "included unexpected topic entries: " + markerResponses + "(expected to find exactly one " +
                "entry with topic partition " + abortSpec.topicPartition() + ")"));
        }

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse = topicResponses.get(0);
        List<WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult> partitionResponses =
            topicResponse.partitions();

        if (partitionResponses.size() != 1 || partitionResponses.get(0).partitionIndex() != abortSpec.topicPartition().partition()) {
            return ApiResult.failed(abortSpec.topicPartition(), new KafkaException("WriteTxnMarkers response " +
                "included unexpected partition entries for topic " + abortSpec.topicPartition().topic() +
                ": " + markerResponses + "(expected to find exactly one entry with partition " +
                abortSpec.topicPartition().partition() + ")"));
        }

        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse = partitionResponses.get(0);
        Errors error = Errors.forCode(partitionResponse.errorCode());

        if (error != Errors.NONE) {
            return handleError(error);
        } else {
            return ApiResult.completed(abortSpec.topicPartition(), null);
        }
    }

    private ApiResult<TopicPartition, Void> handleError(Errors error) {
        switch (error) {
            case CLUSTER_AUTHORIZATION_FAILED:
                log.error("WriteTxnMarkers request for abort spec {} failed cluster authorization", abortSpec);
                return ApiResult.failed(abortSpec.topicPartition(), new ClusterAuthorizationException(
                    "WriteTxnMarkers request with " + abortSpec + " failed due to cluster " +
                        "authorization error"));

            case INVALID_PRODUCER_EPOCH:
                log.error("WriteTxnMarkers request for abort spec {} failed due to an invalid producer epoch",
                    abortSpec);
                return ApiResult.failed(abortSpec.topicPartition(), new InvalidProducerEpochException(
                    "WriteTxnMarkers request with " + abortSpec + " failed due an invalid producer epoch"));

            case TRANSACTION_COORDINATOR_FENCED:
                log.error("WriteTxnMarkers request for abort spec {} failed because the coordinator epoch is fenced",
                    abortSpec);
                return ApiResult.failed(abortSpec.topicPartition(), new TransactionCoordinatorFencedException(
                    "WriteTxnMarkers request with " + abortSpec + " failed since the provided " +
                        "coordinator epoch " + abortSpec.coordinatorEpoch() + " has been fenced " +
                        "by the active coordinator"));

            case NOT_LEADER_OR_FOLLOWER:
            case REPLICA_NOT_AVAILABLE:
            case BROKER_NOT_AVAILABLE:
            case UNKNOWN_TOPIC_OR_PARTITION:
                log.debug("WriteTxnMarkers request for abort spec {} failed due to {}. Will retry after attempting to " +
                        "find the leader again", abortSpec, error);
                return ApiResult.unmapped(singletonList(abortSpec.topicPartition()));

            default:
                log.error("WriteTxnMarkers request for abort spec {} failed due to an unexpected error {}",
                    abortSpec, error);
                return ApiResult.failed(abortSpec.topicPartition(), error.exception(
                    "WriteTxnMarkers request with " + abortSpec + " failed due to unexpected error: " + error.message()));
        }
    }

    private void validateTopicPartitions(Set<TopicPartition> topicPartitions) {
        if (!topicPartitions.equals(singleton(abortSpec.topicPartition()))) {
            throw new IllegalArgumentException("Received unexpected topic partitions " + topicPartitions +
                " (expected only " + singleton(abortSpec.topicPartition()) + ")");
        }
    }

}
