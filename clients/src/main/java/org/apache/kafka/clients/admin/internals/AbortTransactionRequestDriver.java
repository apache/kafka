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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class AbortTransactionRequestDriver extends MetadataRequestDriver<Void> {
    private final Logger log;
    private final AbortTransactionSpec abortSpec;
    private final TopicPartition topicPartition;

    public AbortTransactionRequestDriver(
        AbortTransactionSpec abortSpec,
        long deadlineMs,
        long retryBackoffMs,
        LogContext logContext
    ) {
        super(singleton(abortSpec.topicPartition()), deadlineMs, retryBackoffMs, logContext);
        this.abortSpec = abortSpec;
        this.topicPartition = abortSpec.topicPartition();
        this.log = logContext.logger(AbortTransactionRequestDriver.class);
    }

    @Override
    WriteTxnMarkersRequest.Builder buildFulfillmentRequest(Integer brokerId, Set<TopicPartition> topicPartitions) {
        if (!topicPartitions.equals(singleton(topicPartition))) {
            throw new IllegalArgumentException("Received unexpected topic partitions " + topicPartitions +
                " (expected " + singleton(topicPartition) + ")");
        }
        WriteTxnMarkersRequest.TxnMarkerEntry markerEntry = new WriteTxnMarkersRequest.TxnMarkerEntry(
            abortSpec.producerId(),
            (short) abortSpec.producerEpoch(),
            abortSpec.coordinatorEpoch(),
            TransactionResult.ABORT,
            singletonList(topicPartition)
        );
        return new WriteTxnMarkersRequest.Builder(singletonList(markerEntry));
    }

    @Override
    void handleFulfillmentResponse(Integer brokerId, Set<TopicPartition> keys, AbstractResponse abstractResponse) {
        WriteTxnMarkersResponse response = (WriteTxnMarkersResponse) abstractResponse;
        List<WriteTxnMarkersResponseData.WritableTxnMarkerResult> markerResponses = response.data.markers();

        if (markerResponses.size() != 1) {
            super.completeExceptionally(topicPartition, new KafkaException("WriteTxnMarkers response " +
                "included unexpected marker entries: " + markerResponses + "(expected to find exactly one " +
                "entry with producerId " + abortSpec.producerId() + ")"));
            return;
        }

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse = markerResponses.get(0);
        List<WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult> topicResponses = markerResponse.topics();

        if (topicResponses.size() != 1) {
            super.completeExceptionally(topicPartition, new KafkaException("WriteTxnMarkers response " +
                "included unexpected topic entries: " + markerResponses + "(expected to find exactly one " +
                "entry with topic partition " + topicPartition + ")"));
            return;
        }

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse = topicResponses.get(0);
        List<WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult> partitionResponses =
            topicResponse.partitions();

        if (partitionResponses.size() != 1) {
            super.completeExceptionally(topicPartition, new KafkaException("WriteTxnMarkers response " +
                "included unexpected partition entries for topic " + topicPartition.topic() +
                ": " + markerResponses + "(expected to find exactly one entry with partition " +
                topicPartition.partition() + ")"));
            return;
        }

        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse = partitionResponses.get(0);
        Errors error = Errors.forCode(partitionResponse.errorCode());

        if (error != Errors.NONE) {
            handleError(error);
        } else {
            super.complete(topicPartition, null);
        }
    }

    private void handleError(Errors error) {
        switch (error) {
            case CLUSTER_AUTHORIZATION_FAILED:
                super.completeExceptionally(topicPartition, new ClusterAuthorizationException(
                    "WriteTxnMarkers request with " + abortSpec + " failed due to cluster " +
                        "authorization error."));
                break;

            case TRANSACTION_COORDINATOR_FENCED:
                super.completeExceptionally(topicPartition, new TransactionCoordinatorFencedException(
                    "WriteTxnMarkers request with " + abortSpec + " failed since the provided " +
                        "coordinator epoch " + abortSpec.coordinatorEpoch() + " has been fenced " +
                        "by the active coordinator"));
                break;

            case NOT_LEADER_OR_FOLLOWER:
            case REPLICA_NOT_AVAILABLE:
            case BROKER_NOT_AVAILABLE:
            case UNKNOWN_TOPIC_OR_PARTITION:
                log.debug("WriteTxnMarkers request with {} failed due to {}. Will retry after backing off",
                    topicPartition, error);
                super.unmap(topicPartition);
                break;

            default:
                super.completeExceptionally(topicPartition, error.exception(
                    "WriteTxnMarkers request with " + abortSpec + " failed due to unexpected error"));
        }
    }

}
