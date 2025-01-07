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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * ShareSessionHandler maintains the share session state for connecting to a broker.
 *
 * <p>Using the protocol outlined by KIP-932, clients can create share sessions. These
 * sessions allow the client to fetch data from a set of share-partitions repeatedly,
 * without explicitly enumerating all the partitions in the request and response.
 *
 * <p>ShareSessionHandler tracks the partitions which are in the session. It also determines
 * which partitions need to be included in each ShareFetch/ShareAcknowledge request.
 */
public class ShareSessionHandler {
    private final Logger log;
    private final int node;
    private final Uuid memberId;

    /**
     * The metadata for the next ShareFetchRequest/ShareAcknowledgeRequest.
     */
    private ShareRequestMetadata nextMetadata;

    /**
     * All the partitions in the share session.
     */
    private final LinkedHashMap<TopicPartition, TopicIdPartition> sessionPartitions;

    /*
     * The partitions to be included in the next ShareFetch request.
     */
    private LinkedHashMap<TopicPartition, TopicIdPartition> nextPartitions;

    /*
     * The acknowledgements to be included in the next ShareFetch/ShareAcknowledge request.
     */
    private LinkedHashMap<TopicIdPartition, Acknowledgements> nextAcknowledgements;

    public ShareSessionHandler(LogContext logContext, int node, Uuid memberId) {
        this.log = logContext.logger(ShareSessionHandler.class);
        this.node = node;
        this.memberId = memberId;
        this.nextMetadata = ShareRequestMetadata.initialEpoch(memberId);
        this.sessionPartitions = new LinkedHashMap<>();
        this.nextPartitions = new LinkedHashMap<>();
        this.nextAcknowledgements = new LinkedHashMap<>();
    }

    Map<TopicPartition, TopicIdPartition> sessionPartitionMap() {
        return sessionPartitions;
    }

    public Collection<TopicIdPartition> sessionPartitions() {
        return Collections.unmodifiableCollection(sessionPartitions.values());
    }

    public void addPartitionToFetch(TopicIdPartition topicIdPartition, Acknowledgements partitionAcknowledgements) {
        nextPartitions.put(topicIdPartition.topicPartition(), topicIdPartition);
        if (partitionAcknowledgements != null) {
            nextAcknowledgements.put(topicIdPartition, partitionAcknowledgements);
        }
    }

    public ShareFetchRequest.Builder newShareFetchBuilder(String groupId, FetchConfig fetchConfig) {
        List<TopicIdPartition> added = new ArrayList<>();
        List<TopicIdPartition> removed = new ArrayList<>();
        List<TopicIdPartition> replaced = new ArrayList<>();

        if (nextMetadata.isNewSession()) {
            // Add any new partitions to the session
            for (Entry<TopicPartition, TopicIdPartition> entry : nextPartitions.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition topicIdPartition = entry.getValue();
                sessionPartitions.put(topicPartition, topicIdPartition);
            }

            // If it's a new session, all the partitions must be added to the request
            added.addAll(sessionPartitions.values());
        } else {
            // Iterate over the session partitions, tallying which were added
            Iterator<Entry<TopicPartition, TopicIdPartition>> partitionIterator = sessionPartitions.entrySet().iterator();
            while (partitionIterator.hasNext()) {
                Entry<TopicPartition, TopicIdPartition> entry = partitionIterator.next();
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition prevData = entry.getValue();
                TopicIdPartition nextData = nextPartitions.remove(topicPartition);
                if (nextData != null) {
                    // If the topic ID does not match, the topic has been recreated
                    if (!prevData.equals(nextData)) {
                        nextPartitions.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        replaced.add(prevData);
                    }
                } else {
                    // This partition is not in the builder, so we need to remove it from the session
                    partitionIterator.remove();
                    removed.add(prevData);
                }
            }

            // Add any new partitions to the session
            for (Entry<TopicPartition, TopicIdPartition> entry : nextPartitions.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition topicIdPartition = entry.getValue();
                sessionPartitions.put(topicPartition, topicIdPartition);
                added.add(topicIdPartition);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Build ShareFetch {} for node {}. Added {}, removed {}, replaced {} out of {}",
                    nextMetadata, node,
                    topicIdPartitionsToLogString(added),
                    topicIdPartitionsToLogString(removed),
                    topicIdPartitionsToLogString(replaced),
                    topicIdPartitionsToLogString(sessionPartitions.values()));
        }

        // The replaced topic-partitions need to be removed, and their replacements are already added
        removed.addAll(replaced);

        Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgementBatches = new HashMap<>();
        nextAcknowledgements.forEach((partition, acknowledgements) -> acknowledgementBatches.put(partition, acknowledgements.getAcknowledgementBatches()
                .stream().map(AcknowledgementBatch::toShareFetchRequest)
                .collect(Collectors.toList())));

        nextPartitions = new LinkedHashMap<>();
        nextAcknowledgements = new LinkedHashMap<>();

        return ShareFetchRequest.Builder.forConsumer(
                groupId, nextMetadata, fetchConfig.maxWaitMs,
                fetchConfig.minBytes, fetchConfig.maxBytes, fetchConfig.fetchSize,
                added, removed, acknowledgementBatches);
    }

    public ShareAcknowledgeRequest.Builder newShareAcknowledgeBuilder(String groupId, FetchConfig fetchConfig) {
        if (nextMetadata.isNewSession()) {
            // A share session cannot be started with a ShareAcknowledge request
            nextPartitions.clear();
            nextAcknowledgements.clear();
            return null;
        }

        Map<TopicIdPartition, List<ShareAcknowledgeRequestData.AcknowledgementBatch>> acknowledgementBatches = new HashMap<>();
        nextAcknowledgements.forEach((partition, acknowledgements) ->
                acknowledgementBatches.put(partition, acknowledgements.getAcknowledgementBatches()
                        .stream().map(AcknowledgementBatch::toShareAcknowledgeRequest)
                        .collect(Collectors.toList())));

        nextAcknowledgements = new LinkedHashMap<>();

        return ShareAcknowledgeRequest.Builder.forConsumer(groupId, nextMetadata, acknowledgementBatches);
    }

    private String topicIdPartitionsToLogString(Collection<TopicIdPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + partitions.stream().map(TopicIdPartition::toString).collect(Collectors.joining(", ")) + ")";
    }

    /**
     * Handle the ShareFetch response.
     *
     * @param response  The response.
     * @param version   The version of the request.
     * @return          True if the response is well-formed; false if it can't be processed
     *                  because of missing or unexpected partitions.
     */
    public boolean handleResponse(ShareFetchResponse response, short version) {
        if ((response.error() == Errors.SHARE_SESSION_NOT_FOUND) ||
                (response.error() == Errors.INVALID_SHARE_SESSION_EPOCH)) {
            log.info("Node {} was unable to process the ShareFetch request with {}: {}.",
                    node, nextMetadata, response.error());
            nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
            return false;
        }

        if (response.error() != Errors.NONE) {
            log.info("Node {} was unable to process the ShareFetch request with {}: {}.",
                    node, nextMetadata, response.error());
            nextMetadata = nextMetadata.nextEpoch();
            return false;
        }

        // The share session was continued by the server
        if (log.isDebugEnabled())
            log.debug("Node {} sent a ShareFetch response with throttleTimeMs = {} " +
                    "for session {}", node, response.throttleTimeMs(), memberId);
        nextMetadata = nextMetadata.nextEpoch();
        return true;
    }

    /**
     * Handle the ShareAcknowledge response.
     *
     * @param response  The response.
     * @param version   The version of the request.
     * @return          True if the response is well-formed; false if it can't be processed
     *                  because of missing or unexpected partitions.
     */
    public boolean handleResponse(ShareAcknowledgeResponse response, short version) {
        if ((response.error() == Errors.SHARE_SESSION_NOT_FOUND) ||
                (response.error() == Errors.INVALID_SHARE_SESSION_EPOCH)) {
            log.info("Node {} was unable to process the ShareAcknowledge request with {}: {}.",
                    node, nextMetadata, response.error());
            nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
            return false;
        }

        if (response.error() != Errors.NONE) {
            log.info("Node {} was unable to process the ShareAcknowledge request with {}: {}.",
                    node, nextMetadata, response.error());
            nextMetadata = nextMetadata.nextEpoch();
            return false;
        }

        // The share session was continued by the server
        if (log.isDebugEnabled())
            log.debug("Node {} sent a ShareAcknowledge response with throttleTimeMs = {} " +
                    "for session {}", node, response.throttleTimeMs(), memberId);
        nextMetadata = nextMetadata.nextEpoch();
        return true;
    }

    /**
     * The client will initiate the session close on next ShareFetch request.
     */
    public void notifyClose() {
        log.debug("Set the metadata for next ShareFetch request to close the share session memberId={}",
                nextMetadata.memberId());
        nextMetadata = nextMetadata.finalEpoch();
    }

    /**
     * Handle an error sending the prepared request.
     * When a network error occurs, we close any existing share session on our next request,
     * and try to create a new session.
     *
     * @param t     The exception.
     */
    public void handleError(Throwable t) {
        log.info("Error sending fetch request {} to node {}:", nextMetadata, node, t);
        nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
    }
}
