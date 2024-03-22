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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
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
import java.util.Set;

/**
 * ShareSessionHandler maintains the share session state for connecting to a broker.
 *
 * <p>Using the protocol outlined by KIP-932, clients can create share sessions. These
 * sessions allow the client to fetch data from a set of share-partitions repeatedly,
 * without explicitly enumerating all the partitions in the request and response.
 *
 * <p>ShareSessionHandler tracks the partitions which are in the session. It also determines
 * which partitions need to be included in each ShareFetch request.
 */
public class ShareSessionHandler {
    private final Logger log;
    private final int node;
    private final Uuid memberId;

    /**
     * The metadata for the next ShareFetchRequest.
     */
    private ShareFetchMetadata nextMetadata;

    /**
     * All the partitions in the share session.
     */
    private LinkedHashMap<TopicPartition, TopicIdPartition> sessionPartitions;

    /**
     * All the topic names mapped to topic ids for topics which exist in the fetch request session.
     */
    private Map<Uuid, String> sessionTopicNames = new HashMap<>(0);
    private final Map<TopicIdPartition, Acknowledgements> nextAcknowledgements = new LinkedHashMap<>();

    public Map<Uuid, String> sessionTopicNames() {
        return sessionTopicNames;
    }

    public Set<TopicPartition> sessionTopics() {
        return sessionPartitions.keySet();
    }

    public ShareSessionHandler(LogContext logContext, int node, Uuid memberId) {
        this.log = logContext.logger(ShareSessionHandler.class);
        this.node = node;
        this.memberId = memberId;
        this.nextMetadata = ShareFetchMetadata.initialEpoch(memberId);
        this.sessionPartitions = new LinkedHashMap<>();
    }

    public static class ShareFetchRequestData {

        /**
         * The partitions to send in the ShareFetch request.
         */
        private final Map<TopicPartition, TopicIdPartition> toSend;

        /**
         * The partitions to send in the ShareFetch "forget" list.
         */
        private final List<TopicIdPartition> toForget;

        /**
         * The partitions whose TopicIds have changed due to topic recreation.
         */
        private final List<TopicIdPartition> toReplace;

        /**
         * All the partitions which exist in the fetch request session.
         */
        private final Map<TopicPartition, TopicIdPartition> sessionPartitions;

        /**
         * The metadata to use in this fetch request.
         */
        private final ShareFetchMetadata metadata;

        /**
         * A map of the acknowledgements to send.
         */
        private final Map<TopicIdPartition, Acknowledgements> acknowledgements;

        ShareFetchRequestData(Map<TopicPartition, TopicIdPartition> toSend,
                              List<TopicIdPartition> toForget,
                              List<TopicIdPartition> toReplace,
                              Map<TopicPartition, TopicIdPartition> sessionPartitions,
                              Map<TopicIdPartition, Acknowledgements> acknowledgements,
                              ShareFetchMetadata metadata) {
            this.toSend = toSend;
            this.toForget = toForget;
            this.toReplace = toReplace;
            this.sessionPartitions = sessionPartitions;
            this.metadata = metadata;
            this.acknowledgements = acknowledgements;
        }

        public Map<TopicPartition, TopicIdPartition> toSend() {
            return toSend;
        }

        public List<TopicIdPartition> toForget() {
            return toForget;
        }

        public List<TopicIdPartition> toReplace() {
            return toReplace;
        }

        public Map<TopicPartition, TopicIdPartition> sessionPartitions() {
            return sessionPartitions;
        }

        public Map<TopicIdPartition, Acknowledgements> acknowledgements() {
            return acknowledgements;
        }

        public ShareFetchMetadata metadata() {
            return metadata;
        }
    }

    public class Builder {

        private LinkedHashMap<TopicPartition, TopicIdPartition> next;
        private Map<Uuid, String> topicNames;

        Builder() {
            this.next = new LinkedHashMap<>();
            this.topicNames = new HashMap<>();
        }

        public void add(TopicIdPartition topicIdPartition, Acknowledgements partitionAcknowledgements) {
            next.put(topicIdPartition.topicPartition(), topicIdPartition);
            topicNames.putIfAbsent(topicIdPartition.topicId(), topicIdPartition.topic());
            if (partitionAcknowledgements != null) {
                nextAcknowledgements.put(topicIdPartition, partitionAcknowledgements);
            } else {
                nextAcknowledgements.remove(topicIdPartition);
            }
        }

        public ShareFetchRequestData build() {
            if (nextMetadata.isNewSession()) {
                sessionPartitions = next;
                next = null;
                sessionTopicNames = topicNames;
                Map<TopicPartition, TopicIdPartition> toSend =
                        Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
                return new ShareFetchRequestData(toSend, Collections.emptyList(), Collections.emptyList(),
                        sessionPartitions, nextAcknowledgements, nextMetadata);
            }

            List<TopicIdPartition> added = new ArrayList<>();
            List<TopicIdPartition> removed = new ArrayList<>();
            List<TopicIdPartition> replaced = new ArrayList<>();

            // Iterate over the session partitions, tallying which were added to the builder
            Iterator<Entry<TopicPartition, TopicIdPartition>> partitionIterator =
                    sessionPartitions.entrySet().iterator();
            while (partitionIterator.hasNext()) {
                Entry<TopicPartition, TopicIdPartition> entry = partitionIterator.next();
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition prevData = entry.getValue();
                TopicIdPartition nextData = next.remove(topicPartition);
                if (nextData != null) {
                    // If the topic ID does not match, the topic has been recreated
                    if (!prevData.equals(nextData)) {
                        next.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        replaced.add(prevData);
                    }
                } else {
                    // This partition not in the builder, so we need to remove it from the session
                    partitionIterator.remove();
                    removed.add(prevData);
                }
            }

            // Add any new partitions to the session
            for (Entry<TopicPartition, TopicIdPartition> entry : next.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition topicIdPartition = entry.getValue();
                sessionPartitions.put(topicPartition, topicIdPartition);
                added.add(topicIdPartition);
            }

            sessionTopicNames = topicNames;

            if (log.isDebugEnabled()) {
                log.debug("Build ShareFetch {} for node {}. Added {}, removed {}, replaced {} out of {}",
                        nextMetadata, node,
                        topicIdPartitionsToLogString(added),
                        topicIdPartitionsToLogString(removed),
                        topicIdPartitionsToLogString(replaced),
                        topicIdPartitionsToLogString(sessionPartitions.values()));
            }
            // Temporarily the broker is sessionless so we need to repeat all topic-partitions on every request
//            Map<TopicPartition, TopicIdPartition> toSend = Collections.unmodifiableMap(next);
            Map<TopicPartition, TopicIdPartition> curSessionPartitions = Collections.unmodifiableMap(sessionPartitions);
            next = null;
//            return new ShareFetchRequestData(toSend, removed, replaced, curSessionPartitions, nextMetadata);
            return new ShareFetchRequestData(curSessionPartitions, removed, replaced, curSessionPartitions, nextAcknowledgements, nextMetadata);
        }
    }

    Builder newBuilder() {
        return new Builder();
    }

    private String topicIdPartitionsToLogString(Collection<TopicIdPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + Utils.join(partitions, ", ") + ")";
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
            log.info("Node {} was unable to process the ShareFetch request with {}: {}.",
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
     * When a network error occurs, we close any existing fetch session on our next request,
     * and try to create a new session.
     *
     * @param t     The exception.
     */
    public void handleError(Throwable t) {
        log.info("Error sending fetch request {} to node {}:", nextMetadata, node, t);
        nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
    }
}