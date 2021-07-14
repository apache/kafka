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

package org.apache.kafka.clients;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * FetchSessionHandler maintains the fetch session state for connecting to a broker.
 *
 * Using the protocol outlined by KIP-227, clients can create incremental fetch sessions.
 * These sessions allow the client to fetch information about a set of partition over
 * and over, without explicitly enumerating all the partitions in the request and the
 * response.
 *
 * FetchSessionHandler tracks the partitions which are in the session.  It also
 * determines which partitions need to be included in each fetch request, and what
 * the attached fetch session metadata should be for each request.  The corresponding
 * class on the receiving broker side is FetchManager.
 */
public class FetchSessionHandler {
    private final Logger log;

    private final int node;

    /**
     * The metadata for the next fetch request.
     */
    private FetchMetadata nextMetadata = FetchMetadata.INITIAL;

    public FetchSessionHandler(LogContext logContext, int node) {
        this.log = logContext.logger(FetchSessionHandler.class);
        this.node = node;
    }

    /**
     * All of the partitions which exist in the fetch request session.
     */
    private LinkedHashMap<TopicPartition, PartitionData> sessionPartitions =
        new LinkedHashMap<>(0);

    /**
     * All of the topic ids mapped to topic names for topics which exist in the fetch request session.
     */
    private Map<String, Uuid> sessionTopicIds = new HashMap<>(0);

    /**
     * All of the topic names mapped to topic ids for topics which exist in the fetch request session.
     */
    private Map<Uuid, String> sessionTopicNames = new HashMap<>(0);

    public Map<Uuid, String> sessionTopicNames() {
        return sessionTopicNames;
    }

    public static class FetchRequestData {
        /**
         * The partitions to send in the fetch request.
         */
        private final Map<TopicPartition, PartitionData> toSend;

        /**
         * The partitions to send in the request's "forget" list.
         */
        private final List<TopicPartition> toForget;

        /**
         * All of the partitions which exist in the fetch request session.
         */
        private final Map<TopicPartition, PartitionData> sessionPartitions;

        /**
         * All of the topic IDs for topics which exist in the fetch request session.
         */
        private final Map<String, Uuid> topicIds;

        /**
         * All of the topic IDs for topics which exist in the fetch request session
         */
        private final Map<Uuid, String> topicNames;

        /**
         * The metadata to use in this fetch request.
         */
        private final FetchMetadata metadata;

        /**
         * A boolean indicating whether we have a topic ID for every topic in the request so that we can send a request that
         * uses topic IDs
         */
        private final boolean canUseTopicIds;

        FetchRequestData(Map<TopicPartition, PartitionData> toSend,
                         List<TopicPartition> toForget,
                         Map<TopicPartition, PartitionData> sessionPartitions,
                         Map<String, Uuid> topicIds,
                         Map<Uuid, String> topicNames,
                         FetchMetadata metadata,
                         boolean canUseTopicIds) {
            this.toSend = toSend;
            this.toForget = toForget;
            this.sessionPartitions = sessionPartitions;
            this.topicIds = topicIds;
            this.topicNames = topicNames;
            this.metadata = metadata;
            this.canUseTopicIds = canUseTopicIds;
        }

        /**
         * Get the set of partitions to send in this fetch request.
         */
        public Map<TopicPartition, PartitionData> toSend() {
            return toSend;
        }

        /**
         * Get a list of partitions to forget in this fetch request.
         */
        public List<TopicPartition> toForget() {
            return toForget;
        }

        /**
         * Get the full set of partitions involved in this fetch request.
         */
        public Map<TopicPartition, PartitionData> sessionPartitions() {
            return sessionPartitions;
        }

        public Map<String, Uuid> topicIds() {
            return topicIds;
        }

        public Map<Uuid, String> topicNames() {
            return topicNames;
        }

        public FetchMetadata metadata() {
            return metadata;
        }

        public boolean canUseTopicIds() {
            return canUseTopicIds;
        }

        @Override
        public String toString() {
            StringBuilder bld;
            if (metadata.isFull()) {
                bld = new StringBuilder("FullFetchRequest(toSend=(");
                String prefix = "";
                for (TopicPartition partition : toSend.keySet()) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
            } else {
                bld = new StringBuilder("IncrementalFetchRequest(toSend=(");
                String prefix = "";
                for (TopicPartition partition : toSend.keySet()) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), toForget=(");
                prefix = "";
                for (TopicPartition partition : toForget) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), implied=(");
                prefix = "";
                for (TopicPartition partition : sessionPartitions.keySet()) {
                    if (!toSend.containsKey(partition)) {
                        bld.append(prefix);
                        bld.append(partition);
                        prefix = ", ";
                    }
                }
            }
            bld.append("), topicIds=(");
            String prefix = "";
            for (Map.Entry<String, Uuid> entry : topicIds.entrySet()) {
                bld.append(prefix);
                bld.append(entry.getKey());
                bld.append(": ");
                bld.append(entry.getValue());
                prefix = ", ";
            }
            if (canUseTopicIds) {
                bld.append("), canUseTopicIds=True");
            } else {
                bld.append("), canUseTopicIds=False");
            }
            bld.append(")");
            return bld.toString();
        }
    }

    public class Builder {
        /**
         * The next partitions which we want to fetch.
         *
         * It is important to maintain the insertion order of this list by using a LinkedHashMap rather
         * than a regular Map.
         *
         * One reason is that when dealing with FULL fetch requests, if there is not enough response
         * space to return data from all partitions, the server will only return data from partitions
         * early in this list.
         *
         * Another reason is because we make use of the list ordering to optimize the preparation of
         * incremental fetch requests (see below).
         */
        private LinkedHashMap<TopicPartition, PartitionData> next;
        private Map<String, Uuid> topicIds;
        private final boolean copySessionPartitions;
        private int partitionsWithoutTopicIds = 0;

        Builder() {
            this.next = new LinkedHashMap<>();
            this.topicIds = new HashMap<>();
            this.copySessionPartitions = true;
        }

        Builder(int initialSize, boolean copySessionPartitions) {
            this.next = new LinkedHashMap<>(initialSize);
            this.topicIds = new HashMap<>(initialSize);
            this.copySessionPartitions = copySessionPartitions;
        }

        /**
         * Mark that we want data from this partition in the upcoming fetch.
         */
        public void add(TopicPartition topicPartition, Uuid topicId, PartitionData data) {
            next.put(topicPartition, data);
            // topicIds should not change between adding partitions and building, so we can use putIfAbsent
            if (!topicId.equals(Uuid.ZERO_UUID)) {
                topicIds.putIfAbsent(topicPartition.topic(), topicId);
            } else {
                partitionsWithoutTopicIds++;
            }
        }

        public FetchRequestData build() {
            // For incremental sessions we don't have to worry about removed partitions when using topic IDs because we will either
            //   a) already be using topic IDs and have the ID in the session
            //   b) not be using topic IDs before and will close the session upon trying to use them
            boolean canUseTopicIds = partitionsWithoutTopicIds == 0;

            if (nextMetadata.isFull()) {
                if (log.isDebugEnabled()) {
                    log.debug("Built full fetch {} for node {} with {}.",
                              nextMetadata, node, partitionsToLogString(next.keySet()));
                }
                sessionPartitions = next;
                next = null;
                // Only add topic IDs to the session if we are using topic IDs.
                if (canUseTopicIds) {
                    sessionTopicIds = topicIds;
                    sessionTopicNames = new HashMap<>(topicIds.size());
                    topicIds.forEach((name, id) -> sessionTopicNames.put(id, name));
                } else {
                    sessionTopicIds = new HashMap<>();
                    sessionTopicNames = new HashMap<>();
                }
                topicIds = null;
                Map<TopicPartition, PartitionData> toSend =
                    Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
                Map<String, Uuid> toSendTopicIds =
                    Collections.unmodifiableMap(new HashMap<>(sessionTopicIds));
                Map<Uuid, String> toSendTopicNames =
                    Collections.unmodifiableMap(new HashMap<>(sessionTopicNames));
                return new FetchRequestData(toSend, Collections.emptyList(), toSend, toSendTopicIds, toSendTopicNames, nextMetadata, canUseTopicIds);
            }

            List<TopicPartition> added = new ArrayList<>();
            List<TopicPartition> removed = new ArrayList<>();
            List<TopicPartition> altered = new ArrayList<>();
            for (Iterator<Entry<TopicPartition, PartitionData>> iter =
                     sessionPartitions.entrySet().iterator(); iter.hasNext(); ) {
                Entry<TopicPartition, PartitionData> entry = iter.next();
                TopicPartition topicPartition = entry.getKey();
                PartitionData prevData = entry.getValue();
                PartitionData nextData = next.remove(topicPartition);
                if (nextData != null) {
                    if (!prevData.equals(nextData)) {
                        // Re-add the altered partition to the end of 'next'
                        next.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        altered.add(topicPartition);
                    }
                } else {
                    // Remove this partition from the session.
                    iter.remove();
                    // Indicate that we no longer want to listen to this partition.
                    removed.add(topicPartition);
                }
            }
            // Add any new partitions to the session.
            for (Entry<TopicPartition, PartitionData> entry : next.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                PartitionData nextData = entry.getValue();
                if (sessionPartitions.containsKey(topicPartition)) {
                    // In the previous loop, all the partitions which existed in both sessionPartitions
                    // and next were moved to the end of next, or removed from next.  Therefore,
                    // once we hit one of them, we know there are no more unseen entries to look
                    // at in next.
                    break;
                }
                sessionPartitions.put(topicPartition, nextData);
                added.add(topicPartition);
            }

            // Add topic IDs to session if we can use them. If an ID is inconsistent, we will handle in the receiving broker.
            // If we switched from using topic IDs to not using them (or vice versa), that error will also be handled in the receiving broker.
            if (canUseTopicIds) {
                for (Map.Entry<String, Uuid> topic : topicIds.entrySet()) {
                    String topicName = topic.getKey();
                    Uuid addedId = topic.getValue();
                    sessionTopicIds.put(topicName, addedId);
                    sessionTopicNames.put(addedId, topicName);
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Built incremental fetch {} for node {}. Added {}, altered {}, removed {} " +
                          "out of {}", nextMetadata, node, partitionsToLogString(added),
                          partitionsToLogString(altered), partitionsToLogString(removed),
                          partitionsToLogString(sessionPartitions.keySet()));
            }
            Map<TopicPartition, PartitionData> toSend = Collections.unmodifiableMap(next);
            Map<TopicPartition, PartitionData> curSessionPartitions = copySessionPartitions
                    ? Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions))
                    : Collections.unmodifiableMap(sessionPartitions);
            Map<String, Uuid> toSendTopicIds =
                Collections.unmodifiableMap(new HashMap<>(sessionTopicIds));
            Map<Uuid, String> toSendTopicNames =
                Collections.unmodifiableMap(new HashMap<>(sessionTopicNames));
            next = null;
            topicIds = null;
            return new FetchRequestData(toSend, Collections.unmodifiableList(removed), curSessionPartitions,
                                        toSendTopicIds, toSendTopicNames, nextMetadata, canUseTopicIds);
        }
    }

    public Builder newBuilder() {
        return new Builder();
    }


    /** A builder that allows for presizing the PartitionData hashmap, and avoiding making a
     *  secondary copy of the sessionPartitions, in cases where this is not necessarily.
     *  This builder is primarily for use by the Replica Fetcher
     * @param size the initial size of the PartitionData hashmap
     * @param copySessionPartitions boolean denoting whether the builder should make a deep copy of
     *                              session partitions
     */
    public Builder newBuilder(int size, boolean copySessionPartitions) {
        return new Builder(size, copySessionPartitions);
    }

    private String partitionsToLogString(Collection<TopicPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + Utils.join(partitions, ", ") + ")";
    }

    /**
     * Return missing items which are expected to be in a particular set, but which are not.
     *
     * @param toFind    The items to look for.
     * @param toSearch  The set of items to search.
     * @return          null if all items were found; some of the missing ones in a set, if not.
     */
    static <T> Set<T> findMissing(Set<T> toFind, Set<T> toSearch) {
        Set<T> ret = new LinkedHashSet<>();
        for (T toFindItem: toFind) {
            if (!toSearch.contains(toFindItem)) {
                ret.add(toFindItem);
            }
        }
        return ret;
    }

    /**
     * Verify that a full fetch response contains all the partitions in the fetch session.
     *
     * @param topicPartitions  The topicPartitions from the FetchResponse.
     * @param ids              The topic IDs from the FetchResponse.
     * @param version          The version of the FetchResponse.
     * @return                 True if the full fetch response partitions are valid.
     */
    String verifyFullFetchResponsePartitions(Set<TopicPartition> topicPartitions, Set<Uuid> ids, short version) {
        StringBuilder bld = new StringBuilder();
        Set<TopicPartition> extra =
            findMissing(topicPartitions, sessionPartitions.keySet());
        Set<TopicPartition> omitted =
            findMissing(sessionPartitions.keySet(), topicPartitions);
        Set<Uuid> extraIds = new HashSet<>();
        if (version >= 13) {
            extraIds = findMissing(ids, sessionTopicNames.keySet());
        }
        if (!omitted.isEmpty()) {
            bld.append("omittedPartitions=(").append(Utils.join(omitted, ", ")).append(", ");
        }
        if (!extra.isEmpty()) {
            bld.append("extraPartitions=(").append(Utils.join(extra, ", ")).append(", ");
        }
        if (!extraIds.isEmpty()) {
            bld.append("extraIds=(").append(Utils.join(extraIds, ", ")).append(", ");
        }
        if ((!omitted.isEmpty()) || (!extra.isEmpty()) || (!extraIds.isEmpty())) {
            bld.append("response=(").append(Utils.join(topicPartitions, ", ")).append(")");
            return bld.toString();
        }
        return null;
    }

    /**
     * Verify that the partitions in an incremental fetch response are contained in the session.
     *
     * @param topicPartitions  The topicPartitions from the FetchResponse.
     * @param ids              The topic IDs from the FetchResponse.
     * @param version          The version of the FetchResponse.
     * @return                 True if the incremental fetch response partitions are valid.
     */
    String verifyIncrementalFetchResponsePartitions(Set<TopicPartition> topicPartitions, Set<Uuid> ids, short version) {
        Set<Uuid> extraIds = new HashSet<>();
        if (version >= 13) {
            extraIds = findMissing(ids, sessionTopicNames.keySet());
        }
        Set<TopicPartition> extra =
            findMissing(topicPartitions, sessionPartitions.keySet());
        StringBuilder bld = new StringBuilder();
        if (extra.isEmpty())
            bld.append("extraPartitions=(").append(Utils.join(extra, ", ")).append("), ");
        if (extraIds.isEmpty())
            bld.append("extraIds=(").append(Utils.join(extraIds, ", ")).append("), ");
        if ((!extra.isEmpty()) || (!extraIds.isEmpty())) {
            bld.append("response=(").append(Utils.join(topicPartitions, ", ")).append(")");
            return bld.toString();
        }
        return null;
    }

    /**
     * Create a string describing the partitions in a FetchResponse.
     *
     * @param topicPartitions  The topicPartitions from the FetchResponse.
     * @return                 The string to log.
     */
    private String responseDataToLogString(Set<TopicPartition> topicPartitions) {
        if (!log.isTraceEnabled()) {
            int implied = sessionPartitions.size() - topicPartitions.size();
            if (implied > 0) {
                return String.format(" with %d response partition(s), %d implied partition(s)",
                    topicPartitions.size(), implied);
            } else {
                return String.format(" with %d response partition(s)",
                    topicPartitions.size());
            }
        }
        StringBuilder bld = new StringBuilder();
        bld.append(" with response=(").
            append(Utils.join(topicPartitions, ", ")).
            append(")");
        String prefix = ", implied=(";
        String suffix = "";
        for (TopicPartition partition : sessionPartitions.keySet()) {
            if (!topicPartitions.contains(partition)) {
                bld.append(prefix);
                bld.append(partition);
                prefix = ", ";
                suffix = ")";
            }
        }
        bld.append(suffix);
        return bld.toString();
    }

    /**
     * Handle the fetch response.
     *
     * @param response  The response.
     * @param version   The version of the request.
     * @return          True if the response is well-formed; false if it can't be processed
     *                  because of missing or unexpected partitions.
     */
    public boolean handleResponse(FetchResponse response, short version) {
        if (response.error() != Errors.NONE) {
            log.info("Node {} was unable to process the fetch request with {}: {}.",
                node, nextMetadata, response.error());
            if (response.error() == Errors.FETCH_SESSION_ID_NOT_FOUND) {
                nextMetadata = FetchMetadata.INITIAL;
            } else {
                nextMetadata = nextMetadata.nextCloseExisting();
            }
            return false;
        }
        Set<TopicPartition> topicPartitions = response.responseData(sessionTopicNames, version).keySet();
        if (nextMetadata.isFull()) {
            if (topicPartitions.isEmpty() && response.throttleTimeMs() > 0) {
                // Normally, an empty full fetch response would be invalid.  However, KIP-219
                // specifies that if the broker wants to throttle the client, it will respond
                // to a full fetch request with an empty response and a throttleTimeMs
                // value set.  We don't want to log this with a warning, since it's not an error.
                // However, the empty full fetch response can't be processed, so it's still appropriate
                // to return false here.
                if (log.isDebugEnabled()) {
                    log.debug("Node {} sent a empty full fetch response to indicate that this " +
                        "client should be throttled for {} ms.", node, response.throttleTimeMs());
                }
                nextMetadata = FetchMetadata.INITIAL;
                return false;
            }
            String problem = verifyFullFetchResponsePartitions(topicPartitions, response.topicIds(), version);
            if (problem != null) {
                log.info("Node {} sent an invalid full fetch response with {}", node, problem);
                nextMetadata = FetchMetadata.INITIAL;
                return false;
            } else if (response.sessionId() == INVALID_SESSION_ID) {
                if (log.isDebugEnabled())
                    log.debug("Node {} sent a full fetch response{}", node, responseDataToLogString(topicPartitions));
                nextMetadata = FetchMetadata.INITIAL;
                return true;
            } else {
                // The server created a new incremental fetch session.
                if (log.isDebugEnabled())
                    log.debug("Node {} sent a full fetch response that created a new incremental " +
                            "fetch session {}{}", node, response.sessionId(), responseDataToLogString(topicPartitions));
                nextMetadata = FetchMetadata.newIncremental(response.sessionId());
                return true;
            }
        } else {
            String problem = verifyIncrementalFetchResponsePartitions(topicPartitions, response.topicIds(), version);
            if (problem != null) {
                log.info("Node {} sent an invalid incremental fetch response with {}", node, problem);
                nextMetadata = nextMetadata.nextCloseExisting();
                return false;
            } else if (response.sessionId() == INVALID_SESSION_ID) {
                // The incremental fetch session was closed by the server.
                if (log.isDebugEnabled())
                    log.debug("Node {} sent an incremental fetch response closing session {}{}",
                            node, nextMetadata.sessionId(), responseDataToLogString(topicPartitions));
                nextMetadata = FetchMetadata.INITIAL;
                return true;
            } else {
                // The incremental fetch session was continued by the server.
                // We don't have to do anything special here to support KIP-219, since an empty incremental
                // fetch request is perfectly valid.
                if (log.isDebugEnabled())
                    log.debug("Node {} sent an incremental fetch response with throttleTimeMs = {} " +
                        "for session {}{}", node, response.throttleTimeMs(), response.sessionId(),
                        responseDataToLogString(topicPartitions));
                nextMetadata = nextMetadata.nextIncremental();
                return true;
            }
        }
    }

    /**
     * Handle an error sending the prepared request.
     *
     * When a network error occurs, we close any existing fetch session on our next request,
     * and try to create a new session.
     *
     * @param t     The exception.
     */
    public void handleError(Throwable t) {
        log.info("Error sending fetch request {} to node {}:", nextMetadata, node, t);
        nextMetadata = nextMetadata.nextCloseExisting();
    }
}
