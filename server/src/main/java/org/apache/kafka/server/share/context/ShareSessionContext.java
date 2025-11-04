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

package org.apache.kafka.server.share.context;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ErroneousAndValidPartitionData;
import org.apache.kafka.server.share.session.ShareSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * The context for a share session fetch request.
 */
public class ShareSessionContext extends ShareFetchContext {

    private static final Logger log = LoggerFactory.getLogger(ShareSessionContext.class);

    private final int epoch;
    private final boolean isSubsequent;
    private List<TopicIdPartition> shareFetchData;
    private ShareSession session;

    /**
     * The share fetch context for the first request that starts a share session.
     *
     * @param epoch              The epoch of share session received in the share fetch request.
     * @param shareFetchData     The share partition data from the share fetch request.
     */
    public ShareSessionContext(int epoch,
                               List<TopicIdPartition> shareFetchData) {
        this.epoch = epoch;
        this.shareFetchData = shareFetchData;
        this.isSubsequent = false;
    }

    /**
     * The share fetch context for a subsequent request that utilizes an existing share session.
     *
     * @param epoch        The epoch of share session received in the share fetch request.
     * @param session      The subsequent fetch request session.
     */
    public ShareSessionContext(int epoch, ShareSession session) {
        this.epoch = epoch;
        this.session = session;
        this.isSubsequent = true;
    }

    // Visible for testing
    public List<TopicIdPartition> shareFetchData() {
        return shareFetchData;
    }

    // Visible for testing
    public boolean isSubsequent() {
        return isSubsequent;
    }

    // Visible for testing
    public ShareSession session() {
        return session;
    }

    @Override
    boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public ShareFetchResponse throttleResponse(int throttleTimeMs) {
        if (!isSubsequent) {
            return ShareFetchResponse.of(Errors.NONE, throttleTimeMs, new LinkedHashMap<>(), List.of(), 0);
        }
        int expectedEpoch = ShareRequestMetadata.nextEpoch(epoch);
        int sessionEpoch;
        synchronized (session) {
            sessionEpoch = session.epoch;
        }
        if (sessionEpoch != expectedEpoch) {
            log.debug("Subsequent share session {} expected epoch {}, but got {}. " +
                    "Possible duplicate request.", session.key(), expectedEpoch, sessionEpoch);
            return ShareFetchResponse.of(Errors.INVALID_SHARE_SESSION_EPOCH, throttleTimeMs, new LinkedHashMap<>(), List.of(), 0);
        }
        return ShareFetchResponse.of(Errors.NONE, throttleTimeMs, new LinkedHashMap<>(), List.of(), 0);
    }

    /**
     * Iterator that goes over the given partition map and selects partitions that need to be included in the response.
     * If updateShareContextAndRemoveUnselected is set to true, the share context will be updated for the selected
     * partitions and also remove unselected ones as they are encountered.
     */
    private class PartitionIterator implements Iterator<Entry<TopicIdPartition, PartitionData>> {
        private final Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> iterator;
        private final boolean updateShareContextAndRemoveUnselected;
        private Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> nextElement;


        public PartitionIterator(Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> iterator, boolean updateShareContextAndRemoveUnselected) {
            this.iterator = iterator;
            this.updateShareContextAndRemoveUnselected = updateShareContextAndRemoveUnselected;
        }

        @Override
        public boolean hasNext() {
            while ((nextElement == null) && iterator.hasNext()) {
                Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> element = iterator.next();
                TopicIdPartition topicPart = element.getKey();
                ShareFetchResponseData.PartitionData respData = element.getValue();
                synchronized (session) {
                    CachedSharePartition cachedPart = session.partitionMap().find(new CachedSharePartition(topicPart));
                    boolean mustRespond = cachedPart.maybeUpdateResponseData(respData, updateShareContextAndRemoveUnselected);
                    if (mustRespond) {
                        nextElement = element;
                        if (updateShareContextAndRemoveUnselected && ShareFetchResponse.recordsSize(respData) > 0) {
                            // Session.partitionMap is of type ImplicitLinkedHashCollection<> which tracks the order of insertion of elements.
                            // Since, we are updating an element in this case, we need to perform a remove and then a mustAdd to maintain the correct order
                            session.partitionMap().remove(cachedPart);
                            session.partitionMap().mustAdd(cachedPart);
                        }
                    } else {
                        if (updateShareContextAndRemoveUnselected) {
                            iterator.remove();
                        }
                    }
                }
            }
            return nextElement != null;
        }

        @Override
        public Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> element = nextElement;
            nextElement = null;
            return element;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int responseSize(LinkedHashMap<TopicIdPartition, PartitionData> updates, short version) {
        if (!isSubsequent)
            return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
        synchronized (session) {
            int expectedEpoch = ShareRequestMetadata.nextEpoch(epoch);
            if (session.epoch != expectedEpoch) {
                return ShareFetchResponse.sizeOf(version, Collections.emptyIterator());
            }
            // Pass the partition iterator which updates neither the share fetch context nor the partition map.
            return ShareFetchResponse.sizeOf(version, new PartitionIterator(updates.entrySet().iterator(), false));
        }
    }

    @Override
    public ShareFetchResponse updateAndGenerateResponseData(String groupId, String memberId,
                                                     LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
        if (!isSubsequent) {
            return ShareFetchResponse.of(Errors.NONE, 0, updates, List.of(), 0);
        } else {
            int expectedEpoch = ShareRequestMetadata.nextEpoch(epoch);
            int sessionEpoch;
            synchronized (session) {
                sessionEpoch = session.epoch;
            }
            if (sessionEpoch != expectedEpoch) {
                log.debug("Subsequent share session {} expected epoch {}, but got {}. Possible duplicate request.",
                        session.key(), expectedEpoch, sessionEpoch);
                return ShareFetchResponse.of(Errors.INVALID_SHARE_SESSION_EPOCH, 0, new LinkedHashMap<>(), List.of(), 0);
            }
            // Iterate over the update list using PartitionIterator. This will prune updates which don't need to be sent
            Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> partitionIterator = new PartitionIterator(
                    updates.entrySet().iterator(), true);
            while (partitionIterator.hasNext()) {
                partitionIterator.next();
            }
            log.debug("Subsequent share session context with session key {} returning {}", session.key(),
                    partitionsToLogString(updates.keySet()));
            return ShareFetchResponse.of(Errors.NONE, 0, updates, List.of(), 0);
        }
    }

    @Override
    public ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
        if (!isSubsequent) {
            return new ErroneousAndValidPartitionData(shareFetchData);
        }
        Map<TopicIdPartition, PartitionData> erroneous = new HashMap<>();
        List<TopicIdPartition> valid = new ArrayList<>();
        // Take the session lock and iterate over all the cached partitions.
        synchronized (session) {
            session.partitionMap().forEach(cachedSharePartition -> {
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                        TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                if (topicIdPartition.topic() == null) {
                    erroneous.put(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID));
                } else {
                    valid.add(topicIdPartition);
                }
            });
            return new ErroneousAndValidPartitionData(erroneous, valid);
        }
    }
}
