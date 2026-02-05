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
package org.apache.kafka.server;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.FetchSession.CachedPartition;
import org.apache.kafka.server.FetchSession.FetchSessionCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

public sealed interface FetchContext {
    /**
     * Get the fetch offset for a given partition.
     */
    Optional<Long> getFetchOffset(TopicIdPartition part);

    /**
     * Apply a function to each partition in the fetch request.
     */
    void foreachPartition(BiConsumer<TopicIdPartition, FetchRequest.PartitionData> fun);

    /**
     * Get the response size to be used for quota computation. Since we are returning an empty response in case of
     * throttling, we are not supposed to update the context until we know that we are not going to throttle.
     */
    int getResponseSize(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates, short versionId);

    /**
     * Updates the fetch context with new partition information.  Generates response data.
     * The response data may require subsequent down-conversion.
     */
    FetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates, List<Node> nodeEndpoints);

    default String partitionsToLogString(Collection<TopicIdPartition> partitions, boolean isTraceEnabled) {
        return FetchSession.partitionsToLogString(partitions, isTraceEnabled);
    }

    /**
     * Return an empty throttled response due to quota violation.
     */
    default FetchResponse getThrottledResponse(int throttleTimeMs, List<Node> nodeEndpoints) {
        return FetchResponse.of(Errors.NONE, throttleTimeMs, INVALID_SESSION_ID, new LinkedHashMap<>(), nodeEndpoints);
    }

    /**
     * The fetch context for a fetch request that had a session error.
     */
    final class SessionErrorContext implements FetchContext {
        private static final Logger LOGGER = LoggerFactory.getLogger(SessionErrorContext.class);

        private final Errors error;

        public SessionErrorContext(Errors error) {
            this.error = error;
        }

        @Override
        public Optional<Long> getFetchOffset(TopicIdPartition part) {
            return Optional.empty();
        }

        @Override
        public void foreachPartition(BiConsumer<TopicIdPartition, FetchRequest.PartitionData> fun) {
        }

        @Override
        public int getResponseSize(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates, short versionId) {
            return FetchResponse.sizeOf(versionId, Collections.emptyIterator());
        }

        /**
         * Because of the fetch session error, we don't know what partitions were supposed to be in this request.
         */
        @Override
        public FetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates,
                                                           List<Node> nodeEndpoints) {
            LOGGER.debug("Session error fetch context returning {}", error);
            return FetchResponse.of(error, 0, INVALID_SESSION_ID, new LinkedHashMap<>(), nodeEndpoints);
        }
    }

    /**
     * The fetch context for a sessionless fetch request.
     */
    final class SessionlessFetchContext implements FetchContext {
        private static final Logger LOGGER = LoggerFactory.getLogger(SessionlessFetchContext.class);

        private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchData;

        /**
         * @param fetchData The partition data from the fetch request.
         */
        public SessionlessFetchContext(Map<TopicIdPartition, FetchRequest.PartitionData> fetchData) {
            this.fetchData = fetchData;
        }

        @Override
        public Optional<Long> getFetchOffset(TopicIdPartition part) {
            return Optional.ofNullable(fetchData.get(part)).map(data -> data.fetchOffset);
        }

        @Override
        public void foreachPartition(BiConsumer<TopicIdPartition, FetchRequest.PartitionData> fun) {
            fetchData.forEach(fun);
        }

        @Override
        public int getResponseSize(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates, short versionId) {
            return FetchResponse.sizeOf(versionId, updates.entrySet().iterator());
        }

        @Override
        public FetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates,
                                                           List<Node> nodeEndpoints) {
            LOGGER.debug("Sessionless fetch context returning {}", partitionsToLogString(updates.keySet(), LOGGER.isTraceEnabled()));
            return FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, updates, nodeEndpoints);
        }
    }

    /**
     * The fetch context for a full fetch request.
     */
    final class FullFetchContext implements FetchContext {
        private static final Logger LOGGER = LoggerFactory.getLogger(FullFetchContext.class);

        private final Time time;
        private final FetchSessionCache cache;
        private final Map<TopicIdPartition, FetchRequest.PartitionData> fetchData;
        private final boolean usesTopicIds;
        private final boolean isFromFollower;

        public FullFetchContext(Time time,
                                FetchSessionCacheShard cacheShard,
                                Map<TopicIdPartition, FetchRequest.PartitionData> fetchData,
                                boolean usesTopicIds,
                                boolean isFromFollower) {
            this(time, new FetchSessionCache(List.of(cacheShard)), fetchData, usesTopicIds, isFromFollower);
        }

        /**
         * @param time           The clock to use
         * @param cache          The fetch session cache
         * @param fetchData      The partition data from the fetch request
         * @param usesTopicIds   True if this session should use topic IDs
         * @param isFromFollower True if this fetch request came from a follower
         */
        public FullFetchContext(Time time,
                                FetchSessionCache cache,
                                Map<TopicIdPartition, FetchRequest.PartitionData> fetchData,
                                boolean usesTopicIds,
                                boolean isFromFollower) {
            this.time = time;
            this.cache = cache;
            this.fetchData = fetchData;
            this.usesTopicIds = usesTopicIds;
            this.isFromFollower = isFromFollower;
        }

        @Override
        public Optional<Long> getFetchOffset(TopicIdPartition part) {
            return Optional.ofNullable(fetchData.get(part)).map(data -> data.fetchOffset);
        }

        @Override
        public void foreachPartition(BiConsumer<TopicIdPartition, FetchRequest.PartitionData> fun) {
            fetchData.forEach(fun);
        }

        @Override
        public int getResponseSize(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates, short versionId) {
            return FetchResponse.sizeOf(versionId, updates.entrySet().iterator());
        }

        @Override
        public FetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates,
                                                           List<Node> nodeEndpoints) {
            FetchSessionCacheShard cacheShard = cache.getNextCacheShard();
            int responseSessionId = cacheShard.maybeCreateSession(time.milliseconds(), isFromFollower,
                updates.size(), usesTopicIds, () -> createNewSession(updates));
            LOGGER.debug("Full fetch context with session id {} returning {}",
                responseSessionId, partitionsToLogString(updates.keySet(), LOGGER.isTraceEnabled()));

            return FetchResponse.of(Errors.NONE, 0, responseSessionId, updates, nodeEndpoints);
        }

        private ImplicitLinkedHashCollection<CachedPartition> createNewSession(
                LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates
        ) {
            ImplicitLinkedHashCollection<CachedPartition> cachedPartitions = new ImplicitLinkedHashCollection<>(updates.size());
            updates.forEach((part, respData) -> {
                FetchRequest.PartitionData reqData = fetchData.get(part);
                cachedPartitions.mustAdd(new CachedPartition(part, reqData, respData));
            });

            return cachedPartitions;
        }
    }

    /**
     * The fetch context for an incremental fetch request.
     */
    final class IncrementalFetchContext implements FetchContext {
        private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalFetchContext.class);

        private final FetchMetadata reqMetadata;
        private final FetchSession session;
        private final Map<Uuid, String> topicNames;

        /**
         * @param reqMetadata  The request metadata
         * @param session      The incremental fetch request session
         * @param topicNames   A mapping from topic ID to topic name used to resolve partitions already in the session
         */
        public IncrementalFetchContext(FetchMetadata reqMetadata,
                                       FetchSession session,
                                       Map<Uuid, String> topicNames) {
            this.reqMetadata = reqMetadata;
            this.session = session;
            this.topicNames = topicNames;
        }

        @Override
        public Optional<Long> getFetchOffset(TopicIdPartition part) {
            return session.getFetchOffset(part);
        }

        @Override
        public void foreachPartition(BiConsumer<TopicIdPartition, FetchRequest.PartitionData> fun) {
            // Take the session lock and iterate over all the cached partitions.
            synchronized (session) {
                session.partitionMap().forEach(part -> {
                    // Try to resolve an unresolved partition if it does not yet have a name
                    if (session.usesTopicIds())
                        part.maybeResolveUnknownName(topicNames);
                    fun.accept(new TopicIdPartition(part.topicId(), new TopicPartition(part.topic(), part.partition())), part.reqData());
                });
            }
        }

        /**
         * Iterator that goes over the given partition map and selects partitions that need to be included in the response.
         * If updateFetchContextAndRemoveUnselected is set to true, the fetch context will be updated for the selected
         * partitions and also remove unselected ones as they are encountered.
         */
        private class PartitionIterator implements Iterator<Map.Entry<TopicIdPartition, FetchResponseData.PartitionData>> {
            private final Iterator<Map.Entry<TopicIdPartition, FetchResponseData.PartitionData>> iter;
            private final boolean updateFetchContextAndRemoveUnselected;
            private Map.Entry<TopicIdPartition, FetchResponseData.PartitionData> nextElement;

            public PartitionIterator(Iterator<Map.Entry<TopicIdPartition, FetchResponseData.PartitionData>> iter,
                                     boolean updateFetchContextAndRemoveUnselected) throws NoSuchElementException {
                this.iter = iter;
                this.updateFetchContextAndRemoveUnselected = updateFetchContextAndRemoveUnselected;
            }

            @Override
            public boolean hasNext() {
                while ((nextElement == null) && iter.hasNext()) {
                    Map.Entry<TopicIdPartition, FetchResponseData.PartitionData> element = iter.next();
                    TopicIdPartition topicPart = element.getKey();
                    FetchResponseData.PartitionData respData = element.getValue();
                    CachedPartition cachedPart = session.partitionMap().find(new CachedPartition(topicPart));
                    boolean mustRespond = cachedPart != null && cachedPart.maybeUpdateResponseData(respData, updateFetchContextAndRemoveUnselected);
                    if (mustRespond) {
                        nextElement = element;
                        if (updateFetchContextAndRemoveUnselected && FetchResponse.recordsSize(respData) > 0) {
                            session.partitionMap().remove(cachedPart);
                            session.partitionMap().mustAdd(cachedPart);
                        }
                    } else if (updateFetchContextAndRemoveUnselected) {
                        iter.remove();
                    }
                }

                return nextElement != null;
            }

            @Override
            public Map.Entry<TopicIdPartition, FetchResponseData.PartitionData> next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                Map.Entry<TopicIdPartition, FetchResponseData.PartitionData> element = nextElement;
                nextElement = null;

                return element;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public int getResponseSize(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates, short versionId) {
            synchronized (session) {
                int expectedEpoch = FetchMetadata.nextEpoch(reqMetadata.epoch());
                if (session.epoch() != expectedEpoch) {
                    return FetchResponse.sizeOf(versionId, Collections.emptyIterator());
                } else {
                    // Pass the partition iterator which updates neither the fetch context nor the partition map.
                    return FetchResponse.sizeOf(versionId, new PartitionIterator(updates.entrySet().iterator(), false));
                }
            }
        }

        @Override
        public FetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> updates,
                                                           List<Node> nodeEndpoints) {
            synchronized (session) {
                // Check to make sure that the session epoch didn't change in between
                // creating this fetch context and generating this response.
                int expectedEpoch = FetchMetadata.nextEpoch(reqMetadata.epoch());
                if (session.epoch() != expectedEpoch) {
                    LOGGER.info("Incremental fetch session {} expected epoch {}, but got {}. Possible duplicate request.",
                        session.id(), expectedEpoch, session.epoch());
                    return FetchResponse.of(Errors.INVALID_FETCH_SESSION_EPOCH, 0, session.id(), new LinkedHashMap<>(), nodeEndpoints);
                } else {
                    // Iterate over the update list using PartitionIterator. This will prune updates which don't need to be sent
                    PartitionIterator partitionIter = new PartitionIterator(updates.entrySet().iterator(), true);
                    while (partitionIter.hasNext()) {
                        partitionIter.next();
                    }
                    LOGGER.debug("Incremental fetch context with session id {} returning {}", session.id(),
                        partitionsToLogString(updates.keySet(), LOGGER.isTraceEnabled()));
                    return FetchResponse.of(Errors.NONE, 0, session.id(), updates, nodeEndpoints);
                }
            }
        }

        @Override
        public FetchResponse getThrottledResponse(int throttleTimeMs, List<Node> nodeEndpoints) {
            synchronized (session) {
                // Check to make sure that the session epoch didn't change in between
                // creating this fetch context and generating this response.
                int expectedEpoch = FetchMetadata.nextEpoch(reqMetadata.epoch());
                if (session.epoch() != expectedEpoch) {
                    LOGGER.info("Incremental fetch session {} expected epoch {}, but got {}. Possible duplicate request.",
                        session.id(), expectedEpoch, session.epoch());
                    return FetchResponse.of(Errors.INVALID_FETCH_SESSION_EPOCH, throttleTimeMs, session.id(), new LinkedHashMap<>(), nodeEndpoints);
                } else {
                    return FetchResponse.of(Errors.NONE, throttleTimeMs, session.id(), new LinkedHashMap<>(), nodeEndpoints);
                }
            }
        }
    }
}
