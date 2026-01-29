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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.FetchContext.FullFetchContext;
import org.apache.kafka.server.FetchContext.IncrementalFetchContext;
import org.apache.kafka.server.FetchContext.SessionErrorContext;
import org.apache.kafka.server.FetchContext.SessionlessFetchContext;
import org.apache.kafka.server.FetchSession.FetchSessionCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.requests.FetchMetadata.FINAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

public class FetchManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchManager.class);

    private final Time time;
    private final FetchSessionCache cache;

    public FetchManager(Time time, FetchSessionCacheShard cacheShard) {
        this(time, new FetchSessionCache(List.of(cacheShard)));
    }

    public FetchManager(Time time, FetchSessionCache cache) {
        this.time = time;
        this.cache = cache;
    }

    public FetchContext newContext(short reqVersion,
                                   FetchMetadata reqMetadata,
                                   boolean isFollower,
                                   Map<TopicIdPartition, FetchRequest.PartitionData> fetchData,
                                   List<TopicIdPartition> toForget,
                                   Map<Uuid, String> topicNames) {
        if (reqMetadata.isFull()) {
            String removedFetchSessionStr = "";
            if (reqMetadata.sessionId() != INVALID_SESSION_ID) {
                FetchSessionCacheShard cacheShard = cache.getCacheShard(reqMetadata.sessionId());
                // Any session specified in a FULL fetch request will be closed.
                if (cacheShard.remove(reqMetadata.sessionId()).isPresent())
                    removedFetchSessionStr = " Removed fetch session " + reqMetadata.sessionId() + ".";
            }
            String suffix = "";
            FetchContext fetchContext;
            if (reqMetadata.epoch() == FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                suffix = " Will not try to create a new session.";
                fetchContext = new SessionlessFetchContext(fetchData);
            } else
                fetchContext = new FullFetchContext(time, cache, fetchData, reqVersion >= 13, isFollower);

            LOGGER.debug("Created a new full FetchContext with {}.{}{}",
                partitionsToLogString(fetchData.keySet()), removedFetchSessionStr, suffix);
            return fetchContext;
        } else {
            FetchSessionCacheShard cacheShard = cache.getCacheShard(reqMetadata.sessionId());
            synchronized (cacheShard) {
                Optional<FetchSession> sessionOpt = cacheShard.get(reqMetadata.sessionId());

                if (sessionOpt.isEmpty()) {
                    LOGGER.debug("Session error for {}: no such session ID found.", reqMetadata.sessionId());
                    return new SessionErrorContext(Errors.FETCH_SESSION_ID_NOT_FOUND);
                } else {
                    FetchSession session = sessionOpt.get();
                    synchronized (session) {
                        if (session.epoch() != reqMetadata.epoch()) {
                            LOGGER.debug("Session error for {}: expected epoch {}, but got {} instead.",
                                reqMetadata.sessionId(), session.epoch(), reqMetadata.epoch());

                            return new SessionErrorContext(Errors.INVALID_FETCH_SESSION_EPOCH);
                        } else if (session.usesTopicIds() && reqVersion < 13 || !session.usesTopicIds() && reqVersion >= 13)  {
                            LOGGER.debug("Session error for {}: expected  {}, but request version {} means that we can not.",
                                reqMetadata.sessionId(), session.usesTopicIds() ? "to use topic IDs" : "to not use topic IDs", reqVersion);

                            return new SessionErrorContext(Errors.FETCH_SESSION_TOPIC_ID_ERROR);
                        } else {
                            List<List<TopicIdPartition>> lists = session.update(fetchData, toForget);
                            List<TopicIdPartition> added = lists.get(0);
                            List<TopicIdPartition> updated = lists.get(1);
                            List<TopicIdPartition> removed = lists.get(2);
                            if (session.isEmpty()) {
                                LOGGER.debug("Created a new sessionless FetchContext and closing session id {}, " +
                                    "epoch {}: after removing {}, there are no more partitions left.",
                                    session.id(), session.epoch(), partitionsToLogString(removed));
                                cacheShard.remove(session);

                                return new SessionlessFetchContext(fetchData);
                            } else {
                                cacheShard.touch(session, time.milliseconds());
                                session.setEpoch(FetchMetadata.nextEpoch(session.epoch()));
                                LOGGER.debug("Created a new incremental FetchContext for session id {}, " +
                                    "epoch {}: added {}, updated {}, removed {}",
                                    session.id(), session.epoch(), partitionsToLogString(added),
                                    partitionsToLogString(updated), partitionsToLogString(removed));

                                return new IncrementalFetchContext(reqMetadata, session, topicNames);
                            }
                        }
                    }
                }
            }
        }
    }

    private String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return FetchSession.partitionsToLogString(partitions, LOGGER.isTraceEnabled());
    }
}
