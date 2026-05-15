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
package org.apache.kafka.coordinator.group.api.streams;

import org.apache.kafka.common.Configurable;

import java.util.concurrent.CompletableFuture;

/**
 * A broker-side plugin that manages topology descriptions for streams groups.
 *
 * <p>Implementations receive topology descriptions pushed by Kafka Streams clients
 * and can store, forward, or expose them however they see fit. The broker is
 * authoritative on whether a topology is currently stored: it persists a
 * {@code StoredTopologyEpoch} field on the streams-group metadata record and uses it
 * to decide whether to solicit a fresh push on the heartbeat path. Plugins therefore
 * do not need to maintain a per-tuple state machine to answer "do I have this?" —
 * that question has a broker-side answer.
 *
 * <p>Implementations must be thread-safe. {@link #setTopology} may be called
 * concurrently by multiple group members observing
 * {@code TopologyDescriptionRequired=true} in the same heartbeat cycle; concurrent
 * calls with the same {@code (groupId, topologyEpoch)} pair carry identical data
 * and must be treated as idempotent. {@link #deleteTopology} may be called multiple
 * times for the same {@code groupId} if a prior call's bookkeeping write failed; it
 * must also be idempotent when the group has no stored topology.
 */
public interface StreamsGroupTopologyDescriptionPlugin extends Configurable, AutoCloseable {

    /**
     * Called when a client sends a topology description for a streams group.
     * This method may be called concurrently by multiple members of the same group;
     * all calls for the same (groupId, topologyEpoch) carry identical data.
     *
     * <p>The returned future completes when the topology has been persisted or
     * forwarded. All failures must be signalled by completing the returned future
     * exceptionally — implementations must not throw synchronously from this method.
     * The broker handles the future's completion exception (when present) as follows:
     *
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException} maps to
     *       {@code INVALID_REQUEST} — use it for payloads the plugin cannot accept on
     *       semantic grounds. The broker persists this as a permanent-failure
     *       {@code LastFailedTopologyEpoch} so subsequent heartbeats do not re-solicit at
     *       the same topology epoch.</li>
     *   <li>{@link org.apache.kafka.common.errors.TopologyDescriptionTooLargeException}
     *       maps to {@code TOPOLOGY_DESCRIPTION_TOO_LARGE} — use it when the description
     *       is larger than the plugin is willing to store. Same permanent-failure
     *       treatment as above.</li>
     *   <li>Any other exception maps to {@code TOPOLOGY_DESCRIPTION_UPDATE_FAILED} and is
     *       logged at WARN. The broker treats it as transient and extends the in-memory back-off
     *       it already armed when the heartbeat solicited this push, so the heartbeat path will
     *       not re-solicit a fresh push at the same topology epoch until the next back-off
     *       window has elapsed. Consecutive unsuccessful solicitations (transient plugin failures
     *       or clients that ignore the flag) double the window, starting at 30 s and capped at
     *       1 h; a successful push or a topology-epoch advance clears the state.</li>
     * </ul>
     *
     * @param groupId the streams group ID
     * @param topologyEpoch the topology epoch
     * @param description the topology description
     * @return a future that completes when the operation is done
     */
    CompletableFuture<Void> setTopology(String groupId, int topologyEpoch,
                                        StreamsGroupTopologyDescription description);

    /**
     * Called when the broker removes a streams group. Removes any topology
     * descriptions stored for this group.
     *
     * <p>Invoked on two paths: when a client deletes the group via {@code DeleteGroups}
     * (before the group tombstone is written), and from the broker-internal periodic
     * topology-description cleanup when a streams group becomes empty and all its
     * offsets have expired.
     *
     * <p>The returned future completes when the deletion has been processed.
     * If it completes exceptionally, the broker logs the error; the outcome does not
     * affect the user-visible result of the operation that triggered the removal.
     * The broker may call this method multiple times for the same {@code groupId} if a
     * prior call's bookkeeping write failed — implementations must be idempotent.
     *
     * @param groupId the streams group ID
     * @return a future that completes when the operation is done
     */
    CompletableFuture<Void> deleteTopology(String groupId);

    /**
     * Called to retrieve the stored topology description for a group. This is invoked
     * by the broker when a client calls {@code StreamsGroupDescribe} with
     * {@code IncludeTopologyDescription=true}, but only when the broker-side
     * {@code StoredTopologyEpoch} matches the group's current topology epoch — otherwise
     * the describe path returns {@code NOT_STORED} without invoking this method.
     *
     * <p>Returns a future that resolves to the stored topology description for the
     * given {@code (groupId, topologyEpoch)} pair, or to {@code null} if the plugin has
     * lost its data (e.g. backend wipe); the broker surfaces {@code null} as
     * {@code NOT_STORED} on the describe response.
     *
     * <p>If the future completes exceptionally, the broker signals a read error for this
     * group.
     *
     * @param groupId the streams group ID
     * @param topologyEpoch the topology epoch the caller is asking about
     * @return a future resolving to the stored topology description, or null if none
     */
    CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch);
}
