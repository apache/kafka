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
import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.concurrent.CompletableFuture;

/**
 * A broker-side plugin that stores, forwards, or exposes topology descriptions pushed
 * by Kafka Streams clients.
 *
 * <p>Implementations must be thread-safe. {@link #setTopology} may be called
 * concurrently by multiple members of the same group; calls with the same
 * {@code (groupId, topologyEpoch)} carry identical data and must be idempotent.
 * {@link #deleteTopology} must also be idempotent — it may be called more than once
 * for the same {@code groupId}, including when nothing is stored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StreamsGroupTopologyDescriptionPlugin extends Configurable, AutoCloseable {

    /**
     * Store the topology description for a streams group.
     *
     * <p>The returned future completes when the topology has been persisted or forwarded.
     * Failures must be signalled by completing the future exceptionally — implementations
     * must not throw synchronously; a synchronous throw is treated as a permanent failure
     * with a generic client-visible error message. The completion exception drives
     * broker-side behaviour:
     *
     * <ul>
     *   <li>{@link StreamsTopologyDescriptionPermanentFailureException} — the description will never be accepted
     *       at this topology epoch (e.g. too large, semantically rejected). The broker
     *       ratchets {@code LastFailedTopologyEpoch} and stops re-soliciting until the
     *       epoch advances.</li>
     *   <li>{@link StreamsTopologyDescriptionTransientFailureException} or any other exception — treated as
     *       transient. The broker arms or extends the per-group back-off (30 s → 1 h,
     *       exponential) and re-solicits on a later heartbeat.</li>
     * </ul>
     *
     * In both cases the caller receives error code
     * {@code STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED} with the exception's message in
     * {@code ErrorMessage}; the permanent-vs-transient split is broker-internal state.
     */
    CompletableFuture<Void> setTopology(String groupId, int topologyEpoch, StreamsGroupTopologyDescription description);

    /**
     * Remove any topology description stored for this group. Called when the group is
     * deleted or expires. A failure (future completed exceptionally) is reported to the
     * caller of {@code DeleteGroups} as {@code GROUP_DELETION_FAILED} with the exception
     * message in the per-group {@code ErrorMessage}, and the broker does not tombstone
     * the group; a retry of {@code DeleteGroups} re-invokes this method idempotently.
     * The periodic-cleanup path treats a failure identically — the group's tombstone is
     * deferred to a future cycle.
     */
    CompletableFuture<Void> deleteTopology(String groupId);

    /**
     * Return the stored topology description for {@code (groupId, topologyEpoch)}, or
     * {@code null} if the plugin no longer has the data (e.g. backend wipe). If the future
     * completes exceptionally, the broker reports a read error for the group.
     */
    CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch);
}
