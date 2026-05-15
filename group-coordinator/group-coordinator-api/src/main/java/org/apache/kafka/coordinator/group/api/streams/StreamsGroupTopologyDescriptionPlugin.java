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
 * A broker-side plugin that stores, forwards, or exposes topology descriptions pushed
 * by Kafka Streams clients.
 *
 * <p>Implementations must be thread-safe. {@link #setTopology} may be called
 * concurrently by multiple members of the same group; calls with the same
 * {@code (groupId, topologyEpoch)} carry identical data and must be idempotent.
 * {@link #deleteTopology} must also be idempotent — it may be called more than once
 * for the same {@code groupId}, including when nothing is stored.
 */
public interface StreamsGroupTopologyDescriptionPlugin extends Configurable, AutoCloseable {

    /**
     * Store the topology description for a streams group.
     *
     * <p>The returned future completes when the topology has been persisted or forwarded.
     * Failures must be signalled by completing the future exceptionally — implementations
     * must not throw synchronously. The completion exception maps to the client-visible
     * error code:
     *
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException} — payloads the
     *       plugin will not accept on semantic grounds; reported as {@code INVALID_REQUEST}.</li>
     *   <li>{@link org.apache.kafka.common.errors.TopologyDescriptionTooLargeException} —
     *       descriptions larger than the plugin is willing to store; reported as
     *       {@code TOPOLOGY_DESCRIPTION_TOO_LARGE}.</li>
     *   <li>Any other exception — transient backend failure; reported as
     *       {@code TOPOLOGY_DESCRIPTION_UPDATE_FAILED}.</li>
     * </ul>
     *
     * The first two are treated as permanent at this topology epoch and no further push
     * will be solicited until the epoch advances. The third is treated as transient and
     * may be retried.
     */
    CompletableFuture<Void> setTopology(String groupId, int topologyEpoch,
                                        StreamsGroupTopologyDescription description);

    /**
     * Remove any topology description stored for this group. Called when the group is
     * deleted or expires. Failures are logged by the broker but do not propagate to the
     * user-visible result of whatever operation triggered the removal.
     */
    CompletableFuture<Void> deleteTopology(String groupId);

    /**
     * Return the stored topology description for {@code (groupId, topologyEpoch)}, or
     * {@code null} if the plugin no longer has the data (e.g. backend wipe). If the future
     * completes exceptionally, the broker reports a read error for the group.
     */
    CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch);
}
