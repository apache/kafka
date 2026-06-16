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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A simple record to hold the result of a StreamsGroupHeartbeat request.
 *
 * <p>The three epoch fields let the service layer decide, without re-reading the group,
 * whether to set {@code TopologyDescriptionRequired} on the response: a push is needed
 * when the stored epoch lags the current epoch and the same epoch has not already been
 * recorded as a permanent failure. All three are -1 for failure-fast paths that do not
 * resolve a group.
 *
 * @param data                              The data to be returned to the client.
 * @param creatableTopics                   The internal topics to be created.
 * @param currentTopologyEpoch              The topology epoch the group is operating at after this heartbeat,
 *                                          or -1 if the group has no topology yet.
 * @param storedDescriptionTopologyEpoch    The most recent topology epoch successfully stored by the topology
 *                                          description plugin, or -1 if none.
 * @param failedDescriptionTopologyEpoch    The most recent topology epoch the plugin permanently rejected,
 *                                          or -1 if none.
 */
public record StreamsGroupHeartbeatResult(
    StreamsGroupHeartbeatResponseData data,
    Map<String, CreatableTopic> creatableTopics,
    int currentTopologyEpoch,
    int storedDescriptionTopologyEpoch,
    int failedDescriptionTopologyEpoch
) {

    public StreamsGroupHeartbeatResult {
        Objects.requireNonNull(data);
        creatableTopics = Collections.unmodifiableMap(Objects.requireNonNull(creatableTopics));
    }

    /**
     * Build a heartbeat result that bypasses the service-layer topology post-processing.
     * Used at two distinct call sites:
     *
     * <ul>
     *   <li>Failure-fast paths in {@code GroupCoordinatorService#streamsGroupHeartbeat}
     *       — broker not active, request validation rejected, runtime error translated
     *       by {@code handleOperationException}. The group is not resolved, so there is
     *       no epoch context to track.</li>
     *   <li>Departing-member paths in {@code GroupMetadataManager} (leave or fence).
     *       The group exists but the member will not push a topology description, so
     *       attaching the live epoch would arm a back-off window on its behalf and
     *       delay solicitation for the rest of the group.</li>
     * </ul>
     *
     * <p>All three epoch fields are set to -1, causing
     * {@code maybeSetTopologyDescriptionRequired} to short-circuit before arming the
     * back-off or setting the {@code TopologyDescriptionRequired} flag.
     */
    public static StreamsGroupHeartbeatResult withoutEpochContext(StreamsGroupHeartbeatResponseData data) {
        return new StreamsGroupHeartbeatResult(data, Map.of(), -1, -1, -1);
    }
}
