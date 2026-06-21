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
     * Build a heartbeat result for an error response — a failure-fast path in
     * {@code GroupCoordinatorService#streamsGroupHeartbeat} (broker not active, request
     * validation rejected, or a runtime error translated by {@code handleOperationException}).
     * No group is resolved, so there are no internal topics to create and no epoch context:
     * all three epoch fields are -1, and {@code maybeSetTopologyDescriptionRequired}
     * short-circuits on the error code anyway. Callers must pass a response carrying an error.
     */
    public static StreamsGroupHeartbeatResult forError(StreamsGroupHeartbeatResponseData data) {
        return new StreamsGroupHeartbeatResult(data, Map.of(), -1, -1, -1);
    }
}
