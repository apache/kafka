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
 * @param data                     The data to be returned to the client.
 * @param creatableTopics          The internal topics to be created.
 * @param topologyEpoch            The group's current topology epoch.
 * @param storedTopologyEpoch      The topology epoch currently stored in the topology
 *                                 description plugin (or {@code -1} if none). Compared
 *                                 against {@code topologyEpoch} on the heartbeat path to
 *                                 decide whether to solicit a fresh push.
 * @param lastFailedTopologyEpoch  The most recent topology epoch the plugin permanently
 *                                 rejected (or {@code -1} if none). Suppresses solicitation
 *                                 at that exact epoch to avoid hot-looping.
 */
public record StreamsGroupHeartbeatResult(
    StreamsGroupHeartbeatResponseData data,
    Map<String, CreatableTopic> creatableTopics,
    int topologyEpoch,
    int storedTopologyEpoch,
    int lastFailedTopologyEpoch
) {

    public StreamsGroupHeartbeatResult {
        Objects.requireNonNull(data);
        creatableTopics = Collections.unmodifiableMap(Objects.requireNonNull(creatableTopics));
    }

    /**
     * Convenience constructor for callers (mostly tests) that don't supply plugin-tracking
     * fields. Defaults {@code storedTopologyEpoch} and {@code lastFailedTopologyEpoch} to
     * {@code -1} — treated by the service as "nothing stored / no permanent failure," which
     * means a heartbeat-path solicitation will be issued if {@code topologyEpoch >= 0}.
     */
    public StreamsGroupHeartbeatResult(
        StreamsGroupHeartbeatResponseData data,
        Map<String, CreatableTopic> creatableTopics,
        int topologyEpoch
    ) {
        this(data, creatableTopics, topologyEpoch, -1, -1);
    }

}
