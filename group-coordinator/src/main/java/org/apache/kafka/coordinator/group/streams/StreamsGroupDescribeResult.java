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

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bundles the per-group describe response data with the persisted topology epoch the plugin has stored
 * for each successfully described group (KIP-1331). The stored epoch lets the service layer decide
 * whether to call the topology description plugin's {@code getTopology} for a group — the plugin is
 * only consulted when {@code storedTopologyEpoch == currentTopologyEpoch}.
 *
 * @param describedGroups      The described groups (one per requested group id, including errored ones).
 * @param storedTopologyEpochs Per-group stored topology epoch, keyed by group id. Only present for groups
 *                             that were resolved successfully (no GROUP_ID_NOT_FOUND). A value of -1 means
 *                             no topology description has ever been accepted by the plugin for the group.
 */
public record StreamsGroupDescribeResult(
    List<StreamsGroupDescribeResponseData.DescribedGroup> describedGroups,
    Map<String, Integer> storedTopologyEpochs
) {

    public StreamsGroupDescribeResult {
        describedGroups = List.copyOf(Objects.requireNonNull(describedGroups));
        storedTopologyEpochs = Collections.unmodifiableMap(Objects.requireNonNull(storedTopologyEpochs));
    }
}
