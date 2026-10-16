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
 * Result of a describe operation against a set of streams groups. Pairs each described group with the
 * topology epoch the broker currently has a stored description for.
 *
 * @param describedGroups                  The described groups (one per requested group id, including errored ones).
 * @param storedDescriptionTopologyEpochs  Per-group stored description topology epoch, keyed by group id. Only
 *                                         present for groups resolved successfully (no GROUP_ID_NOT_FOUND); a value
 *                                         of -1 means no description has been stored for the group. KIP-1331 gates
 *                                         calls to the topology description plugin on
 *                                         {@code storedDescriptionTopologyEpoch != -1 &&
 *                                         storedDescriptionTopologyEpoch == currentTopologyEpoch}; the {@code != -1}
 *                                         guard covers brand-new groups where both sides would be -1 and the plugin
 *                                         has nothing to serve.
 */
public record StreamsGroupDescribeResult(
    List<StreamsGroupDescribeResponseData.DescribedGroup> describedGroups,
    Map<String, Integer> storedDescriptionTopologyEpochs
) {

    public StreamsGroupDescribeResult {
        describedGroups = List.copyOf(Objects.requireNonNull(describedGroups));
        storedDescriptionTopologyEpochs = Collections.unmodifiableMap(Objects.requireNonNull(storedDescriptionTopologyEpochs));
    }
}
