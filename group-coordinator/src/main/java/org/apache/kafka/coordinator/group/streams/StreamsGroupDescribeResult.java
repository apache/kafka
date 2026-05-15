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

import java.util.List;
import java.util.Map;

/**
 * Result of a streams group describe operation on a coordinator shard. Bundles the
 * user-facing {@link StreamsGroupDescribeResponseData.DescribedGroup} list with the
 * per-group stored-topology-epoch values needed by the topology description plugin
 * read path.
 *
 * <p>{@code storedTopologyEpochs} only contains entries for groups that were found
 * successfully. Groups that failed with {@code GROUP_ID_NOT_FOUND} are not present.
 * Each value is the {@code StoredTopologyEpoch} field from the group's persisted
 * metadata. The service uses it to decide whether to invoke the plugin's
 * {@code getTopology} (only when it matches the group's current topology epoch).
 */
public record StreamsGroupDescribeResult(
    List<StreamsGroupDescribeResponseData.DescribedGroup> describedGroups,
    Map<String, Integer> storedTopologyEpochs
) {
    /**
     * Convenience constructor for callers (mostly tests) that don't supply stored-topology
     * epochs. Defaults the per-group {@code storedTopologyEpoch} to absent, which the service
     * treats as "nothing stored" — describe will return {@code NOT_STORED} without invoking
     * the plugin's {@code getTopology}.
     */
    public StreamsGroupDescribeResult(
        List<StreamsGroupDescribeResponseData.DescribedGroup> describedGroups
    ) {
        this(describedGroups, Map.of());
    }
}
