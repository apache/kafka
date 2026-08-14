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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The assignment configurations for a streams group.
 *
 * @param numStandbyReplicas      The number of standby replicas for each task.
 * @param rackAwareAssignmentTags The client tags used to distribute standby tasks across racks.
 */
public record AssignmentConfigsImpl(
    int numStandbyReplicas,
    List<String> rackAwareAssignmentTags
) implements AssignmentConfigs {

    // The names under which the configurations are passed to the assignor and recorded for the group.
    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    public static final String RACK_AWARE_ASSIGNMENT_TAGS_CONFIG = "rack.aware.assignment.tags";

    /**
     * The configs of a group that has none of them set, holding the default value of every configuration.
     */
    public static final AssignmentConfigsImpl DEFAULT = new AssignmentConfigsImpl(
        GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT,
        parseRackAwareAssignmentTags(GroupCoordinatorConfig.STREAMS_GROUP_RACK_AWARE_ASSIGNMENT_TAGS_DEFAULT)
    );

    public AssignmentConfigsImpl {
        // The list is exposed to a custom assignor through the public AssignmentConfigs interface.
        rackAwareAssignmentTags = List.copyOf(Objects.requireNonNull(rackAwareAssignmentTags));
    }

    /**
     * Converts the raw assignment configs recorded for the group into the typed configs passed to the assignor.
     */
    public static AssignmentConfigsImpl fromMap(Map<String, String> configs) {
        // Configs are only recorded when set, so every absent key means the configuration is at its default.
        String numStandbyReplicas = configs.getOrDefault(NUM_STANDBY_REPLICAS_CONFIG,
            Integer.toString(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT));
        String rackAwareAssignmentTags = configs.getOrDefault(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG,
            GroupCoordinatorConfig.STREAMS_GROUP_RACK_AWARE_ASSIGNMENT_TAGS_DEFAULT);
        return new AssignmentConfigsImpl(
            Integer.parseInt(numStandbyReplicas),
            parseRackAwareAssignmentTags(rackAwareAssignmentTags)
        );
    }

    /**
     * Parses a recorded rack-aware assignment tags value, the way {@link org.apache.kafka.common.config.ConfigDef}
     * parses a {@code LIST} configuration: an empty value is an empty list, not a list holding an empty string.
     */
    private static List<String> parseRackAwareAssignmentTags(String rackAwareAssignmentTags) {
        return rackAwareAssignmentTags.isEmpty() ? List.of() : List.of(rackAwareAssignmentTags.split(","));
    }

    /**
     * Converts the typed configs into the raw configs recorded for the group; the inverse of {@link #fromMap(Map)}.
     */
    public Map<String, String> toMap() {
        // The rack-aware assignment tags are only recorded when any are configured, matching what fromMap expects.
        Map<String, String> configs = new TreeMap<>();
        configs.put(NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(numStandbyReplicas));
        if (!rackAwareAssignmentTags.isEmpty()) {
            configs.put(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, String.join(",", rackAwareAssignmentTags));
        }
        return configs;
    }

    /**
     * Returns these configs with the number of standby replicas replaced.
     */
    public AssignmentConfigsImpl withNumStandbyReplicas(int numStandbyReplicas) {
        return new AssignmentConfigsImpl(numStandbyReplicas, rackAwareAssignmentTags);
    }

    /**
     * Returns these configs with the rack-aware assignment tags replaced.
     */
    public AssignmentConfigsImpl withRackAwareAssignmentTags(List<String> rackAwareAssignmentTags) {
        return new AssignmentConfigsImpl(numStandbyReplicas, rackAwareAssignmentTags);
    }
}
