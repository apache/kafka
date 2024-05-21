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
package org.apache.kafka.streams.processor.assignment;

import java.util.List;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Assignment related configs for the Kafka Streams {@link TaskAssignor}.
 */
public class AssignmentConfigs {
    private final long acceptableRecoveryLag;
    private final int maxWarmupReplicas;
    private final int numStandbyReplicas;
    private final long probingRebalanceIntervalMs;
    private final List<String> rackAwareAssignmentTags;
    private final int rackAwareTrafficCost;
    private final int rackAwareNonOverlapCost;
    private final String rackAwareAssignmentStrategy;

    public AssignmentConfigs(final StreamsConfig configs) {
        this(
            configs.getLong(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG),
            configs.getInt(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG),
            configs.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG),
            configs.getLong(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG),
            configs.getList(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG),
            configs.getInt(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG),
            configs.getInt(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG),
            configs.getString(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG)
        );
    }

    public AssignmentConfigs(final long acceptableRecoveryLag,
                             final int maxWarmupReplicas,
                             final int numStandbyReplicas,
                             final long probingRebalanceIntervalMs,
                             final List<String> rackAwareAssignmentTags,
                             final int rackAwareTrafficCost,
                             final int rackAwareNonOverlapCost,
                             final String rackAwareAssignmentStrategy
    ) {
        this.acceptableRecoveryLag = validated(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, acceptableRecoveryLag);
        this.maxWarmupReplicas = validated(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, maxWarmupReplicas);
        this.numStandbyReplicas = validated(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        this.probingRebalanceIntervalMs = validated(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, probingRebalanceIntervalMs);
        this.rackAwareAssignmentTags = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, rackAwareAssignmentTags);
        this.rackAwareTrafficCost = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG,
            rackAwareTrafficCost
        );
        this.rackAwareNonOverlapCost = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG,
            rackAwareNonOverlapCost
        );
        this.rackAwareAssignmentStrategy = validated(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG,
            rackAwareAssignmentStrategy
        );
    }

    /**
     * The configured acceptable recovery lag according to
     * {@link StreamsConfig#ACCEPTABLE_RECOVERY_LAG_CONFIG}
     */
    public long acceptableRecoveryLag() {
        return acceptableRecoveryLag;
    }

    /**
     * The maximum warmup replicas as configured via
     * {@link StreamsConfig#MAX_WARMUP_REPLICAS_CONFIG}
     */
    public int maxWarmupReplicas() {
        return maxWarmupReplicas;
    }

    /**
     * The number of standby replicas as configured via
     * {@link StreamsConfig#NUM_STANDBY_REPLICAS_CONFIG}
     */
    public int numStandbyReplicas() {
        return numStandbyReplicas;
    }

    /**
     * The probing rebalance interval in milliseconds as configured via
     * {@link StreamsConfig#PROBING_REBALANCE_INTERVAL_MS_CONFIG}
     */
    public long probingRebalanceIntervalMs() {
        return probingRebalanceIntervalMs;
    }

    /**
     * The rack-aware assignment tags as configured via
     * {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_TAGS_CONFIG}
     */
    public List<String> rackAwareAssignmentTags() {
        return rackAwareAssignmentTags;
    }

    /**
     * The rack-aware assignment traffic cost as configured via
     * {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG}
     */
    public int rackAwareTrafficCost() {
        return rackAwareTrafficCost;
    }

    /**
     * The rack-aware assignment non-overlap cost as configured via
     * {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG}
     */
    public int rackAwareNonOverlapCost() {
        return rackAwareNonOverlapCost;
    }

    /**
     * The rack-aware assignment strategy as configured via
     * {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG}
     */
    public String rackAwareAssignmentStrategy() {
        return rackAwareAssignmentStrategy;
    }

    private static <T> T validated(final String configKey, final T value) {
        final ConfigDef.Validator validator = StreamsConfig.configDef().configKeys().get(configKey).validator;
        if (validator != null) {
            validator.ensureValid(configKey, value);
        }
        return value;
    }

    @Override
    public String toString() {
        return "AssignmentConfigs{" +
               "\n  acceptableRecoveryLag=" + acceptableRecoveryLag +
               "\n  maxWarmupReplicas=" + maxWarmupReplicas +
               "\n  numStandbyReplicas=" + numStandbyReplicas +
               "\n  probingRebalanceIntervalMs=" + probingRebalanceIntervalMs +
               "\n  rackAwareAssignmentTags=" + rackAwareAssignmentTags +
               "\n  rackAwareTrafficCost=" + rackAwareTrafficCost +
               "\n  rackAwareNonOverlapCost=" + rackAwareNonOverlapCost +
               "\n  rackAwareAssignmentStrategy=" + rackAwareAssignmentStrategy +
               "\n}";
    }
}