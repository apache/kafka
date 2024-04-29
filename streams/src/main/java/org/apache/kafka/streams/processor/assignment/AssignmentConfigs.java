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

public class AssignmentConfigs {
    private long acceptableRecoveryLag;
    private int maxWarmupReplicas;
    private int nonOverlapCost;
    private int numStandbyReplicas;
    private long probingRebalanceIntervalMs;
    private List<String> rackAwareAssignmentTags;
    private int trafficCost;

    public long acceptableRecoveryLag() {
        return acceptableRecoveryLag;
    }

    public void setAcceptableRecoveryLag(final long acceptableRecoveryLag) {
        this.acceptableRecoveryLag = acceptableRecoveryLag;
    }

    public int maxWarmupReplicas() {
        return maxWarmupReplicas;
    }

    public void setMaxWarmupReplicas(final int maxWarmupReplicas) {
        this.maxWarmupReplicas = maxWarmupReplicas;
    }

    public int numStandbyReplicas() {
        return numStandbyReplicas;
    }

    public void setNumStandbyReplicas(final int numStandbyReplicas) {
        this.numStandbyReplicas = numStandbyReplicas;
    }

    public long probingRebalanceIntervalMs() {
        return probingRebalanceIntervalMs;
    }

    public void setProbingRebalanceIntervalMs(final long probingRebalanceIntervalMs) {
        this.probingRebalanceIntervalMs = probingRebalanceIntervalMs;
    }

    public List<String> rackAwareAssignmentTags() {
        return rackAwareAssignmentTags;
    }

    public void setRackAwareAssignmentTags(final List<String> rackAwareAssignmentTags) {
        this.rackAwareAssignmentTags = rackAwareAssignmentTags;
    }

    public int trafficCost() {
        return trafficCost;
    }

    public void setTrafficCost(final int trafficCost) {
        this.trafficCost = trafficCost;
    }

    public int nonOverlapCost() {
        return nonOverlapCost;
    }

    public void setNonOverlapCost(final int nonOverlapCost) {
        this.nonOverlapCost = nonOverlapCost;
    }
}