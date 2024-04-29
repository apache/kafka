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

/**
 * Configuration for the KafkaStreams Task Assignor.
 */
public class AssignmentConfigs {

    private long acceptableRecoveryLag;
    private int maxWarmupReplicas;
    private int nonOverlapCost;
    private int numStandbyReplicas;
    private long probingRebalanceIntervalMs;
    private List<String> rackAwareAssignmentTags;
    private int trafficCost;

    /**
     * Returns the acceptable lag in the task recovery process in milliseconds.
     *
     * @return the number of milliseconds of acceptable recovery lag.
     */
    public long acceptableRecoveryLag() {
        return acceptableRecoveryLag;
    }

    /**
     * Sets the acceptable lag in the task recovery process.
     *
     * @param acceptableRecoveryLag the acceptable recovery lag to set, in milliseconds.
     */
    public void setAcceptableRecoveryLag(final long acceptableRecoveryLag) {
        this.acceptableRecoveryLag = acceptableRecoveryLag;
    }

    /**
     * Returns the maximum number of warmup replicas allowed.
     *
     * @return the maximum number of warmup replicas.
     */
    public int maxWarmupReplicas() {
        return maxWarmupReplicas;
    }

    /**
     * Sets the maximum number of warmup replicas allowed.
     *
     * @param maxWarmupReplicas the maximum number of warmup replicas to set.
     */
    public void setMaxWarmupReplicas(final int maxWarmupReplicas) {
        this.maxWarmupReplicas = maxWarmupReplicas;
    }

    /**
     * Returns the number of standby replicas.
     *
     * @return the number of standby replicas.
     */
    public int numStandbyReplicas() {
        return numStandbyReplicas;
    }

    /**
     * Sets the number of standby replicas.
     *
     * @param numStandbyReplicas the number of standby replicas to set.
     */
    public void setNumStandbyReplicas(final int numStandbyReplicas) {
        this.numStandbyReplicas = numStandbyReplicas;
    }

    /**
     * Returns the interval between probing rebalances in milliseconds.
     *
     * @return the interval between probing rebalances.
     */
    public long probingRebalanceIntervalMs() {
        return probingRebalanceIntervalMs;
    }

    /**
     * Sets the interval between probing rebalances.
     *
     * @param probingRebalanceIntervalMs the interval between probing rebalances to set, in milliseconds.
     */
    public void setProbingRebalanceIntervalMs(final long probingRebalanceIntervalMs) {
        this.probingRebalanceIntervalMs = probingRebalanceIntervalMs;
    }

    /**
     * Returns the list of rack-aware assignment tags.
     *
     * @return the list of rack-aware assignment tags.
     */
    public List<String> rackAwareAssignmentTags() {
        return rackAwareAssignmentTags;
    }

    /**
     * Sets the list of rack-aware assignment tags.
     *
     * @param rackAwareAssignmentTags the list of rack-aware assignment tags to set.
     */
    public void setRackAwareAssignmentTags(final List<String> rackAwareAssignmentTags) {
        this.rackAwareAssignmentTags = rackAwareAssignmentTags;
    }

    /**
     * Returns the traffic cost.
     *
     * @return the traffic cost.
     */
    public int trafficCost() {
        return trafficCost;
    }

    /**
     * Sets the traffic cost.
     *
     * @param trafficCost the traffic cost to set.
     */
    public void setTrafficCost(final int trafficCost) {
        this.trafficCost = trafficCost;
    }

    /**
     * Returns the non-overlap cost.
     *
     * @return the non-overlap cost.
     */
    public int nonOverlapCost() {
        return nonOverlapCost;
    }

    /**
     * Sets the non-overlap cost.
     *
     * @param nonOverlapCost the non-overlap cost to set.
     */
    public void setNonOverlapCost(final int nonOverlapCost) {
        this.nonOverlapCost = nonOverlapCost;
    }
}