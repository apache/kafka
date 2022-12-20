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

package org.apache.kafka.controller;

import java.util.concurrent.atomic.AtomicInteger;

public final class MockControllerMetrics implements ControllerMetrics {
    private volatile boolean active = false;
    private volatile int fencedBrokers = 0;
    private volatile int activeBrokers = 0;
    private volatile int topics = 0;
    private volatile int partitions = 0;
    private volatile int offlinePartitions = 0;
    private volatile int preferredReplicaImbalances = 0;
    private volatile AtomicInteger metadataErrors = new AtomicInteger(0);
    private volatile long lastAppliedRecordOffset = 0;
    private volatile long lastCommittedRecordOffset = 0;
    private volatile long lastAppliedRecordTimestamp = 0;

    private volatile boolean closed = false;

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean active() {
        return this.active;
    }

    @Override
    public void updateEventQueueTime(long durationMs) {
        // nothing to do
    }

    @Override
    public void updateEventQueueProcessingTime(long durationMs) {
        // nothing to do
    }

    @Override
    public void setFencedBrokerCount(int brokerCount) {
        this.fencedBrokers = brokerCount;
    }

    @Override
    public int fencedBrokerCount() {
        return this.fencedBrokers;
    }

    @Override
    public void setActiveBrokerCount(int brokerCount) {
        this.activeBrokers = brokerCount;
    }

    @Override
    public int activeBrokerCount() {
        return activeBrokers;
    }

    @Override
    public void setGlobalTopicCount(int topicCount) {
        this.topics = topicCount;
    }

    @Override
    public int globalTopicCount() {
        return this.topics;
    }

    @Override
    public void setGlobalPartitionCount(int partitionCount) {
        this.partitions = partitionCount;
    }

    @Override
    public int globalPartitionCount() {
        return this.partitions;
    }

    @Override
    public void setOfflinePartitionCount(int offlinePartitions) {
        this.offlinePartitions = offlinePartitions;
    }

    @Override
    public int offlinePartitionCount() {
        return this.offlinePartitions;
    }

    @Override
    public void setPreferredReplicaImbalanceCount(int replicaImbalances) {
        this.preferredReplicaImbalances = replicaImbalances;
    }

    @Override
    public int preferredReplicaImbalanceCount() {
        return this.preferredReplicaImbalances;
    }

    @Override
    public void incrementMetadataErrorCount() {
        this.metadataErrors.getAndIncrement();
    }

    @Override
    public int metadataErrorCount() {
        return this.metadataErrors.get();
    }

    @Override
    public void setLastAppliedRecordOffset(long offset) {
        lastAppliedRecordOffset = offset;
    }

    @Override
    public long lastAppliedRecordOffset() {
        return lastAppliedRecordOffset;
    }

    @Override
    public void setLastCommittedRecordOffset(long offset) {
        lastCommittedRecordOffset = offset;
    }

    @Override
    public long lastCommittedRecordOffset() {
        return lastCommittedRecordOffset;
    }

    @Override
    public void setLastAppliedRecordTimestamp(long timestamp) {
        lastAppliedRecordTimestamp = timestamp;
    }

    @Override
    public long lastAppliedRecordTimestamp() {
        return lastAppliedRecordTimestamp;
    }

    @Override
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return this.closed;
    }
}
