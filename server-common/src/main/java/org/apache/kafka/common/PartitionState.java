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


package org.apache.kafka.common;

import org.apache.kafka.common.protocol.MessageUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PartitionState {
    String topicName = "";
    int partitionIndex = 0;
    int leader = 0;
    int leaderEpoch = 0;
    List<Integer> isr = new ArrayList<>(0);
    int partitionEpoch = 0;
    List<Integer> replicas = new ArrayList<>(0);
    List<Integer> addingReplicas = new ArrayList<>(0);
    List<Integer> removingReplicas = new ArrayList<>(0);
    boolean isNew = false;
    byte leaderRecoveryState = (byte) 0;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PartitionState that = (PartitionState) o;
        return partitionIndex == that.partitionIndex &&
                leader == that.leader &&
                leaderEpoch == that.leaderEpoch &&
                partitionEpoch == that.partitionEpoch &&
                isNew == that.isNew &&
                leaderRecoveryState == that.leaderRecoveryState &&
                Objects.equals(topicName, that.topicName) &&
                Objects.equals(isr, that.isr) &&
                Objects.equals(replicas, that.replicas) &&
                Objects.equals(addingReplicas, that.addingReplicas) &&
                Objects.equals(removingReplicas, that.removingReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, partitionIndex, leader, leaderEpoch, isr, partitionEpoch,
                replicas, addingReplicas, removingReplicas, isNew, leaderRecoveryState);
    }

    @Override
    public String toString() {
        return "PartitionState("
                + "topicName='" + topicName + "'"
                + ", partitionIndex=" + partitionIndex
                + ", leader=" + leader
                + ", leaderEpoch=" + leaderEpoch
                + ", isr=" + MessageUtil.deepToString(isr.iterator())
                + ", partitionEpoch=" + partitionEpoch
                + ", replicas=" + MessageUtil.deepToString(replicas.iterator())
                + ", addingReplicas=" + MessageUtil.deepToString(addingReplicas.iterator())
                + ", removingReplicas=" + MessageUtil.deepToString(removingReplicas.iterator())
                + ", isNew=" + (isNew ? "true" : "false")
                + ", leaderRecoveryState=" + leaderRecoveryState
                + ")";
    }

    public String topicName() {
        return this.topicName;
    }

    public int partitionIndex() {
        return this.partitionIndex;
    }

    public int leader() {
        return this.leader;
    }

    public int leaderEpoch() {
        return this.leaderEpoch;
    }

    public List<Integer> isr() {
        return this.isr;
    }

    public int partitionEpoch() {
        return this.partitionEpoch;
    }

    public List<Integer> replicas() {
        return this.replicas;
    }

    public List<Integer> addingReplicas() {
        return this.addingReplicas;
    }

    public List<Integer> removingReplicas() {
        return this.removingReplicas;
    }

    public boolean isNew() {
        return this.isNew;
    }

    public byte leaderRecoveryState() {
        return this.leaderRecoveryState;
    }

    public PartitionState setTopicName(String v) {
        this.topicName = v;
        return this;
    }

    public PartitionState setPartitionIndex(int v) {
        this.partitionIndex = v;
        return this;
    }

    public PartitionState setLeader(int v) {
        this.leader = v;
        return this;
    }

    public PartitionState setLeaderEpoch(int v) {
        this.leaderEpoch = v;
        return this;
    }

    public PartitionState setIsr(List<Integer> v) {
        this.isr = v;
        return this;
    }

    public PartitionState setPartitionEpoch(int v) {
        this.partitionEpoch = v;
        return this;
    }

    public PartitionState setReplicas(List<Integer> v) {
        this.replicas = v;
        return this;
    }

    public PartitionState setAddingReplicas(List<Integer> v) {
        this.addingReplicas = v;
        return this;
    }

    public PartitionState setRemovingReplicas(List<Integer> v) {
        this.removingReplicas = v;
        return this;
    }

    public PartitionState setIsNew(boolean v) {
        this.isNew = v;
        return this;
    }

    public PartitionState setLeaderRecoveryState(byte v) {
        this.leaderRecoveryState = v;
        return this;
    }
}