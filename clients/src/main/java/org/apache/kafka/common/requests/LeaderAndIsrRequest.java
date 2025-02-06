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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.utils.FlattenedIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class LeaderAndIsrRequest {

    /**
     * Indicates if a controller request is incremental, full, or unknown.
     */
    public enum Type {
        UNKNOWN(0),
        INCREMENTAL(1),
        FULL(2);

        private final byte type;
        Type(int type) {
            this.type = (byte) type;
        }

        public byte toByte() {
            return type;
        }
    }

    public static class Builder {
        protected final int controllerId;
        protected final int controllerEpoch;
        protected final long brokerEpoch;
        private final List<PartitionState> partitionStates;
        private final Map<String, Uuid> topicIds;
        private final Collection<Node> liveLeaders;
        private final Type updateType;

        public Builder(int controllerId, int controllerEpoch, long brokerEpoch,
                       List<PartitionState> partitionStates, Map<String, Uuid> topicIds,
                       Collection<Node> liveLeaders) {
            this(controllerId, controllerEpoch, brokerEpoch, partitionStates, topicIds, liveLeaders, Type.UNKNOWN);
        }

        public Builder(int controllerId, int controllerEpoch, long brokerEpoch,
                       List<PartitionState> partitionStates, Map<String, Uuid> topicIds,
                       Collection<Node> liveLeaders, Type updateType) {
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
            this.partitionStates = partitionStates;
            this.topicIds = topicIds;
            this.liveLeaders = liveLeaders;
            this.updateType = updateType;
        }

        public LeaderAndIsrRequest build() {
            return new LeaderAndIsrRequest(this);
        }

        @Override
        public String toString() {
            return "(type=LeaderAndIsRequest" +
                    ", controllerId=" + controllerId +
                    ", controllerEpoch=" + controllerEpoch +
                    ", brokerEpoch=" + brokerEpoch +
                    ", partitionStates=" + partitionStates +
                    ", topicIds=" + topicIds +
                    ", liveLeaders=(" + liveLeaders.stream().map(Node::toString).collect(Collectors.joining(", ")) + ")" +
                    ")";

        }
    }

    private final int controllerId;
    private final int controllerEpoch;
    private final long brokerEpoch;
    private final List<Node> liveLeaders;
    private final List<TopicState> topicStates;
    private final Type requestType;

    public LeaderAndIsrRequest(Builder builder) {
        this.controllerId = builder.controllerId;
        this.controllerEpoch = builder.controllerEpoch;
        this.brokerEpoch = builder.brokerEpoch;
        this.requestType = builder.updateType;
        this.liveLeaders = new ArrayList<>(builder.liveLeaders);
        this.topicStates = new ArrayList<>(groupByTopic(builder.partitionStates, builder.topicIds).values());
    }

    private static Map<String, TopicState> groupByTopic(List<PartitionState> partitionStates, Map<String, Uuid> topicIds) {
        Map<String, TopicState> topicStates = new HashMap<>();
        for (PartitionState partition : partitionStates) {
            TopicState topicState = topicStates.computeIfAbsent(partition.topicName(), t -> {
                var topic = new TopicState();
                topic.topicName = partition.topicName();
                topic.topicId = topicIds.getOrDefault(partition.topicName(), Uuid.ZERO_UUID);
                return topic;
            });
            topicState.partitionStates().add(partition);
        }
        return topicStates;
    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public Iterable<PartitionState> partitionStates() {
        return () -> new FlattenedIterator<>(topicStates.iterator(),
                topicState -> topicState.partitionStates().iterator());
    }

    public Map<String, Uuid> topicIds() {
        return topicStates.stream()
                .collect(Collectors.toMap(TopicState::topicName, TopicState::topicId));
    }

    public List<Node> liveLeaders() {
        return Collections.unmodifiableList(liveLeaders);
    }

    public Type requestType() {
        return requestType;
    }

    public LeaderAndIsrResponse getErrorResponse(Exception e) {
        LinkedHashMap<Uuid, List<LeaderAndIsrResponse.PartitionError>> errorsMap = new LinkedHashMap<>();
        Errors error = Errors.forException(e);

        for (TopicState topicState : topicStates) {
            List<LeaderAndIsrResponse.PartitionError> partitions = new ArrayList<>(topicState.partitionStates().size());
            for (PartitionState partition : topicState.partitionStates()) {
                partitions.add(new LeaderAndIsrResponse.PartitionError(partition.partitionIndex, error.code()));
            }
            errorsMap.put(topicState.topicId, partitions);
        }

        return new LeaderAndIsrResponse(error, errorsMap);

    }

    public static class TopicState {
        String topicName;
        Uuid topicId;
        List<PartitionState> partitionStates;

        public TopicState() {
            this.topicName = "";
            this.topicId = Uuid.ZERO_UUID;
            this.partitionStates = new ArrayList<>(0);
        }

        public String topicName() {
            return this.topicName;
        }

        public Uuid topicId() {
            return this.topicId;
        }

        public List<PartitionState> partitionStates() {
            return this.partitionStates;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass())
                return false;
            TopicState that = (TopicState) o;
            return Objects.equals(topicName, that.topicName) &&
                    Objects.equals(topicId, that.topicId) &&
                    Objects.equals(partitionStates, that.partitionStates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName, topicId, partitionStates);
        }

        @Override
        public String toString() {
            return "LeaderAndIsrTopicState("
                    + "topicName='" + topicName + "'"
                    + ", topicId=" + topicId
                    + ", partitionStates=" + MessageUtil.deepToString(partitionStates.iterator())
                    + ")";
        }
    }

    public static class PartitionState {
        String topicName;
        int partitionIndex;
        int controllerEpoch;
        int leader;
        int leaderEpoch;
        List<Integer> isr;
        int partitionEpoch;
        List<Integer> replicas;
        List<Integer> addingReplicas;
        List<Integer> removingReplicas;
        boolean isNew;
        byte leaderRecoveryState;

        public PartitionState() {
            this.topicName = "";
            this.partitionIndex = 0;
            this.controllerEpoch = 0;
            this.leader = 0;
            this.leaderEpoch = 0;
            this.isr = new ArrayList<>(0);
            this.partitionEpoch = 0;
            this.replicas = new ArrayList<>(0);
            this.addingReplicas = new ArrayList<>(0);
            this.removingReplicas = new ArrayList<>(0);
            this.isNew = false;
            this.leaderRecoveryState = (byte) 0;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            PartitionState that = (PartitionState) o;
            return partitionIndex == that.partitionIndex &&
                    controllerEpoch == that.controllerEpoch &&
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
            return Objects.hash(topicName, partitionIndex, controllerEpoch, leader, leaderEpoch, isr, partitionEpoch,
                    replicas, addingReplicas, removingReplicas, isNew, leaderRecoveryState);
        }

        @Override
        public String toString() {
            return "LeaderAndIsrPartitionState("
                    + "topicName='" + topicName + "'"
                    + ", partitionIndex=" + partitionIndex
                    + ", controllerEpoch=" + controllerEpoch
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

        public int controllerEpoch() {
            return this.controllerEpoch;
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

        public PartitionState setControllerEpoch(int v) {
            this.controllerEpoch = v;
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
}
