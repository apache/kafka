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
import org.apache.kafka.common.requests.AbstractControlRequest.Type;
import org.apache.kafka.common.utils.FlattenedIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class LeaderAndIsrRequest {

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
            TopicState topicState = topicStates.computeIfAbsent(partition.topicName(), t -> new TopicState()
                .setTopicName(partition.topicName())
                .setTopicId(topicIds.getOrDefault(partition.topicName(), Uuid.ZERO_UUID)));
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

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicState)) return false;
            TopicState other = (TopicState) obj;
            if (this.topicName == null) {
                if (other.topicName != null) return false;
            } else {
                if (!this.topicName.equals(other.topicName)) return false;
            }
            if (!this.topicId.equals(other.topicId)) return false;
            if (this.partitionStates == null) {
                if (other.partitionStates != null) return false;
            } else {
                if (!this.partitionStates.equals(other.partitionStates)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topicName == null ? 0 : topicName.hashCode());
            hashCode = 31 * hashCode + topicId.hashCode();
            hashCode = 31 * hashCode + (partitionStates == null ? 0 : partitionStates.hashCode());
            return hashCode;
        }

        @Override
        public String toString() {
            return "LeaderAndIsrTopicState("
                    + "topicName=" + ((topicName == null) ? "null" : "'" + topicName.toString() + "'")
                    + ", topicId=" + topicId.toString()
                    + ", partitionStates=" + MessageUtil.deepToString(partitionStates.iterator())
                    + ")";
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

        public TopicState setTopicName(String v) {
            this.topicName = v;
            return this;
        }

        public TopicState setTopicId(Uuid v) {
            this.topicId = v;
            return this;
        }

        public TopicState setPartitionStates(List<PartitionState> v) {
            this.partitionStates = v;
            return this;
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

        @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionState)) return false;
            PartitionState other = (PartitionState) obj;
            if (this.topicName == null) {
                if (other.topicName != null) return false;
            } else {
                if (!this.topicName.equals(other.topicName)) return false;
            }
            if (partitionIndex != other.partitionIndex) return false;
            if (controllerEpoch != other.controllerEpoch) return false;
            if (leader != other.leader) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            if (this.isr == null) {
                if (other.isr != null) return false;
            } else {
                if (!this.isr.equals(other.isr)) return false;
            }
            if (partitionEpoch != other.partitionEpoch) return false;
            if (this.replicas == null) {
                if (other.replicas != null) return false;
            } else {
                if (!this.replicas.equals(other.replicas)) return false;
            }
            if (this.addingReplicas == null) {
                if (other.addingReplicas != null) return false;
            } else {
                if (!this.addingReplicas.equals(other.addingReplicas)) return false;
            }
            if (this.removingReplicas == null) {
                if (other.removingReplicas != null) return false;
            } else {
                if (!this.removingReplicas.equals(other.removingReplicas)) return false;
            }
            if (isNew != other.isNew) return false;
            if (leaderRecoveryState != other.leaderRecoveryState) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topicName == null ? 0 : topicName.hashCode());
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + controllerEpoch;
            hashCode = 31 * hashCode + leader;
            hashCode = 31 * hashCode + leaderEpoch;
            hashCode = 31 * hashCode + (isr == null ? 0 : isr.hashCode());
            hashCode = 31 * hashCode + partitionEpoch;
            hashCode = 31 * hashCode + (replicas == null ? 0 : replicas.hashCode());
            hashCode = 31 * hashCode + (addingReplicas == null ? 0 : addingReplicas.hashCode());
            hashCode = 31 * hashCode + (removingReplicas == null ? 0 : removingReplicas.hashCode());
            hashCode = 31 * hashCode + (isNew ? 1231 : 1237);
            hashCode = 31 * hashCode + leaderRecoveryState;
            return hashCode;
        }

        @Override
        public String toString() {
            return "LeaderAndIsrPartitionState("
                    + "topicName=" + ((topicName == null) ? "null" : "'" + topicName + "'")
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
