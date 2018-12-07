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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class LeaderAndIsrRequest extends AbstractControlRequest {
    private static final Field.ComplexArray TOPIC_STATES = new Field.ComplexArray("topic_states", "Topic states");
    private static final Field.ComplexArray PARTITION_STATES = new Field.ComplexArray("partition_states", "Partition states");
    private static final Field.ComplexArray LIVE_LEADERS = new Field.ComplexArray("live_leaders", "Live leaders");

    // PartitionState fields
    private static final Field.Int32 LEADER = new Field.Int32("leader", "The broker id for the leader.");
    private static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch", "The leader epoch.");
    private static final Field.Array ISR = new Field.Array("isr", INT32, "The in sync replica ids.");
    private static final Field.Int32 ZK_VERSION = new Field.Int32("zk_version", "The ZK version.");
    private static final Field.Array REPLICAS = new Field.Array("replicas", INT32, "The replica ids.");
    private static final Field.Bool IS_NEW = new Field.Bool("is_new", "Whether the replica should have existed on the broker or not");

    // live_leaders fields
    private static final Field.Int32 END_POINT_ID = new Field.Int32("id", "The broker id");
    private static final Field.Str HOST = new Field.Str("host", "The hostname of the broker.");
    private static final Field.Int32  PORT = new Field.Int32("port", "The port on which the broker accepts requests.");

    private static final Field PARTITION_STATES_V0  = PARTITION_STATES.withFields(
            TOPIC_NAME,
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS);

    // PARTITION_STATES_V1 added a per-partition is_new Field.
    // This field specifies whether the replica should have existed on the broker or not.
    private static final Field PARTITION_STATES_V1  = PARTITION_STATES.withFields(
            TOPIC_NAME,
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS,
            IS_NEW);

    private static final Field PARTITION_STATES_V2  = PARTITION_STATES.withFields(
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS,
            IS_NEW);

    // TOPIC_STATES_V2 normalizes TOPIC_STATES_V1 to make it more memory efficient
    private static final Field TOPIC_STATES_V2 = TOPIC_STATES.withFields(
            TOPIC_NAME,
            PARTITION_STATES_V2);

    private static final Field LIVE_LEADERS_V0 = LIVE_LEADERS.withFields(
            END_POINT_ID,
            HOST,
            PORT);

    private static final Schema LEADER_AND_ISR_REQUEST_V0 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V0,
            LIVE_LEADERS_V0);

    // LEADER_AND_ISR_REQUEST_V1 added a per-partition is_new Field. This field specifies whether the replica should
    // have existed on the broker or not.
    private static final Schema LEADER_AND_ISR_REQUEST_V1 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V1,
            LIVE_LEADERS_V0);

    // LEADER_AND_ISR_REQUEST_V2 added a broker_epoch Field. This field specifies the generation of the broker across
    // bounces. It also normalizes partitions under each topic.
    private static final Schema LEADER_AND_ISR_REQUEST_V2 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            BROKER_EPOCH,
            TOPIC_STATES_V2,
            LIVE_LEADERS_V0);

    public static Schema[] schemaVersions() {
        return new Schema[]{LEADER_AND_ISR_REQUEST_V0, LEADER_AND_ISR_REQUEST_V1, LEADER_AND_ISR_REQUEST_V2};
    }

    public static class Builder extends AbstractControlRequest.Builder<LeaderAndIsrRequest> {
        private final Map<TopicPartition, PartitionState> partitionStates;
        private final Set<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       Map<TopicPartition, PartitionState> partitionStates, Set<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            return new LeaderAndIsrRequest(controllerId, controllerEpoch, brokerEpoch, partitionStates, liveLeaders, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();
        }
    }

    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Set<Node> liveLeaders;

    private LeaderAndIsrRequest(int controllerId, int controllerEpoch, long brokerEpoch, Map<TopicPartition, PartitionState> partitionStates,
                                Set<Node> liveLeaders, short version) {
        super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
        this.partitionStates = partitionStates;
        this.liveLeaders = liveLeaders;
    }

    public LeaderAndIsrRequest(Struct struct, short version) {
        super(ApiKeys.LEADER_AND_ISR, struct, version);

        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        if (struct.hasField(TOPIC_STATES)) {
            for (Object topicStatesDataObj : struct.get(TOPIC_STATES)) {
                Struct topicStatesData = (Struct) topicStatesDataObj;
                String topic = topicStatesData.get(TOPIC_NAME);
                for (Object partitionStateDataObj : topicStatesData.get(PARTITION_STATES)) {
                    Struct partitionStateData = (Struct) partitionStateDataObj;
                    int partition = partitionStateData.get(PARTITION_ID);
                    PartitionState partitionState = new PartitionState(partitionStateData);
                    partitionStates.put(new TopicPartition(topic, partition), partitionState);
                }
            }
        } else {
            for (Object partitionStateDataObj : struct.get(PARTITION_STATES)) {
                Struct partitionStateData = (Struct) partitionStateDataObj;
                String topic = partitionStateData.get(TOPIC_NAME);
                int partition = partitionStateData.get(PARTITION_ID);
                PartitionState partitionState = new PartitionState(partitionStateData);
                partitionStates.put(new TopicPartition(topic, partition), partitionState);
            }
        }

        Set<Node> leaders = new HashSet<>();
        for (Object leadersDataObj : struct.get(LIVE_LEADERS)) {
            Struct leadersData = (Struct) leadersDataObj;
            int id = leadersData.get(END_POINT_ID);
            String host = leadersData.get(HOST);
            int port = leadersData.get(PORT);
            leaders.add(new Node(id, host, port));
        }

        this.partitionStates = partitionStates;
        this.liveLeaders = leaders;
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.LEADER_AND_ISR.requestSchema(version));
        struct.set(CONTROLLER_ID, controllerId);
        struct.set(CONTROLLER_EPOCH, controllerEpoch);
        struct.setIfExists(BROKER_EPOCH, brokerEpoch);

        if (struct.hasField(TOPIC_STATES)) {
            Map<String, Map<Integer, PartitionState>> topicStates = CollectionUtils.groupPartitionDataByTopic(partitionStates);
            List<Struct> topicStatesData = new ArrayList<>(topicStates.size());
            for (Map.Entry<String, Map<Integer, PartitionState>> entry : topicStates.entrySet()) {
                Struct topicStateData = struct.instance(TOPIC_STATES);
                topicStateData.set(TOPIC_NAME, entry.getKey());
                Map<Integer, PartitionState> partitionMap = entry.getValue();
                List<Struct> partitionStatesData = new ArrayList<>(partitionMap.size());
                for (Map.Entry<Integer, PartitionState> partitionEntry : partitionMap.entrySet()) {
                    Struct partitionStateData = topicStateData.instance(PARTITION_STATES);
                    partitionStateData.set(PARTITION_ID, partitionEntry.getKey());
                    partitionEntry.getValue().setStruct(partitionStateData);
                    partitionStatesData.add(partitionStateData);
                }
                topicStateData.set(PARTITION_STATES, partitionStatesData.toArray());
                topicStatesData.add(topicStateData);
            }
            struct.set(TOPIC_STATES, topicStatesData.toArray());
        } else {
            List<Struct> partitionStatesData = new ArrayList<>(partitionStates.size());
            for (Map.Entry<TopicPartition, PartitionState> entry : partitionStates.entrySet()) {
                Struct partitionStateData = struct.instance(PARTITION_STATES);
                TopicPartition topicPartition = entry.getKey();
                partitionStateData.set(TOPIC_NAME, topicPartition.topic());
                partitionStateData.set(PARTITION_ID, topicPartition.partition());
                entry.getValue().setStruct(partitionStateData);
                partitionStatesData.add(partitionStateData);
            }
            struct.set(PARTITION_STATES, partitionStatesData.toArray());
        }

        List<Struct> leadersData = new ArrayList<>(liveLeaders.size());
        for (Node leader : liveLeaders) {
            Struct leaderData = struct.instance(LIVE_LEADERS);
            leaderData.set(END_POINT_ID, leader.id());
            leaderData.set(HOST, leader.host());
            leaderData.set(PORT, leader.port());
            leadersData.add(leaderData);
        }
        struct.set(LIVE_LEADERS, leadersData.toArray());
        return struct;
    }

    @Override
    public LeaderAndIsrResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        Map<TopicPartition, Errors> responses = new HashMap<>(partitionStates.size());
        for (TopicPartition partition : partitionStates.keySet()) {
            responses.put(partition, error);
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new LeaderAndIsrResponse(error, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LEADER_AND_ISR.latestVersion()));
        }
    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public Map<TopicPartition, PartitionState> partitionStates() {
        return partitionStates;
    }

    public Set<Node> liveLeaders() {
        return liveLeaders;
    }

    public static LeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrRequest(ApiKeys.LEADER_AND_ISR.parseRequest(version, buffer), version);
    }

    public static final class PartitionState {
        public final BasePartitionState basePartitionState;
        public final boolean isNew;

        public PartitionState(int controllerEpoch,
                              int leader,
                              int leaderEpoch,
                              List<Integer> isr,
                              int zkVersion,
                              List<Integer> replicas,
                              boolean isNew) {
            this.basePartitionState = new BasePartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas);
            this.isNew = isNew;
        }

        private PartitionState(Struct struct) {
            int controllerEpoch = struct.get(CONTROLLER_EPOCH);
            int leader = struct.get(LEADER);
            int leaderEpoch = struct.get(LEADER_EPOCH);

            Object[] isrArray = struct.get(ISR);
            List<Integer> isr = new ArrayList<>(isrArray.length);
            for (Object r : isrArray)
                isr.add((Integer) r);

            int zkVersion = struct.get(ZK_VERSION);

            Object[] replicasArray = struct.get(REPLICAS);
            List<Integer> replicas = new ArrayList<>(replicasArray.length);
            for (Object r : replicasArray)
                replicas.add((Integer) r);

            this.basePartitionState = new BasePartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas);
            this.isNew = struct.getOrElse(IS_NEW, false);
        }

        @Override
        public String toString() {
            return "PartitionState(controllerEpoch=" + basePartitionState.controllerEpoch +
                ", leader=" + basePartitionState.leader +
                ", leaderEpoch=" + basePartitionState.leaderEpoch +
                ", isr=" + Utils.join(basePartitionState.isr, ",") +
                ", zkVersion=" + basePartitionState.zkVersion +
                ", replicas=" + Utils.join(basePartitionState.replicas, ",") +
                ", isNew=" + isNew + ")";
        }

        private void setStruct(Struct struct) {
            struct.set(CONTROLLER_EPOCH, basePartitionState.controllerEpoch);
            struct.set(LEADER, basePartitionState.leader);
            struct.set(LEADER_EPOCH, basePartitionState.leaderEpoch);
            struct.set(ISR, basePartitionState.isr.toArray());
            struct.set(ZK_VERSION, basePartitionState.zkVersion);
            struct.set(REPLICAS, basePartitionState.replicas.toArray());
            struct.setIfExists(IS_NEW, isNew);
        }
    }

}
