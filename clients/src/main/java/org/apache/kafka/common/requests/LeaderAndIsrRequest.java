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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
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
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class LeaderAndIsrRequest extends AbstractRequest {
    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    private static final String PARTITION_STATES_KEY_NAME = "partition_states";
    private static final String LIVE_LEADERS_KEY_NAME = "live_leaders";

    // partition_states key names
    private static final String LEADER_KEY_NAME = "leader";
    private static final String LEADER_EPOCH_KEY_NAME = "leader_epoch";
    private static final String ISR_KEY_NAME = "isr";
    private static final String ZK_VERSION_KEY_NAME = "zk_version";
    private static final String REPLICAS_KEY_NAME = "replicas";
    private static final String IS_NEW_KEY_NAME = "is_new";

    // live_leaders key names
    private static final String END_POINT_ID_KEY_NAME = "id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";

    private static final Schema LEADER_AND_ISR_REQUEST_PARTITION_STATE_V0 = new Schema(
            TOPIC_NAME,
            PARTITION_ID,
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(LEADER_KEY_NAME, INT32, "The broker id for the leader."),
            new Field(LEADER_EPOCH_KEY_NAME, INT32, "The leader epoch."),
            new Field(ISR_KEY_NAME, new ArrayOf(INT32), "The in sync replica ids."),
            new Field(ZK_VERSION_KEY_NAME, INT32, "The ZK version."),
            new Field(REPLICAS_KEY_NAME, new ArrayOf(INT32), "The replica ids."));

    // LEADER_AND_ISR_REQUEST_PARTITION_STATE_V1 added a per-partition is_new Field.
    // This field specifies whether the replica should have existed on the broker or not.
    private static final Schema LEADER_AND_ISR_REQUEST_PARTITION_STATE_V1 = new Schema(
            TOPIC_NAME,
            PARTITION_ID,
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(LEADER_KEY_NAME, INT32, "The broker id for the leader."),
            new Field(LEADER_EPOCH_KEY_NAME, INT32, "The leader epoch."),
            new Field(ISR_KEY_NAME, new ArrayOf(INT32), "The in sync replica ids."),
            new Field(ZK_VERSION_KEY_NAME, INT32, "The ZK version."),
            new Field(REPLICAS_KEY_NAME, new ArrayOf(INT32), "The replica ids."),
            new Field(IS_NEW_KEY_NAME, BOOLEAN, "Whether the replica should have existed on the broker or not"));

    private static final Schema LEADER_AND_ISR_REQUEST_LIVE_LEADER_V0 = new Schema(
            new Field(END_POINT_ID_KEY_NAME, INT32, "The broker id."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."));

    private static final Schema LEADER_AND_ISR_REQUEST_V0 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(LEADER_AND_ISR_REQUEST_PARTITION_STATE_V0)),
            new Field(LIVE_LEADERS_KEY_NAME, new ArrayOf(LEADER_AND_ISR_REQUEST_LIVE_LEADER_V0)));

    // LEADER_AND_ISR_REQUEST_V1 added a per-partition is_new Field. This field specifies whether the replica should
    // have existed on the broker or not.
    private static final Schema LEADER_AND_ISR_REQUEST_V1 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(LEADER_AND_ISR_REQUEST_PARTITION_STATE_V1)),
            new Field(LIVE_LEADERS_KEY_NAME, new ArrayOf(LEADER_AND_ISR_REQUEST_LIVE_LEADER_V0)));

    public static Schema[] schemaVersions() {
        return new Schema[]{LEADER_AND_ISR_REQUEST_V0, LEADER_AND_ISR_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<LeaderAndIsrRequest> {
        private final int controllerId;
        private final int controllerEpoch;
        private final Map<TopicPartition, PartitionState> partitionStates;
        private final Set<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch,
                       Map<TopicPartition, PartitionState> partitionStates, Set<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            return new LeaderAndIsrRequest(controllerId, controllerEpoch, partitionStates, liveLeaders, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();
        }
    }

    private final int controllerId;
    private final int controllerEpoch;
    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Set<Node> liveLeaders;

    private LeaderAndIsrRequest(int controllerId, int controllerEpoch, Map<TopicPartition, PartitionState> partitionStates,
                                Set<Node> liveLeaders, short version) {
        super(version);
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStates = partitionStates;
        this.liveLeaders = liveLeaders;
    }

    public LeaderAndIsrRequest(Struct struct, short version) {
        super(version);

        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        for (Object partitionStateDataObj : struct.getArray(PARTITION_STATES_KEY_NAME)) {
            Struct partitionStateData = (Struct) partitionStateDataObj;
            String topic = partitionStateData.get(TOPIC_NAME);
            int partition = partitionStateData.get(PARTITION_ID);
            int controllerEpoch = partitionStateData.getInt(CONTROLLER_EPOCH_KEY_NAME);
            int leader = partitionStateData.getInt(LEADER_KEY_NAME);
            int leaderEpoch = partitionStateData.getInt(LEADER_EPOCH_KEY_NAME);

            Object[] isrArray = partitionStateData.getArray(ISR_KEY_NAME);
            List<Integer> isr = new ArrayList<>(isrArray.length);
            for (Object r : isrArray)
                isr.add((Integer) r);

            int zkVersion = partitionStateData.getInt(ZK_VERSION_KEY_NAME);

            Object[] replicasArray = partitionStateData.getArray(REPLICAS_KEY_NAME);
            List<Integer> replicas = new ArrayList<>(replicasArray.length);
            for (Object r : replicasArray)
                replicas.add((Integer) r);
            boolean isNew = partitionStateData.hasField(IS_NEW_KEY_NAME) ? partitionStateData.getBoolean(IS_NEW_KEY_NAME) : false;

            PartitionState partitionState = new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas, isNew);
            partitionStates.put(new TopicPartition(topic, partition), partitionState);
        }

        Set<Node> leaders = new HashSet<>();
        for (Object leadersDataObj : struct.getArray(LIVE_LEADERS_KEY_NAME)) {
            Struct leadersData = (Struct) leadersDataObj;
            int id = leadersData.getInt(END_POINT_ID_KEY_NAME);
            String host = leadersData.getString(HOST_KEY_NAME);
            int port = leadersData.getInt(PORT_KEY_NAME);
            leaders.add(new Node(id, host, port));
        }

        controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);
        controllerEpoch = struct.getInt(CONTROLLER_EPOCH_KEY_NAME);
        this.partitionStates = partitionStates;
        this.liveLeaders = leaders;
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.LEADER_AND_ISR.requestSchema(version));
        struct.set(CONTROLLER_ID_KEY_NAME, controllerId);
        struct.set(CONTROLLER_EPOCH_KEY_NAME, controllerEpoch);

        List<Struct> partitionStatesData = new ArrayList<>(partitionStates.size());
        for (Map.Entry<TopicPartition, PartitionState> entry : partitionStates.entrySet()) {
            Struct partitionStateData = struct.instance(PARTITION_STATES_KEY_NAME);
            TopicPartition topicPartition = entry.getKey();
            partitionStateData.set(TOPIC_NAME, topicPartition.topic());
            partitionStateData.set(PARTITION_ID, topicPartition.partition());
            PartitionState partitionState = entry.getValue();
            partitionStateData.set(CONTROLLER_EPOCH_KEY_NAME, partitionState.basePartitionState.controllerEpoch);
            partitionStateData.set(LEADER_KEY_NAME, partitionState.basePartitionState.leader);
            partitionStateData.set(LEADER_EPOCH_KEY_NAME, partitionState.basePartitionState.leaderEpoch);
            partitionStateData.set(ISR_KEY_NAME, partitionState.basePartitionState.isr.toArray());
            partitionStateData.set(ZK_VERSION_KEY_NAME, partitionState.basePartitionState.zkVersion);
            partitionStateData.set(REPLICAS_KEY_NAME, partitionState.basePartitionState.replicas.toArray());
            if (partitionStateData.hasField(IS_NEW_KEY_NAME))
                partitionStateData.set(IS_NEW_KEY_NAME, partitionState.isNew);
            partitionStatesData.add(partitionStateData);
        }
        struct.set(PARTITION_STATES_KEY_NAME, partitionStatesData.toArray());

        List<Struct> leadersData = new ArrayList<>(liveLeaders.size());
        for (Node leader : liveLeaders) {
            Struct leaderData = struct.instance(LIVE_LEADERS_KEY_NAME);
            leaderData.set(END_POINT_ID_KEY_NAME, leader.id());
            leaderData.set(HOST_KEY_NAME, leader.host());
            leaderData.set(PORT_KEY_NAME, leader.port());
            leadersData.add(leaderData);
        }
        struct.set(LIVE_LEADERS_KEY_NAME, leadersData.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, Errors> responses = new HashMap<>(partitionStates.size());
        for (TopicPartition partition : partitionStates.keySet()) {
            responses.put(partition, Errors.forException(e));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new LeaderAndIsrResponse(Errors.NONE, responses);
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
    }

}
