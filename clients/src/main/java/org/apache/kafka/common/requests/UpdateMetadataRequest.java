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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class UpdateMetadataRequest extends AbstractRequest {

    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    private static final String PARTITION_STATES_KEY_NAME = "partition_states";
    private static final String LIVE_BROKERS_KEY_NAME = "live_brokers";

    // PartitionState key names
    private static final String LEADER_KEY_NAME = "leader";
    private static final String LEADER_EPOCH_KEY_NAME = "leader_epoch";
    private static final String ISR_KEY_NAME = "isr";
    private static final String ZK_VERSION_KEY_NAME = "zk_version";
    private static final String REPLICAS_KEY_NAME = "replicas";
    private static final String OFFLINE_REPLICAS_KEY_NAME = "offline_replicas";

    // Broker key names
    private static final String BROKER_ID_KEY_NAME = "id";
    private static final String ENDPOINTS_KEY_NAME = "end_points";
    private static final String RACK_KEY_NAME = "rack";

    // EndPoint key names
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";
    private static final String LISTENER_NAME_KEY_NAME = "listener_name";
    private static final String SECURITY_PROTOCOL_TYPE_KEY_NAME = "security_protocol_type";

    private static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V0 = new Schema(
                    TOPIC_NAME,
                    PARTITION_ID,
                    new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
                    new Field(LEADER_KEY_NAME, INT32, "The broker id for the leader."),
                    new Field(LEADER_EPOCH_KEY_NAME, INT32, "The leader epoch."),
                    new Field(ISR_KEY_NAME, new ArrayOf(INT32), "The in sync replica ids."),
                    new Field(ZK_VERSION_KEY_NAME, INT32, "The ZK version."),
                    new Field(REPLICAS_KEY_NAME, new ArrayOf(INT32), "The replica ids."));

    private static final Schema UPDATE_METADATA_REQUEST_BROKER_V0 = new Schema(
            new Field(BROKER_ID_KEY_NAME, INT32, "The broker id."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."));

    private static final Schema UPDATE_METADATA_REQUEST_V0 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V0)),
            new Field(LIVE_BROKERS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V0)));

    private static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V1 = UPDATE_METADATA_REQUEST_PARTITION_STATE_V0;

    // for some reason, V1 sends `port` before `host` while V0 sends `host` before `port
    private static final Schema UPDATE_METADATA_REQUEST_END_POINT_V1 = new Schema(
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(SECURITY_PROTOCOL_TYPE_KEY_NAME, INT16, "The security protocol type."));

    private static final Schema UPDATE_METADATA_REQUEST_BROKER_V1 = new Schema(
            new Field(BROKER_ID_KEY_NAME, INT32, "The broker id."),
            new Field(ENDPOINTS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_END_POINT_V1)));

    private static final Schema UPDATE_METADATA_REQUEST_V1 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V1)),
            new Field(LIVE_BROKERS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V1)));

    private static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V2 = UPDATE_METADATA_REQUEST_PARTITION_STATE_V1;

    private static final Schema UPDATE_METADATA_REQUEST_END_POINT_V2 = UPDATE_METADATA_REQUEST_END_POINT_V1;

    private static final Schema UPDATE_METADATA_REQUEST_BROKER_V2 = new Schema(
            new Field(BROKER_ID_KEY_NAME, INT32, "The broker id."),
            new Field(ENDPOINTS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_END_POINT_V2)),
            new Field(RACK_KEY_NAME, NULLABLE_STRING, "The rack"));

    private static final Schema UPDATE_METADATA_REQUEST_V2 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V2)),
            new Field(LIVE_BROKERS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V2)));

    private static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V3 = UPDATE_METADATA_REQUEST_PARTITION_STATE_V2;

    // UPDATE_METADATA_REQUEST_PARTITION_STATE_V4 added a per-partition offline_replicas field. This field specifies
    // the list of replicas that are offline.
    private static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V4 = new Schema(
            TOPIC_NAME,
            PARTITION_ID,
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(LEADER_KEY_NAME, INT32, "The broker id for the leader."),
            new Field(LEADER_EPOCH_KEY_NAME, INT32, "The leader epoch."),
            new Field(ISR_KEY_NAME, new ArrayOf(INT32), "The in sync replica ids."),
            new Field(ZK_VERSION_KEY_NAME, INT32, "The ZK version."),
            new Field(REPLICAS_KEY_NAME, new ArrayOf(INT32), "The replica ids."),
            new Field(OFFLINE_REPLICAS_KEY_NAME, new ArrayOf(INT32), "The offline replica ids"));

    private static final Schema UPDATE_METADATA_REQUEST_END_POINT_V3 = new Schema(
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(LISTENER_NAME_KEY_NAME, STRING, "The listener name."),
            new Field(SECURITY_PROTOCOL_TYPE_KEY_NAME, INT16, "The security protocol type."));

    private static final Schema UPDATE_METADATA_REQUEST_BROKER_V3 = new Schema(
            new Field(BROKER_ID_KEY_NAME, INT32, "The broker id."),
            new Field(ENDPOINTS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_END_POINT_V3)),
            new Field(RACK_KEY_NAME, NULLABLE_STRING, "The rack"));

    private static final Schema UPDATE_METADATA_REQUEST_V3 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V3)),
            new Field(LIVE_BROKERS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V3)));

    // UPDATE_METADATA_REQUEST_V4 added a per-partition offline_replicas field. This field specifies the list of replicas that are offline.
    private static final Schema UPDATE_METADATA_REQUEST_V4 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(PARTITION_STATES_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V4)),
            new Field(LIVE_BROKERS_KEY_NAME, new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V3)));

    public static Schema[] schemaVersions() {
        return new Schema[] {UPDATE_METADATA_REQUEST_V0, UPDATE_METADATA_REQUEST_V1, UPDATE_METADATA_REQUEST_V2,
            UPDATE_METADATA_REQUEST_V3, UPDATE_METADATA_REQUEST_V4};
    }

    public static class Builder extends AbstractRequest.Builder<UpdateMetadataRequest> {
        private final int controllerId;
        private final int controllerEpoch;
        private final Map<TopicPartition, PartitionState> partitionStates;
        private final Set<Broker> liveBrokers;

        public Builder(short version, int controllerId, int controllerEpoch,
                       Map<TopicPartition, PartitionState> partitionStates, Set<Broker> liveBrokers) {
            super(ApiKeys.UPDATE_METADATA, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.partitionStates = partitionStates;
            this.liveBrokers = liveBrokers;
        }

        @Override
        public UpdateMetadataRequest build(short version) {
            if (version == 0) {
                for (Broker broker : liveBrokers) {
                    if (broker.endPoints.size() != 1 || broker.endPoints.get(0).securityProtocol != SecurityProtocol.PLAINTEXT) {
                        throw new UnsupportedVersionException("UpdateMetadataRequest v0 only handles PLAINTEXT endpoints");
                    }
                }
            }
            return new UpdateMetadataRequest(version, controllerId, controllerEpoch, partitionStates, liveBrokers);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: UpdateMetadataRequest=").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", partitionStates=").append(partitionStates).
                append(", liveBrokers=").append(Utils.join(liveBrokers, ", ")).
                append(")");
            return bld.toString();
        }
    }

    public static final class PartitionState {
        public final BasePartitionState basePartitionState;
        public final List<Integer> offlineReplicas;

        public PartitionState(int controllerEpoch,
                              int leader,
                              int leaderEpoch,
                              List<Integer> isr,
                              int zkVersion,
                              List<Integer> replicas,
                              List<Integer> offlineReplicas) {
            this.basePartitionState = new BasePartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas);
            this.offlineReplicas = offlineReplicas;
        }

        @Override
        public String toString() {
            return "PartitionState(controllerEpoch=" + basePartitionState.controllerEpoch +
                ", leader=" + basePartitionState.leader +
                ", leaderEpoch=" + basePartitionState.leaderEpoch +
                ", isr=" + Arrays.toString(basePartitionState.isr.toArray()) +
                ", zkVersion=" + basePartitionState.zkVersion +
                ", replicas=" + Arrays.toString(basePartitionState.replicas.toArray()) +
                ", offlineReplicas=" + Arrays.toString(offlineReplicas.toArray()) + ")";
        }
    }

    public static final class Broker {
        public final int id;
        public final List<EndPoint> endPoints;
        public final String rack; // introduced in V2

        public Broker(int id, List<EndPoint> endPoints, String rack) {
            this.id = id;
            this.endPoints = endPoints;
            this.rack = rack;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(id=").append(id);
            bld.append(", endPoints=").append(Utils.join(endPoints, ","));
            bld.append(", rack=").append(rack);
            bld.append(")");
            return bld.toString();
        }
    }

    public static final class EndPoint {
        public final String host;
        public final int port;
        public final SecurityProtocol securityProtocol;
        public final ListenerName listenerName; // introduced in V3

        public EndPoint(String host, int port, SecurityProtocol securityProtocol, ListenerName listenerName) {
            this.host = host;
            this.port = port;
            this.securityProtocol = securityProtocol;
            this.listenerName = listenerName;
        }

        @Override
        public String toString() {
            return "(host=" + host + ", port=" + port + ", listenerName=" + listenerName +
                    ", securityProtocol=" + securityProtocol + ")";
        }
    }

    private final int controllerId;
    private final int controllerEpoch;
    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Set<Broker> liveBrokers;

    private UpdateMetadataRequest(short version, int controllerId, int controllerEpoch,
                                  Map<TopicPartition, PartitionState> partitionStates, Set<Broker> liveBrokers) {
        super(version);
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStates = partitionStates;
        this.liveBrokers = liveBrokers;
    }

    public UpdateMetadataRequest(Struct struct, short versionId) {
        super(versionId);
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

            List<Integer> offlineReplicas = new ArrayList<>();
            if (partitionStateData.hasField(OFFLINE_REPLICAS_KEY_NAME)) {
                Object[] offlineReplicasArray = partitionStateData.getArray(OFFLINE_REPLICAS_KEY_NAME);
                for (Object r : offlineReplicasArray)
                    offlineReplicas.add((Integer) r);
            }

            PartitionState partitionState =
                new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas, offlineReplicas);
            partitionStates.put(new TopicPartition(topic, partition), partitionState);
        }

        Set<Broker> liveBrokers = new HashSet<>();

        for (Object brokerDataObj : struct.getArray(LIVE_BROKERS_KEY_NAME)) {
            Struct brokerData = (Struct) brokerDataObj;
            int brokerId = brokerData.getInt(BROKER_ID_KEY_NAME);

            // V0
            if (brokerData.hasField(HOST_KEY_NAME)) {
                String host = brokerData.getString(HOST_KEY_NAME);
                int port = brokerData.getInt(PORT_KEY_NAME);
                List<EndPoint> endPoints = new ArrayList<>(1);
                SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
                endPoints.add(new EndPoint(host, port, securityProtocol, ListenerName.forSecurityProtocol(securityProtocol)));
                liveBrokers.add(new Broker(brokerId, endPoints, null));
            } else { // V1, V2 or V3
                List<EndPoint> endPoints = new ArrayList<>();
                for (Object endPointDataObj : brokerData.getArray(ENDPOINTS_KEY_NAME)) {
                    Struct endPointData = (Struct) endPointDataObj;
                    int port = endPointData.getInt(PORT_KEY_NAME);
                    String host = endPointData.getString(HOST_KEY_NAME);
                    short protocolTypeId = endPointData.getShort(SECURITY_PROTOCOL_TYPE_KEY_NAME);
                    SecurityProtocol securityProtocol = SecurityProtocol.forId(protocolTypeId);
                    String listenerName;
                    if (endPointData.hasField(LISTENER_NAME_KEY_NAME)) // V3
                        listenerName = endPointData.getString(LISTENER_NAME_KEY_NAME);
                    else
                        listenerName = securityProtocol.name;
                    endPoints.add(new EndPoint(host, port, securityProtocol, new ListenerName(listenerName)));
                }
                String rack = null;
                if (brokerData.hasField(RACK_KEY_NAME)) { // V2
                    rack = brokerData.getString(RACK_KEY_NAME);
                }
                liveBrokers.add(new Broker(brokerId, endPoints, rack));
            }
        }
        controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);
        controllerEpoch = struct.getInt(CONTROLLER_EPOCH_KEY_NAME);
        this.partitionStates = partitionStates;
        this.liveBrokers = liveBrokers;
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.UPDATE_METADATA.requestSchema(version));
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
            if (partitionStateData.hasField(OFFLINE_REPLICAS_KEY_NAME))
              partitionStateData.set(OFFLINE_REPLICAS_KEY_NAME, partitionState.offlineReplicas.toArray());
            partitionStatesData.add(partitionStateData);
        }
        struct.set(PARTITION_STATES_KEY_NAME, partitionStatesData.toArray());

        List<Struct> brokersData = new ArrayList<>(liveBrokers.size());
        for (Broker broker : liveBrokers) {
            Struct brokerData = struct.instance(LIVE_BROKERS_KEY_NAME);
            brokerData.set(BROKER_ID_KEY_NAME, broker.id);

            if (version == 0) {
                EndPoint endPoint = broker.endPoints.get(0);
                brokerData.set(HOST_KEY_NAME, endPoint.host);
                brokerData.set(PORT_KEY_NAME, endPoint.port);
            } else {
                List<Struct> endPointsData = new ArrayList<>(broker.endPoints.size());
                for (EndPoint endPoint : broker.endPoints) {
                    Struct endPointData = brokerData.instance(ENDPOINTS_KEY_NAME);
                    endPointData.set(PORT_KEY_NAME, endPoint.port);
                    endPointData.set(HOST_KEY_NAME, endPoint.host);
                    endPointData.set(SECURITY_PROTOCOL_TYPE_KEY_NAME, endPoint.securityProtocol.id);
                    if (version >= 3)
                        endPointData.set(LISTENER_NAME_KEY_NAME, endPoint.listenerName.value());
                    endPointsData.add(endPointData);

                }
                brokerData.set(ENDPOINTS_KEY_NAME, endPointsData.toArray());
                if (version >= 2) {
                    brokerData.set(RACK_KEY_NAME, broker.rack);
                }
            }

            brokersData.add(brokerData);
        }
        struct.set(LIVE_BROKERS_KEY_NAME, brokersData.toArray());

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        if (versionId <= 4)
            return new UpdateMetadataResponse(Errors.forException(e));
        else
            throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ApiKeys.UPDATE_METADATA.latestVersion()));
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

    public Set<Broker> liveBrokers() {
        return liveBrokers;
    }

    public static UpdateMetadataRequest parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataRequest(ApiKeys.UPDATE_METADATA.parseRequest(version, buffer), version);
    }

}
