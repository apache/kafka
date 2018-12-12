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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.CollectionUtils;
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
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class UpdateMetadataRequest extends AbstractControlRequest {
    private static final Field.ComplexArray TOPIC_STATES = new Field.ComplexArray("topic_states", "Topic states");
    private static final Field.ComplexArray PARTITION_STATES = new Field.ComplexArray("partition_states", "Partition states");
    private static final Field.ComplexArray LIVE_BROKERS = new Field.ComplexArray("live_brokers", "Live broekrs");

    // PartitionState fields
    private static final Field.Int32 LEADER = new Field.Int32("leader", "The broker id for the leader.");
    private static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch", "The leader epoch.");
    private static final Field.Array ISR = new Field.Array("isr", INT32, "The in sync replica ids.");
    private static final Field.Int32 ZK_VERSION = new Field.Int32("zk_version", "The ZK version.");
    private static final Field.Array REPLICAS = new Field.Array("replicas", INT32, "The replica ids.");
    private static final Field.Array OFFLINE_REPLICAS = new Field.Array("offline_replicas", INT32, "The offline replica ids");

    // Live brokers fields
    private static final Field.Int32 BROKER_ID = new Field.Int32("id", "The broker id");
    private static final Field.ComplexArray ENDPOINTS = new Field.ComplexArray("end_points", "The endpoints");
    private static final Field.NullableStr RACK = new Field.NullableStr("rack", "The rack");

    // EndPoint fields
    private static final Field.Str HOST = new Field.Str("host", "The hostname of the broker.");
    private static final Field.Int32  PORT = new Field.Int32("port", "The port on which the broker accepts requests.");
    private static final Field.Str LISTENER_NAME = new Field.Str("listener_name", "The listener name.");
    private static final Field.Int16  SECURITY_PROTOCOL_TYPE = new Field.Int16("security_protocol_type", "The security protocol type.");

    private static final Field PARTITION_STATES_V0 = PARTITION_STATES.withFields(
            TOPIC_NAME,
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS);

    // PARTITION_STATES_V4 added a per-partition offline_replicas field. This field specifies
    // the list of replicas that are offline.
    private static final Field PARTITION_STATES_V4 = PARTITION_STATES.withFields(
            TOPIC_NAME,
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS,
            OFFLINE_REPLICAS);

    private static final Field PARTITION_STATES_V5 = PARTITION_STATES.withFields(
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS,
            OFFLINE_REPLICAS);

    // TOPIC_STATES_V5 normalizes TOPIC_STATES_V4 to
    // make it more memory efficient
    private static final Field TOPIC_STATES_V5 = TOPIC_STATES.withFields(
            TOPIC_NAME,
            PARTITION_STATES_V5);

    // for some reason, V1 sends `port` before `host` while V0 sends `host` before `port
    private static final Field ENDPOINTS_V1 = ENDPOINTS.withFields(
            PORT,
            HOST,
            SECURITY_PROTOCOL_TYPE);

    private static final Field ENDPOINTS_V3 = ENDPOINTS.withFields(
            PORT,
            HOST,
            LISTENER_NAME,
            SECURITY_PROTOCOL_TYPE);

    private static final Field LIVE_BROKERS_V0 = LIVE_BROKERS.withFields(
            BROKER_ID,
            HOST,
            PORT);

    private static final Field LIVE_BROKERS_V1 = LIVE_BROKERS.withFields(
            BROKER_ID,
            ENDPOINTS_V1);

    private static final Field LIVE_BROKERS_V2 = LIVE_BROKERS.withFields(
            BROKER_ID,
            ENDPOINTS_V1,
            RACK);

    private static final Field LIVE_BROKERS_V3 = LIVE_BROKERS.withFields(
            BROKER_ID,
            ENDPOINTS_V3,
            RACK);

    private static final Schema UPDATE_METADATA_REQUEST_V0 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V0,
            LIVE_BROKERS_V0);

    private static final Schema UPDATE_METADATA_REQUEST_V1 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V0,
            LIVE_BROKERS_V1);

    private static final Schema UPDATE_METADATA_REQUEST_V2 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V0,
            LIVE_BROKERS_V2);


    private static final Schema UPDATE_METADATA_REQUEST_V3 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V0,
            LIVE_BROKERS_V3);

    // UPDATE_METADATA_REQUEST_V4 added a per-partition offline_replicas field. This field specifies the list of replicas that are offline.
    private static final Schema UPDATE_METADATA_REQUEST_V4 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            PARTITION_STATES_V4,
            LIVE_BROKERS_V3);

    // UPDATE_METADATA_REQUEST_V5 added a broker_epoch Field. This field specifies the generation of the broker across
    // bounces. It also normalizes partitions under each topic.
    private static final Schema UPDATE_METADATA_REQUEST_V5 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            BROKER_EPOCH,
            TOPIC_STATES_V5,
            LIVE_BROKERS_V3);

    public static Schema[] schemaVersions() {
        return new Schema[] {UPDATE_METADATA_REQUEST_V0, UPDATE_METADATA_REQUEST_V1, UPDATE_METADATA_REQUEST_V2,
            UPDATE_METADATA_REQUEST_V3, UPDATE_METADATA_REQUEST_V4, UPDATE_METADATA_REQUEST_V5};
    }

    public static class Builder extends AbstractControlRequest.Builder<UpdateMetadataRequest> {
        private final Map<TopicPartition, PartitionState> partitionStates;
        private final Set<Broker> liveBrokers;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       Map<TopicPartition, PartitionState> partitionStates, Set<Broker> liveBrokers) {
            super(ApiKeys.UPDATE_METADATA, version, controllerId, controllerEpoch, brokerEpoch);
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
            return new UpdateMetadataRequest(version, controllerId, controllerEpoch, brokerEpoch, partitionStates, liveBrokers);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: UpdateMetadataRequest=").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", brokerEpoch=").append(brokerEpoch).
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

            this.offlineReplicas = new ArrayList<>();
            if (struct.hasField(OFFLINE_REPLICAS)) {
                Object[] offlineReplicasArray = struct.get(OFFLINE_REPLICAS);
                for (Object r : offlineReplicasArray)
                    offlineReplicas.add((Integer) r);
            }
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

        private void setStruct(Struct struct) {
            struct.set(CONTROLLER_EPOCH, basePartitionState.controllerEpoch);
            struct.set(LEADER, basePartitionState.leader);
            struct.set(LEADER_EPOCH, basePartitionState.leaderEpoch);
            struct.set(ISR, basePartitionState.isr.toArray());
            struct.set(ZK_VERSION, basePartitionState.zkVersion);
            struct.set(REPLICAS, basePartitionState.replicas.toArray());
            struct.setIfExists(OFFLINE_REPLICAS, offlineReplicas.toArray());
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

    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Set<Broker> liveBrokers;

    private UpdateMetadataRequest(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                                  Map<TopicPartition, PartitionState> partitionStates, Set<Broker> liveBrokers) {
        super(ApiKeys.UPDATE_METADATA, version, controllerId, controllerEpoch, brokerEpoch);
        this.partitionStates = partitionStates;
        this.liveBrokers = liveBrokers;
    }

    public UpdateMetadataRequest(Struct struct, short versionId) {
        super(ApiKeys.UPDATE_METADATA, struct, versionId);
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

        Set<Broker> liveBrokers = new HashSet<>();

        for (Object brokerDataObj : struct.get(LIVE_BROKERS)) {
            Struct brokerData = (Struct) brokerDataObj;
            int brokerId = brokerData.get(BROKER_ID);

            // V0
            if (brokerData.hasField(HOST)) {
                String host = brokerData.get(HOST);
                int port = brokerData.get(PORT);
                List<EndPoint> endPoints = new ArrayList<>(1);
                SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
                endPoints.add(new EndPoint(host, port, securityProtocol, ListenerName.forSecurityProtocol(securityProtocol)));
                liveBrokers.add(new Broker(brokerId, endPoints, null));
            } else { // V1, V2 or V3
                List<EndPoint> endPoints = new ArrayList<>();
                for (Object endPointDataObj : brokerData.get(ENDPOINTS)) {
                    Struct endPointData = (Struct) endPointDataObj;
                    int port = endPointData.get(PORT);
                    String host = endPointData.get(HOST);
                    short protocolTypeId = endPointData.get(SECURITY_PROTOCOL_TYPE);
                    SecurityProtocol securityProtocol = SecurityProtocol.forId(protocolTypeId);
                    String listenerName;
                    if (endPointData.hasField(LISTENER_NAME)) // V3
                        listenerName = endPointData.get(LISTENER_NAME);
                    else
                        listenerName = securityProtocol.name;
                    endPoints.add(new EndPoint(host, port, securityProtocol, new ListenerName(listenerName)));
                }
                String rack = null;
                if (brokerData.hasField(RACK)) { // V2
                    rack = brokerData.get(RACK);
                }
                liveBrokers.add(new Broker(brokerId, endPoints, rack));
            }
        }
        this.partitionStates = partitionStates;
        this.liveBrokers = liveBrokers;
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.UPDATE_METADATA.requestSchema(version));
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

        List<Struct> brokersData = new ArrayList<>(liveBrokers.size());
        for (Broker broker : liveBrokers) {
            Struct brokerData = struct.instance(LIVE_BROKERS);
            brokerData.set(BROKER_ID, broker.id);

            if (version == 0) {
                EndPoint endPoint = broker.endPoints.get(0);
                brokerData.set(HOST, endPoint.host);
                brokerData.set(PORT, endPoint.port);
            } else {
                List<Struct> endPointsData = new ArrayList<>(broker.endPoints.size());
                for (EndPoint endPoint : broker.endPoints) {
                    Struct endPointData = brokerData.instance(ENDPOINTS);
                    endPointData.set(PORT, endPoint.port);
                    endPointData.set(HOST, endPoint.host);
                    endPointData.set(SECURITY_PROTOCOL_TYPE, endPoint.securityProtocol.id);
                    if (version >= 3)
                        endPointData.set(LISTENER_NAME, endPoint.listenerName.value());
                    endPointsData.add(endPointData);

                }
                brokerData.set(ENDPOINTS, endPointsData.toArray());
                if (version >= 2) {
                    brokerData.set(RACK, broker.rack);
                }
            }

            brokersData.add(brokerData);
        }
        struct.set(LIVE_BROKERS, brokersData.toArray());

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        if (versionId <= 5)
            return new UpdateMetadataResponse(Errors.forException(e));
        else
            throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                versionId, this.getClass().getSimpleName(), ApiKeys.UPDATE_METADATA.latestVersion()));
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
