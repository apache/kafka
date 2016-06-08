/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateMetadataRequest extends AbstractRequest {

    public static final class PartitionState {
        public final int controllerEpoch;
        public final int leader;
        public final int leaderEpoch;
        public final List<Integer> isr;
        public final int zkVersion;
        public final Set<Integer> replicas;

        public PartitionState(int controllerEpoch, int leader, int leaderEpoch, List<Integer> isr, int zkVersion, Set<Integer> replicas) {
            this.controllerEpoch = controllerEpoch;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.isr = isr;
            this.zkVersion = zkVersion;
            this.replicas = replicas;
        }
    }

    public static final class Broker {
        public final int id;
        public final Map<SecurityProtocol, EndPoint> endPoints;
        public final String rack;

        public Broker(int id, Map<SecurityProtocol, EndPoint> endPoints, String rack) {
            this.id = id;
            this.endPoints = endPoints;
            this.rack = rack;
        }

        @Deprecated
        public Broker(int id, Map<SecurityProtocol, EndPoint> endPoints) {
            this(id, endPoints, null);
        }
    }

    public static final class EndPoint {
        public final String host;
        public final int port;

        public EndPoint(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.UPDATE_METADATA_KEY.id);

    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    private static final String PARTITION_STATES_KEY_NAME = "partition_states";
    private static final String LIVE_BROKERS_KEY_NAME = "live_brokers";

    // PartitionState key names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String LEADER_KEY_NAME = "leader";
    private static final String LEADER_EPOCH_KEY_NAME = "leader_epoch";
    private static final String ISR_KEY_NAME = "isr";
    private static final String ZK_VERSION_KEY_NAME = "zk_version";
    private static final String REPLICAS_KEY_NAME = "replicas";

    // Broker key names
    private static final String BROKER_ID_KEY_NAME = "id";
    private static final String ENDPOINTS_KEY_NAME = "end_points";
    private static final String RACK_KEY_NAME = "rack";

    // EndPoint key names
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";
    private static final String SECURITY_PROTOCOL_TYPE_KEY_NAME = "security_protocol_type";

    private final int controllerId;
    private final int controllerEpoch;
    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Set<Broker> liveBrokers;

    /**
     * Constructor for version 0.
     */
    @Deprecated
    public UpdateMetadataRequest(int controllerId, int controllerEpoch, Set<Node> liveBrokers,
                                 Map<TopicPartition, PartitionState> partitionStates) {
        this(0, controllerId, controllerEpoch, partitionStates,
             brokerEndPointsToBrokers(liveBrokers));
    }

    private static Set<Broker> brokerEndPointsToBrokers(Set<Node> brokerEndPoints) {
        Set<Broker> brokers = new HashSet<>(brokerEndPoints.size());
        for (Node brokerEndPoint : brokerEndPoints) {
            Map<SecurityProtocol, EndPoint> endPoints = Collections.singletonMap(SecurityProtocol.PLAINTEXT,
                    new EndPoint(brokerEndPoint.host(), brokerEndPoint.port()));
            brokers.add(new Broker(brokerEndPoint.id(), endPoints, null));
        }
        return brokers;
    }

    /**
     * Constructor for version 2.
     */
    public UpdateMetadataRequest(int controllerId, int controllerEpoch, Map<TopicPartition,
            PartitionState> partitionStates, Set<Broker> liveBrokers) {
        this(2, controllerId, controllerEpoch, partitionStates, liveBrokers);
    }

    public UpdateMetadataRequest(int version, int controllerId, int controllerEpoch, Map<TopicPartition,
            PartitionState> partitionStates, Set<Broker> liveBrokers) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.UPDATE_METADATA_KEY.id, version)));
        struct.set(CONTROLLER_ID_KEY_NAME, controllerId);
        struct.set(CONTROLLER_EPOCH_KEY_NAME, controllerEpoch);

        List<Struct> partitionStatesData = new ArrayList<>(partitionStates.size());
        for (Map.Entry<TopicPartition, PartitionState> entry : partitionStates.entrySet()) {
            Struct partitionStateData = struct.instance(PARTITION_STATES_KEY_NAME);
            TopicPartition topicPartition = entry.getKey();
            partitionStateData.set(TOPIC_KEY_NAME, topicPartition.topic());
            partitionStateData.set(PARTITION_KEY_NAME, topicPartition.partition());
            PartitionState partitionState = entry.getValue();
            partitionStateData.set(CONTROLLER_EPOCH_KEY_NAME, partitionState.controllerEpoch);
            partitionStateData.set(LEADER_KEY_NAME, partitionState.leader);
            partitionStateData.set(LEADER_EPOCH_KEY_NAME, partitionState.leaderEpoch);
            partitionStateData.set(ISR_KEY_NAME, partitionState.isr.toArray());
            partitionStateData.set(ZK_VERSION_KEY_NAME, partitionState.zkVersion);
            partitionStateData.set(REPLICAS_KEY_NAME, partitionState.replicas.toArray());
            partitionStatesData.add(partitionStateData);
        }
        struct.set(PARTITION_STATES_KEY_NAME, partitionStatesData.toArray());

        List<Struct> brokersData = new ArrayList<>(liveBrokers.size());
        for (Broker broker : liveBrokers) {
            Struct brokerData = struct.instance(LIVE_BROKERS_KEY_NAME);
            brokerData.set(BROKER_ID_KEY_NAME, broker.id);

            if (version == 0) {
                EndPoint endPoint = broker.endPoints.get(SecurityProtocol.PLAINTEXT);
                brokerData.set(HOST_KEY_NAME, endPoint.host);
                brokerData.set(PORT_KEY_NAME, endPoint.port);
            } else {
                List<Struct> endPointsData = new ArrayList<>(broker.endPoints.size());
                for (Map.Entry<SecurityProtocol, EndPoint> entry : broker.endPoints.entrySet()) {
                    Struct endPointData = brokerData.instance(ENDPOINTS_KEY_NAME);
                    endPointData.set(PORT_KEY_NAME, entry.getValue().port);
                    endPointData.set(HOST_KEY_NAME, entry.getValue().host);
                    endPointData.set(SECURITY_PROTOCOL_TYPE_KEY_NAME, entry.getKey().id);
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

        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStates = partitionStates;
        this.liveBrokers = liveBrokers;
    }

    public UpdateMetadataRequest(Struct struct) {
        super(struct);

        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        for (Object partitionStateDataObj : struct.getArray(PARTITION_STATES_KEY_NAME)) {
            Struct partitionStateData = (Struct) partitionStateDataObj;
            String topic = partitionStateData.getString(TOPIC_KEY_NAME);
            int partition = partitionStateData.getInt(PARTITION_KEY_NAME);
            int controllerEpoch = partitionStateData.getInt(CONTROLLER_EPOCH_KEY_NAME);
            int leader = partitionStateData.getInt(LEADER_KEY_NAME);
            int leaderEpoch = partitionStateData.getInt(LEADER_EPOCH_KEY_NAME);

            Object[] isrArray = partitionStateData.getArray(ISR_KEY_NAME);
            List<Integer> isr = new ArrayList<>(isrArray.length);
            for (Object r : isrArray)
                isr.add((Integer) r);

            int zkVersion = partitionStateData.getInt(ZK_VERSION_KEY_NAME);

            Object[] replicasArray = partitionStateData.getArray(REPLICAS_KEY_NAME);
            Set<Integer> replicas = new HashSet<>(replicasArray.length);
            for (Object r : replicasArray)
                replicas.add((Integer) r);

            PartitionState partitionState = new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas);
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
                Map<SecurityProtocol, EndPoint> endPoints = new HashMap<>(1);
                endPoints.put(SecurityProtocol.PLAINTEXT, new EndPoint(host, port));
                liveBrokers.add(new Broker(brokerId, endPoints, null));
            } else { // V1 or V2
                Map<SecurityProtocol, EndPoint> endPoints = new HashMap<>();
                for (Object endPointDataObj : brokerData.getArray(ENDPOINTS_KEY_NAME)) {
                    Struct endPointData = (Struct) endPointDataObj;
                    int port = endPointData.getInt(PORT_KEY_NAME);
                    String host = endPointData.getString(HOST_KEY_NAME);
                    short protocolTypeId = endPointData.getShort(SECURITY_PROTOCOL_TYPE_KEY_NAME);
                    endPoints.put(SecurityProtocol.forId(protocolTypeId), new EndPoint(host, port));
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
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        if (versionId <= 2)
            return new UpdateMetadataResponse(Errors.forException(e).code());
        else
            throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.UPDATE_METADATA_KEY.id)));
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

    public static UpdateMetadataRequest parse(ByteBuffer buffer, int versionId) {
        return new UpdateMetadataRequest(ProtoUtils.parseRequest(ApiKeys.UPDATE_METADATA_KEY.id, versionId, buffer));
    }

    public static UpdateMetadataRequest parse(ByteBuffer buffer) {
        return new UpdateMetadataRequest(CURRENT_SCHEMA.read(buffer));
    }
}
