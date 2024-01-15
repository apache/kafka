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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataTopicState;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class UpdateMetadataRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<UpdateMetadataRequest> {
        private final List<UpdateMetadataPartitionState> partitionStates;
        private final List<UpdateMetadataBroker> liveBrokers;
        private final Map<String, Uuid> topicIds;
        private final Type updateType;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       List<UpdateMetadataPartitionState> partitionStates, List<UpdateMetadataBroker> liveBrokers,
                       Map<String, Uuid> topicIds) {
            this(version, controllerId, controllerEpoch, brokerEpoch, partitionStates,
                liveBrokers, topicIds, false, Type.UNKNOWN);
        }

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       List<UpdateMetadataPartitionState> partitionStates, List<UpdateMetadataBroker> liveBrokers,
                       Map<String, Uuid> topicIds, boolean kraftController, Type updateType) {
            super(ApiKeys.UPDATE_METADATA, version, controllerId, controllerEpoch, brokerEpoch, kraftController);
            this.partitionStates = partitionStates;
            this.liveBrokers = liveBrokers;
            this.topicIds = topicIds;

            if (version >= 8) {
                this.updateType = updateType;
            } else {
                this.updateType = Type.UNKNOWN;
            }
        }

        @Override
        public UpdateMetadataRequest build(short version) {
            if (version < 3) {
                for (UpdateMetadataBroker broker : liveBrokers) {
                    if (version == 0) {
                        if (broker.endpoints().size() != 1)
                            throw new UnsupportedVersionException("UpdateMetadataRequest v0 requires a single endpoint");
                        if (broker.endpoints().get(0).securityProtocol() != SecurityProtocol.PLAINTEXT.id)
                            throw new UnsupportedVersionException("UpdateMetadataRequest v0 only handles PLAINTEXT endpoints");
                        // Don't null out `endpoints` since it's ignored by the generated code if version >= 1
                        UpdateMetadataEndpoint endpoint = broker.endpoints().get(0);
                        broker.setV0Host(endpoint.host());
                        broker.setV0Port(endpoint.port());
                    } else {
                        if (broker.endpoints().stream().anyMatch(endpoint -> !endpoint.listener().isEmpty() &&
                                !endpoint.listener().equals(listenerNameFromSecurityProtocol(endpoint)))) {
                            throw new UnsupportedVersionException("UpdateMetadataRequest v0-v3 does not support custom " +
                                "listeners, request version: " + version + ", endpoints: " + broker.endpoints());
                        }
                    }
                }
            }

            UpdateMetadataRequestData data = new UpdateMetadataRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveBrokers(liveBrokers);

            if (version >= 8) {
                data.setIsKRaftController(kraftController);
                data.setType(updateType.toByte());
            }

            if (version >= 5) {
                Map<String, UpdateMetadataTopicState> topicStatesMap = groupByTopic(topicIds, partitionStates);
                data.setTopicStates(new ArrayList<>(topicStatesMap.values()));
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }

            return new UpdateMetadataRequest(data, version);
        }

        private static Map<String, UpdateMetadataTopicState> groupByTopic(Map<String, Uuid> topicIds, List<UpdateMetadataPartitionState> partitionStates) {
            Map<String, UpdateMetadataTopicState> topicStates = new HashMap<>();
            for (UpdateMetadataPartitionState partition : partitionStates) {
                // We don't null out the topic name in UpdateMetadataPartitionState since it's ignored by the generated
                // code if version >= 5
                UpdateMetadataTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new UpdateMetadataTopicState()
                            .setTopicName(partition.topicName())
                            .setTopicId(topicIds.getOrDefault(partition.topicName(), Uuid.ZERO_UUID))

                );
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: UpdateMetadataRequest=").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", kraftController=").append(kraftController).
                append(", type=").append(updateType).
                append(", brokerEpoch=").append(brokerEpoch).
                append(", partitionStates=").append(partitionStates).
                append(", liveBrokers=").append(Utils.join(liveBrokers, ", ")).
                append(")");
            return bld.toString();
        }
    }

    private final UpdateMetadataRequestData data;

    UpdateMetadataRequest(UpdateMetadataRequestData data, short version) {
        super(ApiKeys.UPDATE_METADATA, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalize();
    }

    private void normalize() {
        // Version 0 only supported a single host and port and the protocol was always plaintext
        // Version 1 added support for multiple endpoints, each with its own security protocol
        // Version 2 added support for rack
        // Version 3 added support for listener name, which we can infer from the security protocol for older versions
        if (version() < 3) {
            for (UpdateMetadataBroker liveBroker : data.liveBrokers()) {
                // Set endpoints so that callers can rely on it always being present
                if (version() == 0 && liveBroker.endpoints().isEmpty()) {
                    SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
                    liveBroker.setEndpoints(singletonList(new UpdateMetadataEndpoint()
                        .setHost(liveBroker.v0Host())
                        .setPort(liveBroker.v0Port())
                        .setSecurityProtocol(securityProtocol.id)
                        .setListener(ListenerName.forSecurityProtocol(securityProtocol).value())));
                } else {
                    for (UpdateMetadataEndpoint endpoint : liveBroker.endpoints()) {
                        // Set listener so that callers can rely on it always being present
                        if (endpoint.listener().isEmpty())
                            endpoint.setListener(listenerNameFromSecurityProtocol(endpoint));
                    }
                }
            }
        }

        if (version() >= 5) {
            for (UpdateMetadataTopicState topicState : data.topicStates()) {
                for (UpdateMetadataPartitionState partitionState : topicState.partitionStates()) {
                    // Set the topic name so that we can always present the ungrouped view to callers
                    partitionState.setTopicName(topicState.topicName());
                }
            }
        }
    }

    private static String listenerNameFromSecurityProtocol(UpdateMetadataEndpoint endpoint) {
        SecurityProtocol securityProtocol = SecurityProtocol.forId(endpoint.securityProtocol());
        return ListenerName.forSecurityProtocol(securityProtocol).value();
    }

    @Override
    public int controllerId() {
        return data.controllerId();
    }

    @Override
    public boolean isKRaftController() {
        return data.isKRaftController();
    }

    public Type updateType() {
        return Type.fromByte(data.type());
    }

    @Override
    public int controllerEpoch() {
        return data.controllerEpoch();
    }

    @Override
    public long brokerEpoch() {
        return data.brokerEpoch();
    }

    @Override
    public UpdateMetadataResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        UpdateMetadataResponseData data = new UpdateMetadataResponseData()
                .setErrorCode(Errors.forException(e).code());
        return new UpdateMetadataResponse(data);
    }

    public Iterable<UpdateMetadataPartitionState> partitionStates() {
        if (version() >= 5) {
            return () -> new FlattenedIterator<>(data.topicStates().iterator(),
                topicState -> topicState.partitionStates().iterator());
        }
        return data.ungroupedPartitionStates();
    }

    public List<UpdateMetadataTopicState> topicStates() {
        if (version() >= 5) {
            return data.topicStates();
        }
        return Collections.emptyList();
    }

    public List<UpdateMetadataBroker> liveBrokers() {
        return data.liveBrokers();
    }

    @Override
    public UpdateMetadataRequestData data() {
        return data;
    }

    public static UpdateMetadataRequest parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataRequest(new UpdateMetadataRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
