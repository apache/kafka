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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataTopicState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class UpdateMetadataRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<UpdateMetadataRequest> {
        private final List<UpdateMetadataPartitionState> partitionStates;
        private final List<UpdateMetadataBroker> liveBrokers;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       List<UpdateMetadataPartitionState> partitionStates, List<UpdateMetadataBroker> liveBrokers) {
            super(ApiKeys.UPDATE_METADATA, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitionStates = partitionStates;
            this.liveBrokers = liveBrokers;
        }

        @Override
        public UpdateMetadataRequest build(short version) {
            if (version == 0) {
                for (UpdateMetadataBroker broker : liveBrokers) {
                    if (broker.endpoints().size() != 1 || broker.endpoints().get(0).securityProtocol() != SecurityProtocol.PLAINTEXT.id) {
                        throw new UnsupportedVersionException("UpdateMetadataRequest v0 only handles PLAINTEXT endpoints");
                    }
                    // We don't null out `endpoints` since it's ignored by the generated code if version >= 1
                    UpdateMetadataEndpoint endpoint = broker.endpoints().get(0);
                    broker.setV0Host(endpoint.host());
                    broker.setV0Port(endpoint.port());
                }
            }

            UpdateMetadataRequestData data = new UpdateMetadataRequestData()
                    .setControllerId(controllerId)
                    .setControllerEpoch(controllerEpoch)
                    .setBrokerEpoch(brokerEpoch)
                    .setLiveBrokers(liveBrokers);

            if (version >= 5) {
                Map<String, UpdateMetadataTopicState> topicStatesMap = groupByTopic(partitionStates);
                data.setTopicStates(new ArrayList<>(topicStatesMap.values()));
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }

            return new UpdateMetadataRequest(data, version);
        }

        private static Map<String, UpdateMetadataTopicState> groupByTopic(List<UpdateMetadataPartitionState> partitionStates) {
            Map<String, UpdateMetadataTopicState> topicStates = new HashMap<>();
            for (UpdateMetadataPartitionState partition : partitionStates) {
                // We don't null out the topic name in UpdateMetadataTopicState since it's ignored by the generated
                // code if version >= 5
                UpdateMetadataTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new UpdateMetadataTopicState().setTopicName(partition.topicName()));
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
                append(", brokerEpoch=").append(brokerEpoch).
                append(", partitionStates=").append(partitionStates).
                append(", liveBrokers=").append(Utils.join(liveBrokers, ", ")).
                append(")");
            return bld.toString();
        }
    }

    private final UpdateMetadataRequestData data;

    private UpdateMetadataRequest(UpdateMetadataRequestData data, short version) {
        super(ApiKeys.UPDATE_METADATA, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalize();
    }

    private void normalize() {
        if (version() == 0) {
            for (UpdateMetadataBroker liveBroker : data.liveBrokers()) {
                // Set endpoints so that callers can rely on it always being present
                if (liveBroker.endpoints().isEmpty()) {
                    SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
                    liveBroker.setEndpoints(singletonList(new UpdateMetadataEndpoint()
                            .setHost(liveBroker.v0Host())
                            .setPort(liveBroker.v0Port())
                            .setSecurityProtocol(securityProtocol.id)
                            .setListener(ListenerName.forSecurityProtocol(securityProtocol).value())));
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

    public UpdateMetadataRequest(Struct struct, short version) {
        this(new UpdateMetadataRequestData(struct, version), version);
    }

    @Override
    public int controllerId() {
        return data.controllerId();
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
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        if (versionId <= 5)
            return new UpdateMetadataResponse(Errors.forException(e));
        else
            throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                versionId, this.getClass().getSimpleName(), ApiKeys.UPDATE_METADATA.latestVersion()));
    }

    public Iterable<UpdateMetadataPartitionState> partitionStates() {
        if (version() >= 5)
            return () -> new PartitionStateIterator(data.topicStates());
        return data.ungroupedPartitionStates();
    }

    public List<UpdateMetadataBroker> liveBrokers() {
        return data.liveBrokers();
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    protected int size() {
        return data.size(version());
    }

    public static UpdateMetadataRequest parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataRequest(ApiKeys.UPDATE_METADATA.parseRequest(version, buffer), version);
    }

    private static class PartitionStateIterator extends NestedIterator<UpdateMetadataTopicState, UpdateMetadataPartitionState> {

        PartitionStateIterator(List<UpdateMetadataTopicState> topicStates) {
            super(topicStates);
        }

        @Override
        public Iterable<UpdateMetadataPartitionState> innerIterable(UpdateMetadataTopicState outer) {
            return outer.partitionStates();
        }
    }

}
