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
package org.apache.kafka.raft.utils;

import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.ReplicaKey;

import java.net.InetSocketAddress;
import java.util.Optional;

public class DynamicReconfigRpc {
    public static AddRaftVoterRequestData addVoterRequest(
        String clusterId,
        int timeoutMs,
        ReplicaKey voter,
        Endpoints listeners
    ) {
        return new AddRaftVoterRequestData()
            .setClusterId(clusterId)
            .setTimeoutMs(timeoutMs)
            .setVoterId(voter.id())
            .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
            .setListeners(listeners.toAddVoterRequest());
    }

    public static AddRaftVoterResponseData addVoterResponse(
        Errors error,
        String errorMessage
    ) {
        errorMessage = errorMessage == null ? error.message() : errorMessage;

        return new AddRaftVoterResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    public static RemoveRaftVoterRequestData removeVoterRequest(
        String clusterId,
        ReplicaKey voter
    ) {
        return new RemoveRaftVoterRequestData()
            .setClusterId(clusterId)
            .setVoterId(voter.id())
            .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
    }

    public static RemoveRaftVoterResponseData removeVoterResponse(
        Errors error,
        String errorMessage
    ) {
        errorMessage = errorMessage == null ? error.message() : errorMessage;

        return new RemoveRaftVoterResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    public static UpdateRaftVoterRequestData updateVoterRequest(
        String clusterId,
        ReplicaKey voter,
        int epoch,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        UpdateRaftVoterRequestData request = new UpdateRaftVoterRequestData()
            .setClusterId(clusterId)
            .setCurrentLeaderEpoch(epoch)
            .setVoterId(voter.id())
            .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
            .setListeners(endpoints.toUpdateVoterRequest());

        request.kRaftVersionFeature()
            .setMinSupportedVersion(supportedVersions.min())
            .setMaxSupportedVersion(supportedVersions.max());

        return request;
    }

    public static UpdateRaftVoterResponseData updateVoterResponse(
        Errors error,
        ListenerName listenerName,
        LeaderAndEpoch leaderAndEpoch,
        Endpoints endpoints
    ) {
        UpdateRaftVoterResponseData response = new UpdateRaftVoterResponseData()
            .setErrorCode(error.code());

        response.currentLeader()
            .setLeaderId(leaderAndEpoch.leaderId().orElse(-1))
            .setLeaderEpoch(leaderAndEpoch.epoch());

        Optional<InetSocketAddress> address = endpoints.address(listenerName);
        if (address.isPresent()) {
            response.currentLeader()
                .setHost(address.get().getHostString())
                .setPort(address.get().getPort());
        }

        return response;
    }

    public static Optional<ReplicaKey> addVoterRequestVoterKey(AddRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    public static Optional<ReplicaKey> removeVoterRequestVoterKey(RemoveRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    public static Optional<ReplicaKey> updateVoterRequestVoterKey(UpdateRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }
}
