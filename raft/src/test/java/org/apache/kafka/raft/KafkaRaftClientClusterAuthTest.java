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

package org.apache.kafka.raft;

import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;

import static org.apache.kafka.raft.KafkaRaftClientTest.randomReplicaId;
import static org.apache.kafka.raft.RaftClientTestContext.Builder.DEFAULT_ELECTION_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaRaftClientClusterAuthTest {
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testClusterAuthorizationFailedInFetch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .withKip853Rpc(withKip853Rpc)
                .withElectedLeader(epoch, otherNodeId)
                .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();

        RaftRequest.Outbound request = context.assertSentFetchRequest(epoch, 0, 0, context.client.highWatermark());
        FetchResponseData response = new FetchResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        context.deliverResponse(
                request.correlationId(),
                request.destination(),
                response
        );
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testClusterAuthorizationFailedInBeginQuorumEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
                .withUnknownLeader(epoch - 1)
                .withKip853Rpc(withKip853Rpc)
                .build();

        context.time.sleep(context.electionTimeoutMs());
        context.expectAndGrantPreVotes(epoch - 1);
        context.expectAndGrantVotes(epoch);

        context.pollUntilRequest();
        List<RaftRequest.Outbound> requests = context.collectBeginEpochRequests(epoch);
        assertEquals(1, requests.size());
        RaftRequest.Outbound request = requests.get(0);
        assertEquals(otherNodeId, request.destination().id());
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(request.correlationId(), request.destination(), response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testClusterAuthorizationFailedInVote(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .withUnknownLeader(epoch - 1)
                .withKip853Rpc(withKip853Rpc)
                .build();

        // Become a candidate
        context.unattachedToCandidate();
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch, localId);

        RaftRequest.Outbound request = context.assertSentVoteRequest(epoch, 0, 0L, 1);
        VoteResponseData response = new VoteResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(request.correlationId(), request.destination(), response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testClusterAuthorizationFailedInEndQuorumEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .withUnknownLeader(1)
                .withKip853Rpc(withKip853Rpc)
                .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        context.client.shutdown(5000);
        context.pollUntilRequest();

        RaftRequest.Outbound request = context.assertSentEndQuorumEpochRequest(epoch, otherNodeId);
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(request.correlationId(), request.destination(), response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }
}
