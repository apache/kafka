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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupTopologyDescriptionUpdateRequest;
import org.apache.kafka.common.requests.StreamsGroupTopologyDescriptionUpdateResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamsGroupTopologyDescriptionRequestManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final UUID PROCESS_ID = UUID.randomUUID();
    private static final long CURRENT_TIME_MS = 1000L;

    private static final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription TOPOLOGY_DESCRIPTION =
        new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
            .setSubtopologies(Collections.emptyList())
            .setGlobalStores(Collections.emptyList());

    @Mock
    private CoordinatorRequestManager coordinatorRequestManager;

    @Mock
    private StreamsMembershipManager membershipManager;

    private StreamsRebalanceData streamsRebalanceData;

    private StreamsGroupTopologyDescriptionRequestManager manager;

    private final Node coordinatorNode = new Node(1, "localhost", 9092);
    private final Time time = new MockTime(0, CURRENT_TIME_MS, 0);
    private static final String MEMBER_ID = "test-member-id";

    @BeforeEach
    void setUp() {
        streamsRebalanceData = new StreamsRebalanceData(
            PROCESS_ID,
            Optional.empty(),
            Optional.empty(),
            Map.of(),
            Map.of(),
            Optional.of(TOPOLOGY_DESCRIPTION)
        );
        org.mockito.Mockito.lenient().when(membershipManager.memberId()).thenReturn(MEMBER_ID);
        manager = new StreamsGroupTopologyDescriptionRequestManager(
            new LogContext("test"),
            time,
            coordinatorRequestManager,
            membershipManager,
            streamsRebalanceData,
            GROUP_ID
        );
    }

    @Test
    void testNoRequestWhenNoCoordinator() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.PollResult result = manager.poll(CURRENT_TIME_MS);

        assertTrue(result.unsentRequests.isEmpty());
    }

    @Test
    void testNoRequestWhenTopologyDescriptionNotRequired() {
        // topologyDescriptionRequired defaults to false; no other stubs needed because we bail
        // before consulting the coordinator.
        final NetworkClientDelegate.PollResult result = manager.poll(CURRENT_TIME_MS);

        assertTrue(result.unsentRequests.isEmpty());
    }

    @Test
    void testNoRequestWhenTopologyDescriptionAbsent() {
        final StreamsRebalanceData dataWithoutTopology = new StreamsRebalanceData(
            PROCESS_ID,
            Optional.empty(),
            Optional.empty(),
            Map.of(),
            Map.of(),
            Optional.empty()
        );
        final StreamsGroupTopologyDescriptionRequestManager managerWithoutTopology =
            new StreamsGroupTopologyDescriptionRequestManager(
                new LogContext("test"),
                time,
                coordinatorRequestManager,
                membershipManager,
                dataWithoutTopology,
                GROUP_ID
            );
        dataWithoutTopology.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.PollResult result = managerWithoutTopology.poll(CURRENT_TIME_MS);

        assertTrue(result.unsentRequests.isEmpty());
    }

    @Test
    void testRequestSentWhenFlagSetAndCoordinatorPresent() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.PollResult result = manager.poll(CURRENT_TIME_MS);

        assertEquals(1, result.unsentRequests.size());
        final NetworkClientDelegate.UnsentRequest request = result.unsentRequests.get(0);
        assertEquals(Optional.of(coordinatorNode), request.node());
        final StreamsGroupTopologyDescriptionUpdateRequest built =
            (StreamsGroupTopologyDescriptionUpdateRequest) request.requestBuilder().build();
        assertEquals(GROUP_ID, built.data().groupId());
    }

    @Test
    void testNoSecondRequestWhileInFlight() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        // First poll sends request
        final NetworkClientDelegate.PollResult firstResult = manager.poll(CURRENT_TIME_MS);
        assertEquals(1, firstResult.unsentRequests.size());

        // Second poll must not send while first is in-flight
        final NetworkClientDelegate.PollResult secondResult = manager.poll(CURRENT_TIME_MS);
        assertTrue(secondResult.unsentRequests.isEmpty());
        assertTrue(streamsRebalanceData.topologyDescriptionRequired());
    }

    @Test
    void testSuccessResponse_clearsFlag() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.UnsentRequest request = manager.poll(CURRENT_TIME_MS).unsentRequests.get(0);
        request.handler().onComplete(buildResponse(Errors.NONE, null));

        assertFalse(streamsRebalanceData.topologyDescriptionRequired());

        // After success the manager should be able to send again if flag is re-raised
        streamsRebalanceData.setTopologyDescriptionRequired(true);
        final NetworkClientDelegate.PollResult retryResult = manager.poll(CURRENT_TIME_MS);
        assertEquals(1, retryResult.unsentRequests.size());
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {
        "STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED",
        "UNKNOWN_MEMBER_ID",
        "INVALID_GROUP_ID"
    })
    void testNonRetryableErrors_clearFlag(final Errors error) {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.UnsentRequest request = manager.poll(CURRENT_TIME_MS).unsentRequests.get(0);
        request.handler().onComplete(buildResponse(error, "error message"));

        assertFalse(streamsRebalanceData.topologyDescriptionRequired());
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NOT_COORDINATOR", "COORDINATOR_NOT_AVAILABLE"})
    void testCoordinatorErrors_markCoordinatorUnknown_keepFlag(final Errors error) {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.UnsentRequest request = manager.poll(CURRENT_TIME_MS).unsentRequests.get(0);
        request.handler().onComplete(buildResponse(error, "coordinator gone"));

        // Flag stays set so the request will be retried once coordinator is rediscovered
        assertTrue(streamsRebalanceData.topologyDescriptionRequired());
        verify(coordinatorRequestManager).markCoordinatorUnknown("coordinator gone", CURRENT_TIME_MS);
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"COORDINATOR_LOAD_IN_PROGRESS"})
    void testTransientErrors_keepFlag_allowRetry(final Errors error) {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.UnsentRequest request = manager.poll(CURRENT_TIME_MS).unsentRequests.get(0);
        request.handler().onComplete(buildResponse(error, "transient"));

        // Flag stays set and no longer in-flight — next poll may retry
        assertTrue(streamsRebalanceData.topologyDescriptionRequired());

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        final NetworkClientDelegate.PollResult retryResult = manager.poll(CURRENT_TIME_MS);
        assertEquals(1, retryResult.unsentRequests.size());
    }

    @Test
    void testNetworkException_keepsFlag_allowRetry() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.PollResult result = manager.poll(CURRENT_TIME_MS);
        assertEquals(1, result.unsentRequests.size());
        final NetworkClientDelegate.UnsentRequest request = result.unsentRequests.get(0);

        // Simulate a disconnection (network exception path)
        request.handler().onComplete(buildDisconnectedResponse());

        assertTrue(streamsRebalanceData.topologyDescriptionRequired());

        // Should be able to retry
        final NetworkClientDelegate.PollResult retryResult = manager.poll(CURRENT_TIME_MS);
        assertEquals(1, retryResult.unsentRequests.size());
    }

    @Test
    void testNoRequestUntilMemberIdAssigned() {
        // Override the default stub: simulate the gap between consumer startup and the first
        // heartbeat returning a memberId.
        org.mockito.Mockito.reset(membershipManager);
        when(membershipManager.memberId()).thenReturn("");
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.PollResult result = manager.poll(CURRENT_TIME_MS);

        assertTrue(result.unsentRequests.isEmpty(),
            "Push should not be sent until the membership manager has assigned a memberId");
    }

    @Test
    void testRequestCarriesMemberId() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        streamsRebalanceData.setTopologyDescriptionRequired(true);

        final NetworkClientDelegate.PollResult result = manager.poll(CURRENT_TIME_MS);
        assertEquals(1, result.unsentRequests.size());
        final StreamsGroupTopologyDescriptionUpdateRequest.Builder builder =
            (StreamsGroupTopologyDescriptionUpdateRequest.Builder) result.unsentRequests.get(0).requestBuilder();
        assertEquals(MEMBER_ID, builder.build().data().memberId());
    }

    private ClientResponse buildResponse(final Errors error, final String errorMessage) {
        final StreamsGroupTopologyDescriptionUpdateResponseData responseData =
            new StreamsGroupTopologyDescriptionUpdateResponseData()
                .setErrorCode(error.code());
        if (errorMessage != null) {
            responseData.setErrorMessage(errorMessage);
        }
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE, (short) 0, "", 1),
            null,
            "-1",
            CURRENT_TIME_MS,
            CURRENT_TIME_MS,
            false,
            null,
            null,
            new StreamsGroupTopologyDescriptionUpdateResponse(responseData)
        );
    }

    private ClientResponse buildDisconnectedResponse() {
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE, (short) 0, "", 1),
            null,
            "-1",
            CURRENT_TIME_MS,
            CURRENT_TIME_MS,
            true,
            null,
            null,
            null
        );
    }
}
