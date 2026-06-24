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
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamsGroupTopologyDescriptionRequestManagerTest {
    private static final String GROUP_ID = "group-id";
    private static final String MEMBER_ID = "member-id";
    private static final long RETRY_BACKOFF_MS = 100;
    private static final long RETRY_BACKOFF_MAX_MS = 1000;

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final Node coordinatorNode = new Node(1, "host", 9092);

    private CoordinatorRequestManager coordinatorRequestManager;
    private StreamsMembershipManager membershipManager;
    private StreamsRebalanceData streamsRebalanceData;
    private StreamsGroupTopologyDescriptionRequestManager manager;

    @BeforeEach
    public void setUp() {
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        membershipManager = mock(StreamsMembershipManager.class);
        when(membershipManager.groupId()).thenReturn(GROUP_ID);
        when(membershipManager.memberId()).thenReturn(MEMBER_ID);
        streamsRebalanceData = new StreamsRebalanceData(
            UUID.randomUUID(), Optional.empty(), Optional.empty(), Map.of(), Map.of(), Map::of, Map::of
        );
        manager = new StreamsGroupTopologyDescriptionRequestManager(
            logContext, time, RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS,
            membershipManager, streamsRebalanceData, coordinatorRequestManager
        );
    }

    /**
     * Test the situation when the coordinator has not yet been discovered: no request should be sent
     * and the client should silently wait for the coordinator to become known.
     */
    @Test
    public void testCoordinatorUnknown() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());

        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    /**
     * Test the situation when the wire-format topology description has not yet been populated:
     * no request should be sent.
     */
    @Test
    public void testWireTopologyDescriptionNull() {
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    /**
     * Test the situation when memberId is null because the member has not yet joined the group:
     * no request should be sent.
     */
    @Test
    public void testMemberIdNull() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        when(membershipManager.memberId()).thenReturn(null);

        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    /**
     * Test the situation when the topologyPushRequired flag is not set: maximumTimeToWait should
     * return Long.MAX_VALUE and no request should be sent even if every other precondition is satisfied.
     */
    @Test
    public void testTopologyDescriptionWhenPushFlagNotSet() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        assertEquals(Long.MAX_VALUE, manager.maximumTimeToWait(time.milliseconds()));
        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    /**
     * Test the situation when all preconditions are satisfied: maximumTimeToWait should return 0
     * to wake the poll loop immediately, and the subsequent poll should build a request with the
     * correct groupId, memberId, and target coordinator node.
     */
    @Test
    public void testSendRequestWhenAllPreconditionsMet() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        assertEquals(0L, manager.maximumTimeToWait(time.milliseconds()));

        final NetworkClientDelegate.PollResult result = manager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        final NetworkClientDelegate.UnsentRequest unsent = result.unsentRequests.get(0);
        assertEquals(Optional.of(coordinatorNode), unsent.node());
        final StreamsGroupTopologyDescriptionUpdateRequest request =
            (StreamsGroupTopologyDescriptionUpdateRequest) unsent.requestBuilder().build();
        assertEquals(GROUP_ID, request.data().groupId());
        assertEquals(MEMBER_ID, request.data().memberId());
    }

    /**
     * Test the situation when a push request is already in flight: subsequent polls should not
     * enqueue duplicate requests.
     */
    @Test
    public void testNoSecondRequestWhileInflight() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        manager.poll(time.milliseconds());
        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    /**
     * Test the situation when the broker responds with no error: the topologyPushRequired flag
     * should be cleared so the manager stops pushing.
     */
    @Test
    public void testFlagClearedOnSuccess() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        final NetworkClientDelegate.UnsentRequest unsent =
            manager.poll(time.milliseconds()).unsentRequests.get(0);
        unsent.handler().onComplete(buildResponse(Errors.NONE, 0));

        assertFalse(streamsRebalanceData.topologyPushRequired());
    }

    /**
     * Test the situation when the broker responds with UNKNOWN_MEMBER_ID: the flag should be
     * cleared so the heartbeat path can drive a clean rejoin.
     */
    @Test
    public void testFlagClearedOnUnknownMemberId() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        final NetworkClientDelegate.UnsentRequest unsent =
            manager.poll(time.milliseconds()).unsentRequests.get(0);
        unsent.handler().onComplete(buildResponse(Errors.UNKNOWN_MEMBER_ID, 0));

        assertFalse(streamsRebalanceData.topologyPushRequired());
    }

    /**
     * Test the situation when the broker responds with NOT_COORDINATOR or COORDINATOR_NOT_AVAILABLE:
     * coordinator rediscovery should be triggered and the flag should remain set for retry.
     */
    @Test
    public void testCoordinatorErrorTriggersRediscovery() {
        for (final Errors error : new Errors[]{Errors.NOT_COORDINATOR, Errors.COORDINATOR_NOT_AVAILABLE}) {
            streamsRebalanceData.setWireTopologyDescription(
                new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
            streamsRebalanceData.setTopologyPushRequired(true);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            time.sleep(RETRY_BACKOFF_MAX_MS);

            final NetworkClientDelegate.UnsentRequest unsent =
                manager.poll(time.milliseconds()).unsentRequests.get(0);
            unsent.handler().onComplete(buildResponse(error, 0));

            assertTrue(streamsRebalanceData.topologyPushRequired(),
                "Flag should remain set after " + error);
        }
        verify(coordinatorRequestManager, times(2)).markCoordinatorUnknown(any(), anyLong());
    }

    /**
     * Test the situation when the broker responds with COORDINATOR_LOAD_IN_PROGRESS: the flag
     * should remain set for retry but coordinator rediscovery should not be triggered, since the
     * coordinator is correct but still loading.
     */
    @Test
    public void testCoordinatorLoadInProgressDoesNotTriggerRediscovery() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        final NetworkClientDelegate.UnsentRequest unsent =
            manager.poll(time.milliseconds()).unsentRequests.get(0);
        unsent.handler().onComplete(buildResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, 0));

        assertTrue(streamsRebalanceData.topologyPushRequired());
        verify(coordinatorRequestManager, never()).markCoordinatorUnknown(any(), anyLong());
    }

    /**
     * Test the situation when the broker responds with a terminal error listed in KIP-1331: the
     * flag should be cleared and the client should not retry on its own.
     */
    @Test
    public void testTerminalErrorsClearFlag() {
        for (final Errors error : new Errors[]{
            Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED,
            Errors.INVALID_REQUEST,
            Errors.UNSUPPORTED_VERSION,
            Errors.GROUP_ID_NOT_FOUND,
            Errors.GROUP_AUTHORIZATION_FAILED}) {
            streamsRebalanceData.setWireTopologyDescription(
                new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
            streamsRebalanceData.setTopologyPushRequired(true);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            time.sleep(RETRY_BACKOFF_MAX_MS);

            final NetworkClientDelegate.UnsentRequest unsent =
                manager.poll(time.milliseconds()).unsentRequests.get(0);
            unsent.handler().onComplete(buildResponse(error, 0));

            assertFalse(streamsRebalanceData.topologyPushRequired(),
                "Flag should be cleared after " + error);
        }
    }

    /**
     * Test the situation when the push fails with a network exception: the
     * topologyPushRequired flag must remain set so the next poll retries, and a failed attempt
     * is recorded so retry backoff applies.
     */
    @Test
    public void testNetworkExceptionLeavesFlagSetWithBackoff() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        final NetworkClientDelegate.UnsentRequest unsent =
            manager.poll(time.milliseconds()).unsentRequests.get(0);
        unsent.handler().onFailure(time.milliseconds(), Errors.NETWORK_EXCEPTION.exception());

        assertTrue(streamsRebalanceData.topologyPushRequired());
        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());

        time.sleep(RETRY_BACKOFF_MAX_MS);
        assertEquals(1, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    /**
     * Test the situation when the response carries a non-zero ThrottleTimeMs: subsequent push
     * attempts should be delayed by that amount before resuming.
     */
    @Test
    public void testThrottleDelayBlocksSubsequentRequest() {
        streamsRebalanceData.setWireTopologyDescription(
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());
        streamsRebalanceData.setTopologyPushRequired(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        final NetworkClientDelegate.UnsentRequest unsent =
            manager.poll(time.milliseconds()).unsentRequests.get(0);
        unsent.handler().onComplete(buildResponse(Errors.NOT_COORDINATOR, 500));

        streamsRebalanceData.setTopologyPushRequired(true);
        assertEquals(0, manager.poll(time.milliseconds()).unsentRequests.size());

        time.sleep(501);
        assertEquals(1, manager.poll(time.milliseconds()).unsentRequests.size());
    }

    private ClientResponse buildResponse(final Errors error, final int throttleTimeMs) {
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE, (short) 0, "", 1),
            null,
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            new StreamsGroupTopologyDescriptionUpdateResponse(
                new StreamsGroupTopologyDescriptionUpdateResponseData()
                    .setErrorCode(error.code())
                    .setThrottleTimeMs(throttleTimeMs)
            )
        );
    }
}
