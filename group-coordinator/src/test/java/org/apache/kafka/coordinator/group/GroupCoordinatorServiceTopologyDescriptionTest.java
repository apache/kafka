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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.StreamsGroupDescribeResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.apache.kafka.coordinator.group.GroupConfigManagerTest.createConfigManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the topology-description plugin paths added to {@link GroupCoordinatorService}:
 * the new {@code streamsGroupTopologyDescriptionUpdate} RPC, the heartbeat post-processing
 * that sets {@code TopologyDescriptionRequired}, and the back-off interaction.
 */
public class GroupCoordinatorServiceTopologyDescriptionTest {

    private static final TopicPartition GROUP_TP = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);

    @SuppressWarnings("unchecked")
    private static CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> mockRuntime() {
        return (CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord>) mock(CoordinatorRuntime.class);
    }

    private static GroupCoordinatorService buildService(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        boolean startup
    ) {
        return buildService(runtime, plugin, startup, new MockTimer());
    }

    private static GroupCoordinatorService buildService(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        boolean startup,
        MockTimer timer
    ) {
        MockTime time = timer.time();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 600000L, 24),
            runtime,
            new GroupCoordinatorMetrics(),
            createConfigManager(),
            new NoOpStatePersister(),
            timer,
            null,
            plugin,
            time
        );
        if (startup) {
            service.startup(() -> 1);
        }
        return service;
    }

    @Test
    public void testUpdateRejectedWhenCoordinatorNotActive() throws Exception {
        GroupCoordinatorService service = buildService(mockRuntime(), Optional.empty(), false);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), response.errorCode());
    }

    @Test
    public void testUpdateReturnsUnsupportedVersionWhenNoPlugin() throws Exception {
        GroupCoordinatorService service = buildService(mockRuntime(), Optional.empty(), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.errorCode());
        assertNotNull(response.errorMessage());
    }

    @Test
    public void testUpdateRejectsEmptyMemberId() throws Exception {
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        GroupCoordinatorService service = buildService(mockRuntime(), Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest().setMemberId("")
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.INVALID_REQUEST.code(), response.errorCode());
        assertEquals("MemberId can't be empty.", response.errorMessage());
    }

    @Test
    public void testUpdateRejectsEmptyGroupId() throws Exception {
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        GroupCoordinatorService service = buildService(mockRuntime(), Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest().setGroupId("")
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.INVALID_REQUEST.code(), response.errorCode());
    }

    @Test
    public void testUpdateSuccessPersistsStoredEpoch() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NONE.code(), response.errorCode());
        verify(plugin, times(1)).setTopology(eq("foo"), eq(3), any());
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"), eq(GROUP_TP), any());

        // Back-off must be cleared on success: a subsequent heartbeat at the same epoch
        // (stored still lags in this mock-only world because the metadata-record write
        // is captured but not replayed) should set the flag again.
        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdatePermanentFailurePersistsFailedEpoch() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new StreamsTopologyDescriptionPermanentFailureException("too large")));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-set-failed-topology-epoch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), response.errorCode());
        assertEquals("too large", response.errorMessage());
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("streams-group-set-failed-topology-epoch"), eq(GROUP_TP), any());
    }

    @Test
    public void testUpdateTransientFailureWritesNoRecord() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("backend offline")));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), response.errorCode());
        assertEquals("backend offline", response.errorMessage());
        verify(runtime, never()).scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"), any(), any());
        verify(runtime, never()).scheduleWriteOperation(
            eq("streams-group-set-failed-topology-epoch"), any(), any());

        // Back-off must be armed on transient failure: a subsequent heartbeat at the same
        // epoch is suppressed rather than re-soliciting immediately.
        assertFalse(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdatePluginFutureWithNullCauseIsTreatedAsTransient() throws Exception {
        // CompletionException / ExecutionException can legally carry a null cause. If a
        // plugin completes its future with one of those (rare but legal), the handle()
        // callback must not NPE on cause.getMessage() — that would lose the
        // transient/permanent classification and surface UNKNOWN_SERVER_ERROR. The null
        // cause is treated as a transient failure with a generic message.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.failedFuture(new CompletionException(null)));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        // Treated as transient: STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED rather than
        // UNKNOWN_SERVER_ERROR; no metadata record written; back-off armed.
        assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), response.errorCode());
        assertNotNull(response.errorMessage());
        verify(runtime, never()).scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"), any(), any());
        verify(runtime, never()).scheduleWriteOperation(
            eq("streams-group-set-failed-topology-epoch"), any(), any());
        assertFalse(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdateBackoffArmedWhenStoredEpochWriteFailsAfterPluginSuccess() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.failedFuture(new RuntimeException("write failed")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        // The exceptionally branch translates the write failure into an error response.
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.errorCode());
        verify(plugin, times(1)).setTopology(eq("foo"), eq(3), any());

        // Plugin succeeded, write failed. BackoffAction defaulted to ARM and the thenApply
        // that would have set CLEAR never ran, so whenComplete armed the back-off.
        assertFalse(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdatePreValidationFailureDoesNotArmBackoff() throws Exception {
        // A fenced/stale member (or, once routing lands, an unauthorized caller) whose
        // validateStreamsGroupMember check fails must not arm the per-group back-off:
        // legitimate members of the same group must still get TopologyDescriptionRequired
        // on their next heartbeat at the same epoch.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.failedFuture(
            new UnknownMemberIdException("Member fenced from the group.")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), response.errorCode());
        verify(plugin, never()).setTopology(anyString(), anyInt(), any());

        // Pre-plugin failure must leave the back-off untouched. A legitimate heartbeat at
        // the same epoch must still get the flag — otherwise a fenced/unauthorized caller
        // could grief the entire group's re-solicitation until the back-off window expires.
        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdatePostPluginWriteRoutingFailureDoesNotArmBackoff() throws Exception {
        // The plugin succeeds, but between the plugin call and the bookkeeping write this
        // broker stops being the coordinator (NotCoordinatorException surfaces from the
        // write). The client will retry against the new coordinator, which holds no
        // back-off entry of its own; arming a broker-wide entry on this broker would leak
        // until expiry and could later suppress a legitimate solicitation if the group
        // migrates back. CoordinatorLoadInProgressException and CoordinatorNotAvailableException
        // travel the same NOOP branch — covered by one representative case to avoid
        // parameterized-test scaffolding.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.failedFuture(
            new NotCoordinatorException("Lost coordinator status between plugin success and bookkeeping write.")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NOT_COORDINATOR.code(), response.errorCode());
        verify(plugin, times(1)).setTopology(eq("foo"), eq(3), any());

        // Back-off must be untouched so the new coordinator (or this broker after a
        // migration back) can still solicit a fresh push at the same epoch.
        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdateGroupDisappearsBetweenPluginSuccessAndWriteDropsBackoffEntry() throws Exception {
        // KIP-1331 race: the plugin succeeds, then the group is deleted, then the bookkeeping
        // write for StoredDescriptionTopologyEpoch fails with GroupIdNotFoundException. The
        // push has already taken effect at the plugin and the group is gone, so the back-off
        // must be dropped rather than re-armed for a now-orphan group.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.failedFuture(
            new GroupIdNotFoundException("Group deleted between plugin success and bookkeeping write.")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), response.errorCode());
        verify(plugin, times(1)).setTopology(eq("foo"), eq(3), any());

        // Back-off must be dropped, not armed: nobody will ever clear it for a deleted group.
        // A subsequent heartbeat at the same epoch (if the group somehow comes back) must
        // still get the flag — i.e. the orphan back-off entry is gone.
        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 3, -1, -1));
    }

    @Test
    public void testUpdatePluginReturnsNullFutureIsTreatedAsPermanentFailure() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any())).thenReturn(null);
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-set-failed-topology-epoch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), response.errorCode());
        assertEquals("Topology description plugin failed.", response.errorMessage());
        // Treated as permanent failure: FailedDescriptionTopologyEpoch is written.
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("streams-group-set-failed-topology-epoch"), eq(GROUP_TP), any());
        verify(runtime, never()).scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"), any(), any());
    }

    @Test
    public void testHeartbeatSetsTopologyDescriptionRequiredWhenStoredLags() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, -1, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        assertTrue(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatSkipsFlagForDepartingMember() throws Exception {
        // A leave heartbeat carries a negative member epoch. Even though the result shows the
        // stored epoch lagging (which would otherwise solicit a push), the gate must not arm
        // the back-off or set the flag for a member on its way out.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, -1, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT),
            validHeartbeatRequest().setMemberEpoch(-1)
        ).get(5, TimeUnit.SECONDS);

        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatArmSuppressReSolicitCycle() throws Exception {
        // End-to-end exercise of the arm → suppress → re-solicit cycle through the
        // service + TopologyDescriptionManager. Backoff primitive tests cover this in
        // isolation; this asserts the contract holds when the heartbeat write
        // result is fed into maybeSetTopologyDescriptionRequired.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        // Each call must yield a fresh response so mutations from the previous call do not
        // bleed through. thenReturn would hand back the same instance to every invocation
        // and the second heartbeat would observe topologyDescriptionRequired carried over
        // from the first, masking the suppression we want to assert.
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenAnswer(invocation -> CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, -1, -1)));

        MockTimer timer = new MockTimer();
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, timer);

        // 1. First heartbeat — back-off idle. Manager arms it and sets the flag.
        StreamsGroupHeartbeatResult firstResult = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);
        assertTrue(firstResult.data().topologyDescriptionRequired());

        // 2. Second heartbeat — back-off window still active. Manager suppresses the flag.
        StreamsGroupHeartbeatResult secondResult = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);
        assertFalse(secondResult.data().topologyDescriptionRequired());

        // 3. Advance MockTime past the back-off window. INITIAL_DELAY_MS is 30s — the
        // value lives in StreamsGroupTopologyDescriptionBackoff as a package-private
        // constant; sleeping a comfortable margin past it keeps this test independent
        // of the exact delay while still asserting "past the initial window".
        timer.time().sleep(60_000L);

        // 4. Third heartbeat — window expired. Manager re-arms and sets the flag again.
        StreamsGroupHeartbeatResult thirdResult = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);
        assertTrue(thirdResult.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatSkipsFlagWhenStoredMatchesCurrent() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, 5, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatSkipsFlagWhenFailedAtCurrentEpoch() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, -1, 5)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatSkipsFlagWhenStaleTopologyStatusPresent() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                .setStatusCode(Status.STALE_TOPOLOGY.code())
                .setStatusDetail("behind")));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(responseData, Map.of(), 5, -1, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatDecorationFailurePreservesCommittedResponse() throws Exception {
        // If maybeSetTopologyDescriptionRequired throws while decorating an
        // already-committed successful heartbeat (for example, because the response carries
        // an unexpected shape such as a null Status element), the service must NOT translate
        // that into an error response — the broker-side state change has already happened,
        // so we return the committed result as-is and let the next heartbeat retry.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        // Construct a heartbeat result where responseHasStaleTopology will NPE on the null
        // status element. errorCode is NONE so we exercise the success path.
        StreamsGroupHeartbeatResponseData response = new StreamsGroupHeartbeatResponseData()
            .setStatus(Collections.singletonList(null));
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(response, Map.of(), 5, -1, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        // No error translation: the response carries NONE and the original status, the flag
        // is left unset because decoration could not complete.
        assertEquals(Errors.NONE.code(), result.data().errorCode());
        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatNeverSetsFlagWithoutPlugin() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, -1, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.empty(), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testHeartbeatSkipsFlagOnV0Request() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of(), 5, -1, -1)));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT, (short) 0), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);

        assertFalse(result.data().topologyDescriptionRequired());
    }

    @Test
    public void testShutdownClosesPlugin() throws Exception {
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        GroupCoordinatorService service = buildService(mockRuntime(), Optional.of(plugin), true);
        service.shutdown();
        verify(plugin, times(1)).close();
    }

    @Test
    public void testDeleteGroupsPluginFailureReturnsGroupDeletionFailed() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("plugin offline")));

        when(runtime.scheduleWriteOperation(
            eq("delete-share-groups"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-pre-delete"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Set.of("foo")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        DeleteGroupsResponseData.DeletableGroupResultCollection results =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                List.of("foo"),
                BufferSupplier.NO_CACHING
            ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult result = results.find("foo");
        assertNotNull(result);
        assertEquals(Errors.GROUP_DELETION_FAILED.code(), result.errorCode());
        // The raw plugin message ("plugin offline") must not be forwarded to the client.
        assertEquals("Topology description plugin failed to delete the topology.", result.errorMessage());
        verify(runtime, never()).scheduleWriteOperation(
            eq("delete-groups"), any(), any());
    }

    @Test
    public void testDeleteGroupsPluginSuccessProceedsToTombstone() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo"))
            .thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleWriteOperation(
            eq("delete-share-groups"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-pre-delete"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Set.of("foo")));

        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("foo"));
        when(runtime.scheduleWriteOperation(
            eq("delete-groups"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        DeleteGroupsResponseData.DeletableGroupResultCollection results =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                List.of("foo"),
                BufferSupplier.NO_CACHING
            ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult result = results.find("foo");
        assertNotNull(result);
        assertEquals(Errors.NONE.code(), result.errorCode());
        assertNull(result.errorMessage());
        verify(plugin, times(1)).deleteTopology("foo");
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("delete-groups"), eq(GROUP_TP), any());
    }

    @Test
    public void testDeleteGroupsWithoutPluginSkipsPluginCall() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        when(runtime.scheduleWriteOperation(
            eq("delete-share-groups"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));

        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("foo"));
        when(runtime.scheduleWriteOperation(
            eq("delete-groups"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.empty(), true);

        DeleteGroupsResponseData.DeletableGroupResultCollection results =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                List.of("foo"),
                BufferSupplier.NO_CACHING
            ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult result = results.find("foo");
        assertNotNull(result);
        assertEquals(Errors.NONE.code(), result.errorCode());
        verify(runtime, never()).scheduleReadOperation(
            eq("streams-group-topology-pre-delete"), any(), any());
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("delete-groups"), eq(GROUP_TP), any());
    }

    @Test
    public void testDeleteGroupsSkipsPluginCallWhenNoStoredTopology() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        when(runtime.scheduleWriteOperation(
            eq("delete-share-groups"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-pre-delete"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Set.of()));

        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("foo"));
        when(runtime.scheduleWriteOperation(
            eq("delete-groups"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        DeleteGroupsResponseData.DeletableGroupResultCollection results =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                List.of("foo"),
                BufferSupplier.NO_CACHING
            ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult result = results.find("foo");
        assertNotNull(result);
        assertEquals(Errors.NONE.code(), result.errorCode());
        verify(plugin, never()).deleteTopology(anyString());
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("delete-groups"), eq(GROUP_TP), any());
    }

    @Test
    public void testDeleteGroupsMixedPluginOutcome() throws Exception {
        // Two streams groups on the same partition; plugin succeeds for "good", fails for "bad".
        // Only "good" should reach the underlying delete-groups write; "bad" surfaces as
        // GROUP_DELETION_FAILED in the response.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("good"))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(plugin.deleteTopology("bad"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("rejected")));

        when(runtime.scheduleWriteOperation(
            eq("delete-share-groups"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-pre-delete"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Set.of("good", "bad")));

        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("good"));
        when(runtime.scheduleWriteOperation(
            eq("delete-groups"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        DeleteGroupsResponseData.DeletableGroupResultCollection results =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                List.of("good", "bad"),
                BufferSupplier.NO_CACHING
            ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult goodResult = results.find("good");
        assertNotNull(goodResult);
        assertEquals(Errors.NONE.code(), goodResult.errorCode());

        DeleteGroupsResponseData.DeletableGroupResult badResult = results.find("bad");
        assertNotNull(badResult);
        assertEquals(Errors.GROUP_DELETION_FAILED.code(), badResult.errorCode());
        assertEquals("Topology description plugin failed to delete the topology.", badResult.errorMessage());
    }

    @Test
    public void testDescribeWithIncludeFlagDisabledLeavesStatusDefault() throws Exception {
        // includeTopologyDescription=false -> plugin is not consulted regardless of whether
        // the group would otherwise be eligible. TopologyDescriptionStatus stays at the
        // default 0 (NOT_REQUESTED) and the response carries no topologyDescription.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = describedGroupWithTopology("foo", 5);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroup), Map.of("foo", 5))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), false
        ).get(5, TimeUnit.SECONDS);

        assertEquals(1, result.size());
        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_REQUESTED, result.get(0).topologyDescriptionStatus());
        assertNull(result.get(0).topologyDescription());
        verify(plugin, never()).getTopology(anyString(), anyInt());
    }

    @Test
    public void testDescribeSetsNotStoredWhenNoPluginConfigured() throws Exception {
        // includeTopologyDescription=true but no plugin on this broker: every successful group
        // becomes NOT_STORED (the broker has nothing to serve). The errored group keeps its
        // default 0 status because the client should consult the group's errorCode first.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();

        StreamsGroupDescribeResponseData.DescribedGroup goodGroup = describedGroupWithTopology("good", 5);
        StreamsGroupDescribeResponseData.DescribedGroup errorGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("error")
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code());
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(goodGroup, errorGroup), Map.of("good", 5))));

        GroupCoordinatorService service = buildService(runtime, Optional.empty(), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("good", "error"), true
        ).get(5, TimeUnit.SECONDS);

        StreamsGroupDescribeResponseData.DescribedGroup good = findByGroupId(result, "good");
        StreamsGroupDescribeResponseData.DescribedGroup err = findByGroupId(result, "error");
        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED, good.topologyDescriptionStatus());
        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_REQUESTED, err.topologyDescriptionStatus());
    }

    @Test
    public void testDescribeFiltersNonEligibleGroupsToNotStored() throws Exception {
        // One test covers the three synchronous "no plugin call" branches: topology() == null,
        // storedEpoch missing from the result map, storedEpoch mismatched. All three must
        // resolve to NOT_STORED without invoking the plugin.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        StreamsGroupDescribeResponseData.DescribedGroup noTopology = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("no-topology"); // topology field is null
        StreamsGroupDescribeResponseData.DescribedGroup epochMissing = describedGroupWithTopology("epoch-missing", 4);
        StreamsGroupDescribeResponseData.DescribedGroup epochMismatch = describedGroupWithTopology("epoch-mismatch", 4);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(
                    List.of(noTopology, epochMissing, epochMismatch),
                    // "epoch-missing" absent from map; "epoch-mismatch" stored at 7 vs current 4.
                    Map.of("epoch-mismatch", 7))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE),
            List.of("no-topology", "epoch-missing", "epoch-mismatch"),
            true
        ).get(5, TimeUnit.SECONDS);

        for (StreamsGroupDescribeResponseData.DescribedGroup g : result) {
            assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED,
                g.topologyDescriptionStatus(), "group " + g.groupId());
        }
        verify(plugin, never()).getTopology(anyString(), anyInt());
    }

    @Test
    public void testDescribeAttachesAvailableWhenPluginReturnsTopology() throws Exception {
        // Happy path: storedEpoch == currentEpoch, plugin returns a non-null topology, the
        // wire-schema topology is attached and the status becomes AVAILABLE.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        StreamsGroupTopologyDescription pojo = new StreamsGroupTopologyDescription(
            List.of(new StreamsGroupTopologyDescription.Subtopology("sub-0", List.of(
                new StreamsGroupTopologyDescription.Source("src", Set.of("input"), Set.of())))),
            List.of()
        );
        when(plugin.getTopology("foo", 5)).thenReturn(CompletableFuture.completedFuture(pojo));

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = describedGroupWithTopology("foo", 5);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroup), Map.of("foo", 5))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        StreamsGroupDescribeResponseData.DescribedGroup g = result.get(0);
        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE, g.topologyDescriptionStatus());
        assertNotNull(g.topologyDescription());
        assertEquals(1, g.topologyDescription().subtopologies().size());
        assertEquals("sub-0", g.topologyDescription().subtopologies().get(0).subtopologyId());
    }

    @Test
    public void testDescribeMarksErrorWhenPluginFails() throws Exception {
        // Plugin future completes exceptionally -> ERROR; the converted topology is not set.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("plugin offline")));

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = describedGroupWithTopology("foo", 5);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroup), Map.of("foo", 5))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        StreamsGroupDescribeResponseData.DescribedGroup g = result.get(0);
        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_ERROR, g.topologyDescriptionStatus());
        assertNull(g.topologyDescription());
    }

    @Test
    public void testDescribeMarksErrorWhenPluginThrowsSynchronously() throws Exception {
        // SPI contract violation: synchronous throw is treated the same as an exceptional
        // future -> ERROR. The exception must not propagate out of the describe response.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5)).thenThrow(new RuntimeException("synthetic sync throw"));

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = describedGroupWithTopology("foo", 5);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroup), Map.of("foo", 5))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_ERROR,
            result.get(0).topologyDescriptionStatus());
    }

    @Test
    public void testDescribeMarksNotStoredWhenPluginReturnsNullDescription() throws Exception {
        // Plugin's future completes with null -> the plugin no longer holds the data
        // (e.g. backend wipe). Treat as NOT_STORED, not ERROR — the broker successfully
        // queried the plugin and learned there is nothing to return.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5)).thenReturn(CompletableFuture.completedFuture(null));

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = describedGroupWithTopology("foo", 5);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroup), Map.of("foo", 5))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED,
            result.get(0).topologyDescriptionStatus());
        assertNull(result.get(0).topologyDescription());
    }

    @Test
    public void testDescribeLeavesErroredGroupsAlone() throws Exception {
        // Groups with non-NONE errorCode are not eligible for topology attach; status stays
        // at the default 0 (NOT_REQUESTED). The plugin must not be called for them.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        StreamsGroupDescribeResponseData.DescribedGroup errorGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("err")
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code());
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(errorGroup), Map.of())));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("err"), true
        ).get(5, TimeUnit.SECONDS);

        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_REQUESTED,
            result.get(0).topologyDescriptionStatus());
        verify(plugin, never()).getTopology(anyString(), anyInt());
    }

    private static StreamsGroupDescribeResponseData.DescribedGroup describedGroupWithTopology(
        String groupId, int topologyEpoch
    ) {
        return new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.NONE.code())
            .setTopology(new StreamsGroupDescribeResponseData.Topology().setEpoch(topologyEpoch));
    }

    private static StreamsGroupDescribeResponseData.DescribedGroup findByGroupId(
        List<StreamsGroupDescribeResponseData.DescribedGroup> groups, String groupId
    ) {
        return groups.stream().filter(g -> g.groupId().equals(groupId)).findFirst().orElseThrow();
    }

    private static StreamsGroupTopologyDescriptionUpdateRequestData validUpdateRequest() {
        return new StreamsGroupTopologyDescriptionUpdateRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setTopologyEpoch(3)
            .setTopologyDescription(
                new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                    .setSubtopologies(List.of())
                    .setGlobalStores(List.of()));
    }

    private static StreamsGroupHeartbeatRequestData validHeartbeatRequest() {
        return new StreamsGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1500)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of())
            .setTopology(new StreamsGroupHeartbeatRequestData.Topology());
    }

    /**
     * Drive a heartbeat through the service after an update test and report whether the
     * topology-description-required flag was set on the response. Used to observe the
     * back-off state behaviourally: the flag is set iff the back-off window is not in
     * effect for the given epoch, so the assertion stands in for "back-off cleared" vs
     * "back-off armed".
     */
    private static boolean heartbeatTopologyDescriptionRequired(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        GroupCoordinatorService service,
        int currentEpoch,
        int storedEpoch,
        int failedEpoch
    ) throws Exception {
        when(runtime.scheduleWriteOperation(
            eq("streams-group-heartbeat"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(),
            Map.of(),
            currentEpoch,
            storedEpoch,
            failedEpoch
        )));
        StreamsGroupHeartbeatResult result = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT), validHeartbeatRequest()
        ).get(5, TimeUnit.SECONDS);
        return result.data().topologyDescriptionRequired();
    }
}
