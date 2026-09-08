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
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
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
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupDescribeResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupTopologyDescriptionConverter;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
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
        return buildService(runtime, plugin, startup, timer, new GroupCoordinatorMetrics());
    }

    private static GroupCoordinatorService buildService(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        boolean startup,
        MockTimer timer,
        GroupCoordinatorMetrics metrics
    ) {
        MockTime time = timer.time();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 600000L, 24),
            runtime,
            metrics,
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
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
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
    public void testUpdateSkipsPluginWhenBarrierWriteFindsGroupGone() throws Exception {
        // The UNCERTAIN barrier write returns false when the group vanished (or stopped being a
        // streams group) between the validate read and the write. No barrier record exists, so the
        // push must not create a plugin entry that no cleanup path would ever reclaim.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadOperation(
            eq("streams-group-topology-description-validate"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.FALSE));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        StreamsGroupTopologyDescriptionUpdateResponseData response = service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), response.errorCode());
        verify(plugin, never()).setTopology(anyString(), anyInt(), any());
        verify(runtime, never()).scheduleWriteOperation(
            eq("streams-group-set-stored-topology-epoch"), any(), any());
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
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
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
        when(runtime.scheduleWriteOperation(
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

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
        when(runtime.scheduleWriteOperation(
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

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
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
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
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
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
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
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
            eq("mark-topology-uncertain"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
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
        when(runtime.scheduleWriteOperation(
            eq("mark-topology-uncertain-batch"),
            any(),
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
        when(runtime.scheduleWriteOperation(
            eq("mark-topology-uncertain-batch"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(
            eq("finalize-stored-topology-epoch-after-delete-batch"),
            eq(GROUP_TP),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

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
        // The successful plugin delete is smart-finalized before the tombstone, so a group that
        // was revived past the tombstone cannot keep a raced push's epoch over the emptied plugin.
        verify(runtime, times(1)).scheduleWriteOperation(
            eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any());
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
        // No stored topology in the batch: the UNCERTAIN mark write must be skipped entirely,
        // not scheduled as a no-op on the shard's event loop.
        verify(runtime, never()).scheduleWriteOperation(
            eq("mark-topology-uncertain-batch"), any(), any());
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
        when(runtime.scheduleWriteOperation(
            eq("mark-topology-uncertain-batch"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Set.of("good", "bad")));
        when(runtime.scheduleWriteOperation(
            eq("finalize-stored-topology-epoch-after-delete-batch"),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(null));

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
    public void testDeleteGroupsPreservesBackoffOnPluginFailure() throws Exception {
        // Symmetric with the cleanup cycle (testCleanupCyclePreservesBackoffOnPluginFailure): a
        // failed plugin.deleteTopology in DeleteGroups leaves the group at UNCERTAIN(-2) — not
        // tombstoned — which re-solicits a push on the next heartbeat. The back-off entry must
        // survive so a rejoining member does not immediately re-attack the still-broken plugin.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("plugin offline")));
        when(runtime.scheduleWriteOperation(eq("delete-share-groups"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-pre-delete"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().armBackoff("foo", 4);
        service.deleteGroups(
            requestContext(ApiKeys.DELETE_GROUPS), List.of("foo"), BufferSupplier.NO_CACHING
        ).get(5, TimeUnit.SECONDS);

        assertFalse(heartbeatTopologyDescriptionRequired(runtime, service, 4, 2, -1),
            "failed plugin delete in DeleteGroups must not clear the back-off entry");
    }

    @Test
    public void testDeleteGroupsClearsBackoffOnPluginSuccess() throws Exception {
        // Counterpart: a successful plugin.deleteTopology means the group is on its way to
        // tombstone, so its back-off entry is no longer load-bearing and is cleared.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(eq("delete-share-groups"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-pre-delete"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("foo"));
        when(runtime.scheduleWriteOperation(eq("delete-groups"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().armBackoff("foo", 4);
        service.deleteGroups(
            requestContext(ApiKeys.DELETE_GROUPS), List.of("foo"), BufferSupplier.NO_CACHING
        ).get(5, TimeUnit.SECONDS);

        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 4, 2, -1),
            "successful plugin delete in DeleteGroups must clear the back-off entry");
    }

    @Test
    public void testDeleteGroupsMarksUncertainBeforePluginDelete() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("g")).thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleWriteOperation(eq("delete-share-groups"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-pre-delete"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("g")));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("g")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId("g"));
        when(runtime.scheduleWriteOperation(eq("delete-groups"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.deleteGroups(
            requestContext(ApiKeys.DELETE_GROUPS),
            List.of("g"),
            BufferSupplier.NO_CACHING
        ).get(5, TimeUnit.SECONDS);

        InOrder inOrder = inOrder(runtime, plugin);
        inOrder.verify(runtime).scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any());
        inOrder.verify(plugin).deleteTopology("g");
        inOrder.verify(runtime).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
        inOrder.verify(runtime).scheduleWriteOperation(eq("delete-groups"), any(), any());
    }

    @Test
    public void testDeleteGroupsSkipsPluginDeleteForRevivedGroup() throws Exception {
        // The pre-delete read finds "g" with a stored topology, but the mark batch returns an
        // empty subset: "g" was revived between the committed read and the barrier write. The
        // plugin delete must be skipped (no barrier was written), no plugin failure recorded for
        // it, and the pipeline must still proceed to the tombstone write, which reports the
        // group's fate (NON_EMPTY_GROUP for a revived group).
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        when(runtime.scheduleWriteOperation(eq("delete-share-groups"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-pre-delete"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("g")));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of()));

        DeleteGroupsResponseData.DeletableGroupResultCollection tombstoneResult =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        tombstoneResult.add(new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("g")
            .setErrorCode(Errors.NON_EMPTY_GROUP.code()));
        when(runtime.scheduleWriteOperation(eq("delete-groups"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(tombstoneResult));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        DeleteGroupsResponseData.DeletableGroupResultCollection results = service.deleteGroups(
            requestContext(ApiKeys.DELETE_GROUPS),
            List.of("g"),
            BufferSupplier.NO_CACHING
        ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult result = results.find("g");
        assertNotNull(result);
        assertEquals(Errors.NON_EMPTY_GROUP.code(), result.errorCode());
        verify(plugin, never()).deleteTopology(anyString());
        // No plugin delete ran, so there is nothing to smart-finalize either.
        verify(runtime, never()).scheduleWriteOperation(
            eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
        verify(runtime, times(1)).scheduleWriteOperation(eq("delete-groups"), eq(GROUP_TP), any());
    }

    @Test
    public void testCleanupCycleNoOpWhenNoPlugin() {
        // No plugin configured -> the cycle must not even touch the runtime.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = buildService(runtime, Optional.empty(), true);

        service.runOneStreamsTopologyCleanupCycle();

        verify(runtime, never()).scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any());
        verify(runtime, never()).scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any());
    }

    @Test
    public void testCleanupCycleMarksUncertainThenDeletesThenFinalizes() {
        // The cycle must write the UNCERTAIN barrier batch before the plugin delete and run the
        // smart finalize after it, in that order.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));

        Set<String> eligible = Set.of("foo");
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(eligible)));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        InOrder inOrder = inOrder(runtime, plugin);
        inOrder.verify(runtime).scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any());
        inOrder.verify(plugin).deleteTopology("foo");
        inOrder.verify(runtime).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testCleanupCycleSkipsDeleteForRevivedGroup() {
        // The mark batch returns an empty subset: "foo" was revived (or converted) between the
        // committed scan and the mark write. The cycle must not delete it nor finalize.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        Set<String> eligible = Set.of("foo");
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(eligible)));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of()));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(plugin, never()).deleteTopology(any());
        verify(runtime, never()).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testCleanupCycleSwallowsMarkWriteFailure() throws Exception {
        // A routine write failure (e.g. NOT_COORDINATOR during a shard move) on the UNCERTAIN
        // mark must not fail the whole cycle's allOf: the partition is skipped with a targeted
        // warn and the next cycle retries. No plugin delete and no finalize may run for it.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(Errors.NOT_COORDINATOR.exception()));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle().get(5, TimeUnit.SECONDS);

        verify(plugin, never()).deleteTopology(any());
        verify(runtime, never()).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testCleanupCycleClearsStoredEpochOnPluginSuccess() {
        // Eligibility scan returns one group at storedEpoch=4; the mark batch marks it UNCERTAIN;
        // plugin succeeds; the cycle must schedule the smart finalize so a concurrent setTopology
        // that has advanced the field is re-solicited rather than silently undone.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(plugin, times(1)).deleteTopology("foo");
        verify(runtime, times(1)).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any());
    }

    @Test
    public void testCleanupCycleBatchesClearWritesPerPartition() {
        // Two eligible groups land on the same partition's eligibility read — they must trigger
        // exactly one finalize scheduleWriteOperation carrying both groups, not one write per
        // group.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));
        when(plugin.deleteTopology("bar")).thenReturn(CompletableFuture.completedFuture(null));
        Set<String> eligible = new LinkedHashSet<>(List.of("foo", "bar"));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(eligible)));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo", "bar")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(plugin, times(1)).deleteTopology("foo");
        verify(plugin, times(1)).deleteTopology("bar");
        // One finalize write covers both groups; not two per-group writes.
        verify(runtime, times(1)).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testCleanupCycleSkipsClearOnPluginFailure() {
        // Plugin delete fails -> the cycle must NOT finalize; the group is left at -2 so it stays
        // delete-eligible and re-soliciting, and the next cycle retries the plugin call.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("plugin offline")));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(plugin, times(1)).deleteTopology("foo");
        verify(runtime, never()).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testCleanupCyclePreservesBackoffOnPluginFailure() throws Exception {
        // Unconditionally clearing the broker-wide back-off entry on
        // a failed plugin.deleteTopology bypasses push-path ratchet for any group
        // the cycle touches. If a member rejoins between the failing scan and the next cycle,
        // the push-path back-off check finds no entry and re-attacks the broken plugin at
        // attempts=0 every join. The cycle must leave the entry in place so the existing
        // exponential window still throttles concurrent set-topology pushes.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("plugin offline")));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        // Arm a back-off entry at the same currentEpoch we will probe with the heartbeat helper,
        // then run a failing cycle. The helper's gate calls armIfNotActive at that epoch — if
        // the cycle wiped the entry the heartbeat would arm freshly and set the flag; if the
        // entry survived the heartbeat sees an active window and the flag stays unset.
        service.streamsGroupTopologyDescriptionManager().armBackoff("foo", 4);
        service.runOneStreamsTopologyCleanupCycle();

        assertFalse(heartbeatTopologyDescriptionRequired(runtime, service, 4, 2, -1),
            "failed cycle must not clear the back-off entry");
    }

    @Test
    public void testCleanupCycleClearsBackoffOnPluginSuccess() throws Exception {
        // Symmetric counterpart: a successful plugin.deleteTopology means the group is on its
        // way to tombstone — the back-off entry is no longer load-bearing for any future state
        // of this groupId. The cycle clears it; a subsequent re-creation of the same id is a
        // fresh lifecycle and arms a fresh chain.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().armBackoff("foo", 4);
        service.runOneStreamsTopologyCleanupCycle();

        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 4, 2, -1),
            "successful cycle must clear the back-off so a fresh solicitation can arm");
    }

    @Test
    public void testCleanupCycleEmptyEligibility() {
        // No groups eligible -> the mark batch is not scheduled and the plugin is not called.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of())));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(plugin, never()).deleteTopology(anyString());
        verify(runtime, never()).scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any());
    }

    @Test
    public void testCleanupCycleSingleFlightSkipsConcurrentCycle() {
        // The first cycle's per-partition read is parked on an unresolved future. A second
        // call must observe cleanupCycleInFlight and skip without scheduling another read.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(new CompletableFuture<>()));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);

        verify(runtime, times(1)).scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any());
    }

    @Test
    public void testCleanupCycleSingleFlightHoldsFlagUntilClearWriteSettles() {
        // Locks the fix for the gap Copilot flagged: invokeDeleteTopologies's plugin call
        // completes synchronously, but the smart-finalize write is parked on an unresolved
        // future. Until that write settles, the in-flight flag must remain held — a fresh cycle
        // scheduled by the timer would otherwise re-scan the same eligible group (storedEpoch
        // still != -1 because the finalize has not landed) and double-fire plugin.deleteTopology.
        // After the parked write completes the flag is released and a subsequent cycle observes a
        // fresh scheduleReadAllOperation.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        CompletableFuture<Object> parkedFinalizeWrite = new CompletableFuture<>();
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any()))
            .thenReturn(parkedFinalizeWrite);

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);
        // Plugin call resolved synchronously but finalize-write is parked — second cycle skipped.
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);
        verify(runtime, times(1)).scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any());

        // Settle the finalize-write: flag should now release, next cycle scans afresh.
        parkedFinalizeWrite.complete(null);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);
        verify(runtime, times(2)).scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any());
    }

    @Test
    public void testCleanupCycleSingleFlightReleasesFlagOnSynchronousThrowDuringChainConstruction() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenThrow(new RuntimeException("synthetic runtime failure during chain construction"))
            .thenReturn(List.of());

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        // First tick throws — the cycle must still release the flag so a subsequent tick runs.
        assertThrows(RuntimeException.class,
            () -> service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle));
        // Second tick must reach the runtime read, confirming the flag was released.
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);

        verify(runtime, times(2)).scheduleReadAllOperation(
            eq("list-streams-groups-needing-topology-cleanup"), any());
    }

    @Test
    public void testCleanupCycleSingleFlightReleasesFlagAfterCycleCompletes() {
        // The skip case alone does not prove the flag is ever released: a buggy whenComplete
        // (e.g., missing the partitionDone allOf join) would leave it set forever and silently
        // disable every subsequent cycle. Drive a full cycle to completion (read resolves,
        // plugin delete settles, smart finalize write settles), then issue a second cycle
        // and verify it observes the released flag by scheduling a fresh read.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("foo")).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(Set.of("foo"))));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("foo")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);
        // Same runtime, second invocation: must schedule another read (flag released).
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);

        verify(runtime, times(2)).scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any());
    }

    @Test
    public void testCleanupCycleSkipsFollowUpWorkOncePastShutdown() throws Exception {
        // TimerTask.cancel() does not await an in-flight cycle, so a cycle that has already
        // passed the CAS when shutdown fires would otherwise run plugin.deleteTopology and
        // follow-up scheduleWriteOperation against a manager and runtime that are about to
        // close. The per-partition handle inside the manager's cycle now checks the
        // manager's running flag before dispatching the plugin call; this locks that
        // behavior.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        CompletableFuture<Set<String>> parkedRead = new CompletableFuture<>();
        when(runtime.<Set<String>>scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(parkedRead));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        // Cycle dispatched, parked on the per-partition read.
        service.runOneStreamsTopologyCleanupCycle();
        // service.shutdown() closes the manager which flips its running flag false and
        // cancels the scheduled task (and closes the mocked runtime; mocks remain callable
        // for verification).
        service.shutdown();
        // Now resolve the read: the handle runs under running==false and must skip the
        // mark batch, plugin dispatch, and finalize write that would have followed.
        parkedRead.complete(Set.of("foo"));

        verify(plugin, never()).deleteTopology(anyString());
        verify(runtime, never()).scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any());
    }

    @Test
    public void testShutdownCancelsScheduledCleanupTask() throws Exception {
        // startup() with a plugin configured arms the manager's periodic cleanup tick;
        // shutdown() must close the manager so the timer queue does not retain a
        // self-rescheduling task referencing a torn-down runtime. MockTimer.size() filters
        // cancelled entries, so observing 1 → 0 confirms manager.close()'s cancel() landed;
        // advancing the clock past the interval afterwards must not fire the task — both
        // the queue-skip on cancellation and the TimerTask body's running==false defensive
        // return guard against it.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        MockTimer timer = new MockTimer();
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, timer);

        assertEquals(1, timer.size());

        service.shutdown();

        assertEquals(0, timer.size());
        // No cycle should fire even with the clock advanced well past the cleanup interval.
        timer.advanceClock(Duration.ofHours(2).toMillis());
        verify(runtime, never()).scheduleReadAllOperation(
            eq("list-streams-groups-needing-topology-cleanup"), any());
    }

    @Test
    public void testShutdownSafeWhenNoCleanupTaskScheduled() {
        // Plugin absent => manager.startCleanupCycle short-circuits before scheduling a tick,
        // so the scheduledTask field stays null. shutdown() must tolerate the null snapshot
        // without throwing — broker close paths must not propagate.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTimer timer = new MockTimer();
        GroupCoordinatorService service = buildService(runtime, Optional.empty(), true, timer);

        assertEquals(0, timer.size());
        service.shutdown();
        assertEquals(0, timer.size());
    }

    @Test
    public void testCleanupCycleSingleFlightReleasesFlagOnEmptyPartitionList() {
        // Pathological boundary: zero hosted partitions (e.g. broker just started, nothing
        // loaded yet). partitionFutures is empty -> allOf(empty) -> immediate completion ->
        // whenComplete still fires and releases the flag. A subsequent cycle must run.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of());

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);
        service.streamsGroupTopologyDescriptionManager().runOnce(service::runOneStreamsTopologyCleanupCycle);

        verify(runtime, times(2)).scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any());
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

    @Test
    public void testUpdateSuccessRecordsSetSuccessSensor() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-description-validate"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
        when(runtime.scheduleWriteOperation(eq("streams-group-set-stored-topology-epoch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE), validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME);
    }

    @Test
    public void testUpdatePermanentFailureRecordsSetErrorSensor() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new StreamsTopologyDescriptionPermanentFailureException("too large")));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-description-validate"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
        when(runtime.scheduleWriteOperation(eq("streams-group-set-failed-topology-epoch"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE), validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME);
    }

    @Test
    public void testUpdateTransientFailureRecordsSetErrorSensor() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("backend offline")));
        when(runtime.scheduleReadOperation(eq("streams-group-topology-description-validate"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE), validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        // A transient failure is still a failed plugin.setTopology call: it folds into set-error,
        // not a separate sensor.
        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME);
    }

    @Test
    public void testDescribeAvailableRecordsGetSuccessSensor() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        StreamsGroupTopologyDescription pojo = new StreamsGroupTopologyDescription(
            List.of(new StreamsGroupTopologyDescription.Subtopology("sub-0", List.of(
                new StreamsGroupTopologyDescription.Source("src", Set.of("input"), Set.of())))),
            List.of()
        );
        when(plugin.getTopology("foo", 5)).thenReturn(CompletableFuture.completedFuture(pojo));
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroupWithTopology("foo", 5)), Map.of("foo", 5))));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
    }

    @Test
    public void testDescribeNullReturnRecordsGetSuccessSensor() throws Exception {
        // A getTopology returning null is a successful plugin call that found no data
        // (surfaces as NOT_STORED) and must count as get-success, not get-error.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5)).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroupWithTopology("foo", 5)), Map.of("foo", 5))));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
    }

    @Test
    public void testDescribeErrorRecordsGetErrorSensor() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("plugin offline")));
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroupWithTopology("foo", 5)), Map.of("foo", 5))));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
    }

    @Test
    public void testDescribeSynchronousThrowRecordsGetErrorSensor() throws Exception {
        // A getTopology that throws synchronously violates the SPI contract and surfaces as
        // ERROR; it must count as get-error.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5)).thenThrow(new RuntimeException("plugin blew up"));
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroupWithTopology("foo", 5)), Map.of("foo", 5))));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
    }

    @Test
    public void testDescribeNullFutureRecordsGetErrorSensor() throws Exception {
        // A getTopology that returns a null future violates the SPI contract and surfaces as
        // ERROR; it must count as get-error.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.getTopology("foo", 5)).thenReturn(null);
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroupWithTopology("foo", 5)), Map.of("foo", 5))));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
        ).get(5, TimeUnit.SECONDS);

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
    }

    @Test
    public void testDescribeConversionErrorRecordsGetErrorSensor() throws Exception {
        // The plugin returns a valid topology, but converting it to the wire response throws.
        // That defensive catch in applyGetTopologyOutcome is otherwise unreachable (the Node type
        // is sealed to Source/Processor/Sink, so a well-formed topology always converts), so we
        // force toDescribeResponse to throw to exercise it. The outcome surfaces as ERROR to the
        // client and must count as get-error, not get-success.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        StreamsGroupTopologyDescription pojo = new StreamsGroupTopologyDescription(
            List.of(new StreamsGroupTopologyDescription.Subtopology("sub-0", List.of(
                new StreamsGroupTopologyDescription.Source("src", Set.of("input"), Set.of())))),
            List.of()
        );
        when(plugin.getTopology("foo", 5)).thenReturn(CompletableFuture.completedFuture(pojo));
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroupWithTopology("foo", 5)), Map.of("foo", 5))));

        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true, new MockTimer(), metrics);

        // CALLS_REAL_METHODS so only toDescribeResponse is overridden; any other converter call
        // (none on this path) keeps its real behavior.
        try (MockedStatic<StreamsGroupTopologyDescriptionConverter> converter =
                 mockStatic(StreamsGroupTopologyDescriptionConverter.class, CALLS_REAL_METHODS)) {
            converter.when(() -> StreamsGroupTopologyDescriptionConverter.toDescribeResponse(any()))
                .thenThrow(new RuntimeException("conversion failed"));

            service.streamsGroupDescribe(
                requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("foo"), true
            ).get(5, TimeUnit.SECONDS);
        }

        verify(metrics).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
        verify(metrics, never()).recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
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

    @Test
    public void testPushMarksUncertainBeforePluginSetThenAdvances() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.setTopology(eq("foo"), eq(3), any())).thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleReadOperation(eq("streams-group-topology-description-validate"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
        when(runtime.scheduleWriteOperation(eq("streams-group-set-stored-topology-epoch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.streamsGroupTopologyDescriptionUpdate(
            requestContext(ApiKeys.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_UPDATE),
            validUpdateRequest()
        ).get(5, TimeUnit.SECONDS);

        InOrder inOrder = inOrder(runtime, plugin);
        inOrder.verify(runtime).scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any());
        inOrder.verify(plugin).setTopology(eq("foo"), eq(3), any());
        inOrder.verify(runtime).scheduleWriteOperation(eq("streams-group-set-stored-topology-epoch"), any(), any());
    }

    private static JoinGroupRequestData classicJoinRequest(String groupId) {
        return new JoinGroupRequestData()
            .setGroupId(groupId)
            .setSessionTimeoutMs(30000);
    }

    @Test
    public void testClassicJoinDeletesTopologyBeforeConvertingEmptyStreamsGroup() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("g")).thenReturn(CompletableFuture.completedFuture(null));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        // The first classic-group-join detects the empty streams group with a stored topology and
        // returns true (cleanup needed, no conversion); the second runs after cleanup and converts,
        // returning false.
        when(runtime.scheduleWriteOperation(eq("classic-group-join"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE))
            .thenReturn(CompletableFuture.completedFuture(Boolean.FALSE));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        // classicGroupJoin completes responseFuture internally; with a mock runtime that operation
        // never runs, so we await the scheduling of the join writes rather than the response.
        service.joinGroup(requestContext(ApiKeys.JOIN_GROUP), classicJoinRequest("g"), BufferSupplier.NO_CACHING);

        verify(runtime, timeout(5000).times(2)).scheduleWriteOperation(eq("classic-group-join"), any(), any());
        // The successful conversion delete is smart-finalized after the re-join: a no-op for the
        // converted group, but it heals a raced push if the group was revived in between.
        verify(runtime, timeout(5000).times(1)).scheduleWriteOperation(
            eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
        InOrder inOrder = inOrder(runtime, plugin);
        inOrder.verify(runtime).scheduleWriteOperation(eq("classic-group-join"), any(), any());
        inOrder.verify(runtime).scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any());
        inOrder.verify(plugin).deleteTopology("g");
        inOrder.verify(runtime).scheduleWriteOperation(eq("classic-group-join"), any(), any());
        inOrder.verify(runtime).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testClassicJoinFailsWhenPluginDeleteFailsAndDoesNotConvert() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("g"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
        // The single classic-group-join detects the empty streams group and returns true; the
        // post-cleanup conversion never runs because the plugin delete fails.
        when(runtime.scheduleWriteOperation(eq("classic-group-join"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        JoinGroupResponseData resp = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP), classicJoinRequest("g"), BufferSupplier.NO_CACHING)
            .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), resp.errorCode());
        verify(runtime, times(1)).scheduleWriteOperation(eq("classic-group-join"), any(), any());
    }

    @Test
    public void testClassicJoinThrottlesPluginDeleteAfterFailure() throws Exception {
        // First join: the conversion delete fails -> REBALANCE_IN_PROGRESS and the throttle is
        // armed. The classic client retries the join immediately (RebalanceInProgressException
        // skips its retry back-off), so the retry must fail fast without re-invoking the plugin
        // or re-scheduling the mark write while the throttle window is in effect.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("g"))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));
        // Every join detects the empty streams group with a stored topology (cleanup needed).
        when(runtime.scheduleWriteOperation(eq("classic-group-join"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        JoinGroupResponseData first = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP), classicJoinRequest("g"), BufferSupplier.NO_CACHING)
            .get(5, TimeUnit.SECONDS);
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), first.errorCode());

        JoinGroupResponseData second = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP), classicJoinRequest("g"), BufferSupplier.NO_CACHING)
            .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), second.errorCode());
        verify(plugin, times(1)).deleteTopology("g");
        verify(runtime, times(1)).scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any());
    }

    @Test
    public void testClassicJoinSkipsPluginDeleteWhenBarrierWriteFindsGroupChanged() throws Exception {
        // The barrier write returns false when the group changed underneath the join (revived,
        // converted, or removed) between the cleanup check and the mark: no barrier exists, so the
        // plugin delete must be skipped — it could wipe a live group's topology — and the join
        // fails with a retriable error so the client retries against the latest state.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.FALSE));
        when(runtime.scheduleWriteOperation(eq("classic-group-join"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        JoinGroupResponseData resp = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP), classicJoinRequest("g"), BufferSupplier.NO_CACHING)
            .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), resp.errorCode());
        verify(plugin, never()).deleteTopology(any());
        verify(runtime, times(1)).scheduleWriteOperation(eq("classic-group-join"), any(), any());
    }

    @Test
    public void testClassicJoinSkipsPluginDeleteWhenGroupAlreadyClassic() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        // The single classic-group-join finds an already-classic group, completes the response and
        // returns false (no cleanup needed).
        when(runtime.scheduleWriteOperation(eq("classic-group-join"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Boolean.FALSE));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.joinGroup(requestContext(ApiKeys.JOIN_GROUP), classicJoinRequest("g"), BufferSupplier.NO_CACHING);

        verify(runtime, timeout(5000)).scheduleWriteOperation(eq("classic-group-join"), any(), any());
        verify(runtime, times(1)).scheduleWriteOperation(eq("classic-group-join"), any(), any());
        verify(runtime, never()).scheduleWriteOperation(eq("mark-topology-uncertain"), any(), any());
        verify(plugin, never()).deleteTopology(any());
    }

    // -----------------------------------------------------------------------
    // Regression tests for the UNCERTAIN-epoch gap closure (Task 7)
    // -----------------------------------------------------------------------

    @Test
    public void testHalfPushedGroupIsDeleteEligibleViaUncertain() {
        // Regression: plugin.setTopology succeeded but the epoch-advance write failed, leaving
        // storedEpoch at UNCERTAIN (-2). The cleanup cycle must still call plugin.deleteTopology
        // rather than skipping the group because storedEpoch != NONE.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("g")).thenReturn(CompletableFuture.completedFuture(null));
        Set<String> eligible = Set.of("g");
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(eligible)));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("g")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(plugin).deleteTopology("g");
    }

    @Test
    public void testCleanupCycleFinalizesAfterDeleteNotPlainClear() {
        // Regression: after a successful plugin.deleteTopology the cycle must write the smart-
        // finalize batch op ("finalize-stored-topology-epoch-after-delete-batch") so that any
        // racing push that advanced storedEpoch past UNCERTAIN is re-written to UNCERTAIN for
        // re-solicitation. A plain clear would leave a stranded real epoch.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        when(plugin.deleteTopology("g")).thenReturn(CompletableFuture.completedFuture(null));
        Set<String> eligible = Set.of("g");
        when(runtime.scheduleReadAllOperation(eq("list-streams-groups-needing-topology-cleanup"), any()))
            .thenReturn(List.of(CompletableFuture.completedFuture(eligible)));
        when(runtime.scheduleWriteOperation(eq("mark-topology-uncertain-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Set.of("g")));
        when(runtime.scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        service.runOneStreamsTopologyCleanupCycle();

        verify(runtime).scheduleWriteOperation(eq("finalize-stored-topology-epoch-after-delete-batch"), any(), any());
    }

    @Test
    public void testUncertainStoredEpochSolicitsOnHeartbeat() throws Exception {
        // Regression (loss direction): a group whose storedEpoch is UNCERTAIN (-2) — left by a
        // delete that wrote -2 but whose finalize write then failed — must still solicit a push on
        // the next heartbeat. UNCERTAIN != currentEpoch, so the back-off gate does not suppress
        // solicitation and TopologyDescriptionRequired is set.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);
        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);

        assertTrue(heartbeatTopologyDescriptionRequired(runtime, service, 3, StreamsGroup.STORED_TOPOLOGY_EPOCH_UNCERTAIN, -1),
            "storedEpoch == UNCERTAIN must still solicit a push on heartbeat");
    }

    @Test
    public void testDescribeAtUncertainEpochYieldsNotStored() throws Exception {
        // Regression (loss direction): a group at storedEpoch == UNCERTAIN (-2) must be described
        // as NOT_STORED because the broker cannot confirm the plugin holds a valid copy. The
        // describe gate uses `storedEpoch <= NONE` (i.e. <= -1), which also covers -2.
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = mock(StreamsGroupTopologyDescriptionPlugin.class);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = describedGroupWithTopology("g", 3);
        // The storedEpoch map carries UNCERTAIN (-2): the plugin has not confirmed it holds epoch 3.
        when(runtime.scheduleReadOperation(eq("streams-group-describe"), eq(GROUP_TP), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new StreamsGroupDescribeResult(List.of(describedGroup),
                    Map.of("g", StreamsGroup.STORED_TOPOLOGY_EPOCH_UNCERTAIN))));

        GroupCoordinatorService service = buildService(runtime, Optional.of(plugin), true);
        List<StreamsGroupDescribeResponseData.DescribedGroup> result = service.streamsGroupDescribe(
            requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("g"), true
        ).get(5, TimeUnit.SECONDS);

        assertEquals(StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED,
            result.get(0).topologyDescriptionStatus());
        assertNull(result.get(0).topologyDescription());
        verify(plugin, never()).getTopology(anyString(), anyInt());
    }
}
