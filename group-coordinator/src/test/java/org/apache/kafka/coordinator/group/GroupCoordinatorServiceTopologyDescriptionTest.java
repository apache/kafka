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
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupTopologyDescriptionBackoff;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
        MockTimer timer = new MockTimer();
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
            new StreamsGroupTopologyDescriptionBackoff(time)
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
        assertEquals("plugin offline", result.errorMessage());
        verify(runtime, never()).scheduleWriteOperation(
            eq("delete-groups"), any(), any());
    }

    @Test
    public void testDeleteGroupsPluginFailureDowngradesErrorOnV2() throws Exception {
        // KIP-1331: GROUP_DELETION_FAILED and the per-group ErrorMessage field arrive in
        // DeleteGroups v3. For older clients the broker downgrades the error code to
        // UNKNOWN_SERVER_ERROR and drops the message so the v2 wire format is preserved.
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
                requestContext(ApiKeys.DELETE_GROUPS, (short) 2),
                List.of("foo"),
                BufferSupplier.NO_CACHING
            ).get(5, TimeUnit.SECONDS);

        DeleteGroupsResponseData.DeletableGroupResult result = results.find("foo");
        assertNotNull(result);
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), result.errorCode());
        assertNull(result.errorMessage());
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
        assertEquals("rejected", badResult.errorMessage());
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
}
