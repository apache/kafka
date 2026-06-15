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
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.apache.kafka.coordinator.group.GroupConfigManagerTest.createConfigManager;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the heartbeat post-processing on {@link GroupCoordinatorService} that sets
 * {@code TopologyDescriptionRequired} on the response when a topology description plugin
 * is configured and the stored/failed epochs lag the current topology epoch.
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
