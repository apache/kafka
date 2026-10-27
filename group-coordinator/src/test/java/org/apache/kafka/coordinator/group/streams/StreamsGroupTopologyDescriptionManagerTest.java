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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the manager's own push / cleanup-cycle / delete-groups orchestration
 * (see {@link StreamsGroupTopologyDescriptionManager#pushTopology},
 * {@link StreamsGroupTopologyDescriptionManager#runCleanupCycle} and
 * {@link StreamsGroupTopologyDescriptionManager#deleteTopologiesForDeleteGroups}) against a
 * controllable {@link TopologyDescriptionRuntime}, independent of any particular runtime
 * implementation.
 */
public class StreamsGroupTopologyDescriptionManagerTest {

    /**
     * Controllable {@link TopologyDescriptionRuntime}. Each method returns whatever future the
     * test pre-set on the matching field, defaulting to an immediately-successful no-op result,
     * and the batch methods record the group ids they were called with so tests can assert on
     * them.
     */
    private static final class MockTopologyDescriptionRuntime implements TopologyDescriptionRuntime {
        CompletableFuture<Void> validateSetTopologyResult = CompletableFuture.completedFuture(null);
        CompletableFuture<Boolean> markTopologyUncertainResult = CompletableFuture.completedFuture(true);
        CompletableFuture<Void> setStoredTopologyEpochResult = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> setFailedTopologyEpochResult = CompletableFuture.completedFuture(null);
        List<CompletableFuture<Set<String>>> listGroupsNeedingCleanupBatchesResult = List.of();
        CompletableFuture<Set<String>> markTopologyUncertainBatchResult;
        CompletableFuture<Void> finalizeAfterDeleteBatchResult = CompletableFuture.completedFuture(null);
        CompletableFuture<Set<String>> groupsWithStoredTopologyDescriptionResult;

        boolean setStoredTopologyEpochCalled;
        boolean setFailedTopologyEpochCalled;
        boolean listGroupsNeedingCleanupBatchesCalled;
        Set<String> markTopologyUncertainBatchGroupIds;
        Set<String> finalizeAfterDeleteBatchGroupIds;

        @Override
        public CompletableFuture<Void> validateSetTopology(String groupId, String memberId, int topologyEpoch) {
            return validateSetTopologyResult;
        }

        @Override
        public CompletableFuture<Boolean> markTopologyUncertain(String groupId, boolean markWhenNone) {
            return markTopologyUncertainResult;
        }

        @Override
        public CompletableFuture<Void> setStoredTopologyEpoch(String groupId, int topologyEpoch) {
            setStoredTopologyEpochCalled = true;
            return setStoredTopologyEpochResult;
        }

        @Override
        public CompletableFuture<Void> setFailedTopologyEpoch(String groupId, int topologyEpoch) {
            setFailedTopologyEpochCalled = true;
            return setFailedTopologyEpochResult;
        }

        @Override
        public List<CompletableFuture<Set<String>>> listGroupsNeedingCleanupBatches() {
            listGroupsNeedingCleanupBatchesCalled = true;
            return listGroupsNeedingCleanupBatchesResult;
        }

        @Override
        public CompletableFuture<Set<String>> markTopologyUncertainBatch(Set<String> groupIds) {
            markTopologyUncertainBatchGroupIds = groupIds;
            return markTopologyUncertainBatchResult != null
                ? markTopologyUncertainBatchResult
                : CompletableFuture.completedFuture(groupIds);
        }

        @Override
        public CompletableFuture<Void> finalizeAfterDeleteBatch(Set<String> groupIds) {
            finalizeAfterDeleteBatchGroupIds = groupIds;
            return finalizeAfterDeleteBatchResult;
        }

        @Override
        public CompletableFuture<Set<String>> groupsWithStoredTopologyDescription(List<String> groupIds) {
            return groupsWithStoredTopologyDescriptionResult != null
                ? groupsWithStoredTopologyDescriptionResult
                : CompletableFuture.completedFuture(Set.copyOf(groupIds));
        }
    }

    /** Plugin whose {@code setTopology} calls all return the given result. */
    private static final class MockSetTopologyPlugin implements StreamsGroupTopologyDescriptionPlugin {
        private final CompletableFuture<Void> result;

        private MockSetTopologyPlugin(CompletableFuture<Void> result) {
            this.result = result;
        }

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public CompletableFuture<Void> setTopology(
            String groupId, int topologyEpoch, StreamsGroupTopologyDescription description
        ) {
            return result;
        }

        @Override
        public CompletableFuture<Void> deleteTopology(String groupId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() { }
    }

    /** Plugin whose {@code deleteTopology} fails for exactly the given group ids, and succeeds for every other. */
    private static final class MockDeleteTopologyPlugin implements StreamsGroupTopologyDescriptionPlugin {
        private final Set<String> failingGroupIds;

        private MockDeleteTopologyPlugin(Set<String> failingGroupIds) {
            this.failingGroupIds = failingGroupIds;
        }

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public CompletableFuture<Void> setTopology(
            String groupId, int topologyEpoch, StreamsGroupTopologyDescription description
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> deleteTopology(String groupId) {
            return failingGroupIds.contains(groupId)
                ? CompletableFuture.failedFuture(new RuntimeException("plugin delete failed"))
                : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() { }
    }

    /** Plugin that fails the test if {@code setTopology} or {@code deleteTopology} is invoked at all. */
    private static final class MockUnreachablePlugin implements StreamsGroupTopologyDescriptionPlugin {
        int closeCount;

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public CompletableFuture<Void> setTopology(
            String groupId, int topologyEpoch, StreamsGroupTopologyDescription description
        ) {
            throw new AssertionError("plugin.setTopology should not be called");
        }

        @Override
        public CompletableFuture<Void> deleteTopology(String groupId) {
            throw new AssertionError("plugin.deleteTopology should not be called");
        }

        @Override
        public CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    @Test
    public void testPushTopologySuccessSetsStoredEpoch() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockSetTopologyPlugin(CompletableFuture.completedFuture(null))),
            new MockTime(), new GroupCoordinatorMetrics(), runtime);

        StreamsGroupTopologyDescriptionUpdateResponseData response = manager.pushTopology(
            "group", "member", 5, new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
        ).join();

        assertEquals(Errors.NONE.code(), response.errorCode());
        assertTrue(runtime.setStoredTopologyEpochCalled);
        assertFalse(runtime.setFailedTopologyEpochCalled);
    }

    @Test
    public void testPushTopologyPermanentPluginFailureSetsFailedEpoch() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = new MockSetTopologyPlugin(
            CompletableFuture.failedFuture(new StreamsTopologyDescriptionPermanentFailureException("rejected: too large")));
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(plugin), new MockTime(), new GroupCoordinatorMetrics(), runtime);

        StreamsGroupTopologyDescriptionUpdateResponseData response = manager.pushTopology(
            "group", "member", 5, new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
        ).join();

        assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), response.errorCode());
        assertEquals("rejected: too large", response.errorMessage());
        assertTrue(runtime.setFailedTopologyEpochCalled);
        assertFalse(runtime.setStoredTopologyEpochCalled);
    }

    @Test
    public void testPushTopologyTransientPluginFailureArmsBackoffWithoutEpochWrite() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        StreamsGroupTopologyDescriptionPlugin plugin = new MockSetTopologyPlugin(
            CompletableFuture.failedFuture(new RuntimeException("transient boom")));
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(plugin), new MockTime(), new GroupCoordinatorMetrics(), runtime);

        StreamsGroupTopologyDescriptionUpdateResponseData response = manager.pushTopology(
            "group", "member", 5, new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
        ).join();

        assertEquals(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(), response.errorCode());
        assertFalse(runtime.setStoredTopologyEpochCalled);
        assertFalse(runtime.setFailedTopologyEpochCalled);
        assertTrue(manager.backoff().isActive("group", 5));
    }

    @Test
    public void testPushTopologyFailsWithGroupIdNotFoundWhenMarkUncertainReturnsFalse() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        runtime.markTopologyUncertainResult = CompletableFuture.completedFuture(false);
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockUnreachablePlugin()), new MockTime(), new GroupCoordinatorMetrics(), runtime);

        CompletableFuture<StreamsGroupTopologyDescriptionUpdateResponseData> result = manager.pushTopology(
            "group", "member", 5, new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());

        CompletionException thrown = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(GroupIdNotFoundException.class, thrown.getCause());
        assertFalse(runtime.setStoredTopologyEpochCalled);
    }

    @Test
    public void testPushTopologyValidateFailureShortCircuitsBeforePluginCall() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        runtime.validateSetTopologyResult = CompletableFuture.failedFuture(new GroupIdNotFoundException("no such group"));
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockUnreachablePlugin()), new MockTime(), new GroupCoordinatorMetrics(), runtime);

        CompletableFuture<StreamsGroupTopologyDescriptionUpdateResponseData> result = manager.pushTopology(
            "group", "member", 5, new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription());

        CompletionException thrown = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(GroupIdNotFoundException.class, thrown.getCause());
    }

    @Test
    public void testRunCleanupCycleNoOpWhenNoPluginConfigured() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.empty(), new MockTime(), new GroupCoordinatorMetrics(), runtime);

        assertTrue(manager.runCleanupCycle().isDone());
        assertFalse(runtime.listGroupsNeedingCleanupBatchesCalled);
    }

    @Test
    public void testRunCleanupCycleMarksDeletesAndFinalizesEligibleGroup() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        runtime.listGroupsNeedingCleanupBatchesResult = List.of(CompletableFuture.completedFuture(Set.of("g1")));
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockDeleteTopologyPlugin(Set.of())),
            new MockTime(), new GroupCoordinatorMetrics(), runtime);
        // Only startCleanupCycle's running flag matters here; the timer never advances so the
        // task it schedules never fires, and this test drives runCleanupCycle directly instead.
        manager.startCleanupCycle(new MockTimer(), 1000L, manager::runCleanupCycle);

        manager.runCleanupCycle().join();

        assertEquals(Set.of("g1"), runtime.markTopologyUncertainBatchGroupIds);
        assertEquals(Set.of("g1"), runtime.finalizeAfterDeleteBatchGroupIds);
    }

    @Test
    public void testDeleteTopologiesForDeleteGroupsSkipsMarkWhenNoGroupsHaveStoredTopology() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        runtime.groupsWithStoredTopologyDescriptionResult = CompletableFuture.completedFuture(Set.of());
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockUnreachablePlugin()), new MockTime(), new GroupCoordinatorMetrics(), runtime);

        Map<String, ApiError> result = manager.deleteTopologiesForDeleteGroups(List.of("g1")).join();

        assertEquals(Map.of(), result);
        assertNull(runtime.markTopologyUncertainBatchGroupIds);
    }

    @Test
    public void testDeleteTopologiesForDeleteGroupsFinalizesOnlySucceededDeletes() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        runtime.groupsWithStoredTopologyDescriptionResult = CompletableFuture.completedFuture(Set.of("g1", "g2"));
        runtime.markTopologyUncertainBatchResult = CompletableFuture.completedFuture(Set.of("g1", "g2"));
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockDeleteTopologyPlugin(Set.of("g2"))),
            new MockTime(), new GroupCoordinatorMetrics(), runtime);

        Map<String, ApiError> result = manager.deleteTopologiesForDeleteGroups(List.of("g1", "g2")).join();

        assertTrue(result.containsKey("g2"));
        assertFalse(result.containsKey("g1"));
        assertEquals(Set.of("g1"), runtime.finalizeAfterDeleteBatchGroupIds);
    }

    @Test
    public void testDecorateHeartbeatResultNoOpWhenNoPluginConfigured() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.empty(), new MockTime(), new GroupCoordinatorMetrics(), runtime);
        CompletableFuture<StreamsGroupHeartbeatResult> heartbeat = CompletableFuture.completedFuture(
            heartbeatResultAtEpoch(5));

        assertSame(heartbeat, manager.decorateHeartbeatResult(heartbeat, "group", 1, 0));
    }

    @Test
    public void testDecorateHeartbeatResultSetsTopologyDescriptionRequired() {
        MockTopologyDescriptionRuntime runtime = new MockTopologyDescriptionRuntime();
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockUnreachablePlugin()), new MockTime(), new GroupCoordinatorMetrics(), runtime);
        CompletableFuture<StreamsGroupHeartbeatResult> heartbeat = CompletableFuture.completedFuture(
            heartbeatResultAtEpoch(5));

        StreamsGroupHeartbeatResult decorated = manager.decorateHeartbeatResult(heartbeat, "group", 1, 0).join();

        assertTrue(decorated.data().topologyDescriptionRequired());
    }

    private static StreamsGroupHeartbeatResult heartbeatResultAtEpoch(int currentTopologyEpoch) {
        return new StreamsGroupHeartbeatResult(
            new StreamsGroupHeartbeatResponseData(),
            Map.<String, CreatableTopic>of(),
            currentTopologyEpoch,
            -1,
            -1);
    }

    @Test
    public void testCloseInvokesPluginCloseOnlyOnce() throws Exception {
        MockUnreachablePlugin plugin = new MockUnreachablePlugin();
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(plugin), new MockTime(), new GroupCoordinatorMetrics(), new MockTopologyDescriptionRuntime());

        manager.close();
        manager.close();

        assertEquals(1, plugin.closeCount);
    }

    @Test
    public void testStopCleanupCycleNoOpWhenNotRunning() {
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockUnreachablePlugin()), new MockTime(), new GroupCoordinatorMetrics(),
            new MockTopologyDescriptionRuntime());

        manager.stopCleanupCycle();

        assertFalse(manager.isRunning());
        assertNull(manager.scheduledCleanupTask());
    }

    @Test
    public void testStopCleanupCycleCancelsScheduledTask() {
        StreamsGroupTopologyDescriptionManager manager = new StreamsGroupTopologyDescriptionManager(
            new LogContext(), Optional.of(new MockUnreachablePlugin()), new MockTime(), new GroupCoordinatorMetrics(),
            new MockTopologyDescriptionRuntime());
        manager.startCleanupCycle(new MockTimer(), 1000L, manager::runCleanupCycle);
        assertTrue(manager.isRunning());
        assertFalse(manager.scheduledCleanupTask().isCancelled());

        manager.stopCleanupCycle();

        assertFalse(manager.isRunning());
        assertTrue(manager.scheduledCleanupTask().isCancelled());
    }
}
