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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The read and write operations {@link StreamsGroupTopologyDescriptionManager} requires against
 * the streams group's stored state: validating a push, marking an epoch uncertain before
 * invoking the plugin, recording a stored or failed epoch, listing groups eligible for cleanup,
 * and determining which groups currently have a stored topology. Each method takes only group
 * ids and epochs, with no dependency on how a given host routes or partitions that state, so the
 * manager's orchestration logic can be implemented once and reused across hosts.
 *
 * <p>{@code CoordinatorRuntimeTopologyDescriptionRuntime} implements this interface backed by
 * {@code CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord>} against
 * {@code __consumer_offsets}.
 */
public interface TopologyDescriptionRuntime {

    /**
     * Validates a pushed topology description against the group's current state, including
     * member and epoch fencing. Completes exceptionally if the push must be rejected before the
     * plugin is invoked.
     */
    CompletableFuture<Void> validateSetTopology(String groupId, String memberId, int topologyEpoch);

    /**
     * Writes {@code StoredDescriptionTopologyEpoch = UNCERTAIN} for one group before the plugin
     * is invoked, so that a failure between the plugin call and the outcome write leaves the
     * epoch state marked as indeterminate rather than stale. Returns {@code false} if the group
     * is no longer eligible for the mark; callers must not invoke the plugin in that case.
     *
     * @param markWhenNone whether to write the barrier when the group currently has no stored
     *                      epoch at all, as opposed to only when a real epoch is already set.
     */
    CompletableFuture<Boolean> markTopologyUncertain(String groupId, boolean markWhenNone);

    /** Records a successful push by setting {@code StoredDescriptionTopologyEpoch = topologyEpoch}. */
    CompletableFuture<Void> setStoredTopologyEpoch(String groupId, int topologyEpoch);

    /** Records a permanently rejected push by setting {@code FailedDescriptionTopologyEpoch = topologyEpoch}. */
    CompletableFuture<Void> setFailedTopologyEpoch(String groupId, int topologyEpoch);

    /**
     * Scans for streams groups eligible for plugin-side cleanup (empty, with expired offsets,
     * and a stored epoch that is present or {@code UNCERTAIN}). Returns one future per locality
     * the scan is split across; group ids within a single future's result share a locality and
     * may be passed together to {@link #markTopologyUncertainBatch} and
     * {@link #finalizeAfterDeleteBatch}.
     */
    List<CompletableFuture<Set<String>>> listGroupsNeedingCleanupBatches();

    /**
     * Batched form of {@link #markTopologyUncertain}. {@code groupIds} must share one locality,
     * as returned by {@link #listGroupsNeedingCleanupBatches}. Returns the subset still eligible
     * after the write re-checks live state.
     */
    CompletableFuture<Set<String>> markTopologyUncertainBatch(Set<String> groupIds);

    /**
     * Finalizes a batch after the cleanup cycle's {@code plugin.deleteTopology} call: clears the
     * stored epoch to {@code NONE} if it is still {@code UNCERTAIN}, or restores
     * {@code UNCERTAIN} if a concurrent push advanced it. {@code groupIds} must share one
     * locality, matching the batch that was marked.
     */
    CompletableFuture<Void> finalizeAfterDeleteBatch(Set<String> groupIds);

    /**
     * Returns the subset of the given group ids, from an explicit {@code DeleteGroups} request,
     * that currently have a stored topology description.
     */
    CompletableFuture<Set<String>> groupsWithStoredTopologyDescription(List<String> groupIds);
}
