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
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.streams.TopologyDescriptionRuntime;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Implementation of {@link TopologyDescriptionRuntime} backed by
 * {@code CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord>} against
 * {@code __consumer_offsets}. The target partition for each call is resolved from the group id
 * via the injected {@code partitioner}, matching {@code GroupCoordinatorService#topicPartitionFor}.
 *
 * <p>The batch methods and {@link #groupsWithStoredTopologyDescription} require every group id in
 * their input to resolve to the same partition; callers are responsible for that grouping, as
 * {@link #listGroupsNeedingCleanupBatches} and {@code GroupCoordinatorService#deleteGroups}
 * already provide it.
 */
class CoordinatorRuntimeTopologyDescriptionRuntime implements TopologyDescriptionRuntime {

    private final CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;
    private final Function<String, TopicPartition> partitioner;

    CoordinatorRuntimeTopologyDescriptionRuntime(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        Function<String, TopicPartition> partitioner
    ) {
        this.runtime = runtime;
        this.partitioner = partitioner;
    }

    @Override
    public CompletableFuture<Void> validateSetTopology(String groupId, String memberId, int topologyEpoch) {
        return runtime.scheduleReadOperation(
            "streams-group-topology-description-validate",
            partitioner.apply(groupId),
            (coordinator, lastCommittedOffset) -> {
                coordinator.validateStreamsGroupTopologyDescriptionUpdate(
                    groupId, memberId, topologyEpoch, lastCommittedOffset);
                return null;
            });
    }

    @Override
    public CompletableFuture<Boolean> markTopologyUncertain(String groupId, boolean markWhenNone) {
        return runtime.scheduleWriteOperation(
            "mark-topology-uncertain",
            partitioner.apply(groupId),
            coordinator -> coordinator.markStoredDescriptionTopologyEpochUncertain(groupId, markWhenNone));
    }

    @Override
    public CompletableFuture<Void> setStoredTopologyEpoch(String groupId, int topologyEpoch) {
        return runtime.scheduleWriteOperation(
            "streams-group-set-stored-topology-epoch",
            partitioner.apply(groupId),
            coordinator -> coordinator.setStoredDescriptionTopologyEpoch(groupId, topologyEpoch));
    }

    @Override
    public CompletableFuture<Void> setFailedTopologyEpoch(String groupId, int topologyEpoch) {
        return runtime.scheduleWriteOperation(
            "streams-group-set-failed-topology-epoch",
            partitioner.apply(groupId),
            coordinator -> coordinator.setFailedDescriptionTopologyEpoch(groupId, topologyEpoch));
    }

    @Override
    public List<CompletableFuture<Set<String>>> listGroupsNeedingCleanupBatches() {
        return runtime.scheduleReadAllOperation(
            "list-streams-groups-needing-topology-cleanup",
            GroupCoordinatorShard::listStreamsGroupsNeedingTopologyCleanup);
    }

    @Override
    public CompletableFuture<Set<String>> markTopologyUncertainBatch(Set<String> groupIds) {
        if (groupIds.isEmpty()) return CompletableFuture.completedFuture(Set.of());
        return runtime.scheduleWriteOperation(
            "mark-topology-uncertain-batch",
            partitioner.apply(groupIds.iterator().next()),
            coordinator -> coordinator.markStoredDescriptionTopologyEpochUncertainBatch(groupIds));
    }

    @Override
    public CompletableFuture<Void> finalizeAfterDeleteBatch(Set<String> groupIds) {
        if (groupIds.isEmpty()) return CompletableFuture.completedFuture(null);
        return runtime.<Void>scheduleWriteOperation(
            "finalize-stored-topology-epoch-after-delete-batch",
            partitioner.apply(groupIds.iterator().next()),
            coordinator -> coordinator.finalizeStoredDescriptionTopologyEpochAfterDeleteBatch(groupIds));
    }

    @Override
    public CompletableFuture<Set<String>> groupsWithStoredTopologyDescription(List<String> groupIds) {
        if (groupIds.isEmpty()) return CompletableFuture.completedFuture(Set.of());
        return runtime.scheduleReadOperation(
            "streams-group-topology-pre-delete",
            partitioner.apply(groupIds.get(0)),
            (coordinator, lastCommittedOffset) ->
                coordinator.streamsGroupsWithStoredTopologyDescription(groupIds, lastCommittedOffset));
    }
}
