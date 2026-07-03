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

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.coordinator.group.streams.assignor.TaskId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The latest per-task cumulative changelog offsets and end-offsets that a member reported in its heartbeat.
 * <p>
 * These values are transient telemetry that the assignor uses to estimate task lag (for warm-up promotion).
 * Per KIP-1071 they are <b>not</b> persisted to the {@code __consumer_offsets} topic ("not persisted, as they are
 * constantly changing"); they are held in memory on the group coordinator and re-reported by the member on the
 * task-offset interval.
 *
 * @param taskOffsets    Cumulative changelog offsets per task.
 * @param taskEndOffsets Cumulative changelog end-offsets per task.
 */
public record MemberTaskOffsets(Map<TaskId, Long> taskOffsets, Map<TaskId, Long> taskEndOffsets) {

    public static final MemberTaskOffsets EMPTY = new MemberTaskOffsets(Map.of(), Map.of());

    /**
     * Returns a copy of these offsets updated with the values reported in a heartbeat. Task offsets and task
     * end-offsets are reported independently: a {@code null} list means "unchanged since the last heartbeat", so the
     * corresponding map is retained from this instance even when the other one is updated.
     *
     * @param reportedTaskOffsets    The reported task offsets, or {@code null} if unchanged.
     * @param reportedTaskEndOffsets The reported task end-offsets, or {@code null} if unchanged.
     */
    public MemberTaskOffsets update(
        final List<StreamsGroupHeartbeatRequestData.TaskOffset> reportedTaskOffsets,
        final List<StreamsGroupHeartbeatRequestData.TaskOffset> reportedTaskEndOffsets
    ) {
        return new MemberTaskOffsets(
            reportedTaskOffsets == null ? taskOffsets : toTaskIdMap(reportedTaskOffsets),
            reportedTaskEndOffsets == null ? taskEndOffsets : toTaskIdMap(reportedTaskEndOffsets)
        );
    }

    private static Map<TaskId, Long> toTaskIdMap(final List<StreamsGroupHeartbeatRequestData.TaskOffset> taskOffsets) {
        final Map<TaskId, Long> result = new HashMap<>(taskOffsets.size());
        for (final StreamsGroupHeartbeatRequestData.TaskOffset taskOffset : taskOffsets) {
            final TaskId taskId = new TaskId(taskOffset.subtopologyId(), taskOffset.partition());
            // The reported values come straight from the client heartbeat and the protocol does not enforce uniqueness
            // of (subtopologyId, partition). Reject a duplicate with a clear client error rather than silently picking
            // one of the values.
            if (result.putIfAbsent(taskId, taskOffset.offset()) != null) {
                throw new InvalidRequestException("Task offsets contain a duplicate entry for subtopology "
                    + taskOffset.subtopologyId() + " and partition " + taskOffset.partition() + ".");
            }
        }
        return result;
    }
}
