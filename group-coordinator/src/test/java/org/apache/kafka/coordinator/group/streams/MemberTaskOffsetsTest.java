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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MemberTaskOffsetsTest {

    private static StreamsGroupHeartbeatRequestData.TaskOffset taskOffset(final String subtopologyId,
                                                                          final int partition,
                                                                          final long offset) {
        return new StreamsGroupHeartbeatRequestData.TaskOffset()
            .setSubtopologyId(subtopologyId)
            .setPartition(partition)
            .setOffset(offset);
    }

    @Test
    public void shouldConvertHeartbeatRequestTaskOffsets() {
        MemberTaskOffsets result = MemberTaskOffsets.EMPTY.update(
            List.of(taskOffset("sub-1", 0, 10L), taskOffset("sub-1", 1, 20L)),
            List.of(taskOffset("sub-1", 0, 15L), taskOffset("sub-1", 1, 25L))
        );

        assertEquals(
            Map.of(new TaskId("sub-1", 0), 10L, new TaskId("sub-1", 1), 20L),
            result.taskOffsets()
        );
        assertEquals(
            Map.of(new TaskId("sub-1", 0), 15L, new TaskId("sub-1", 1), 25L),
            result.taskEndOffsets()
        );
    }

    @Test
    public void shouldRejectDuplicateTaskEntries() {
        // The protocol does not enforce uniqueness of (subtopologyId, partition) within a reported list. A duplicate
        // entry is a malformed request and must be rejected with a clear client error rather than silently resolved.
        InvalidRequestException e = assertThrows(InvalidRequestException.class, () -> MemberTaskOffsets.EMPTY.update(
            List.of(taskOffset("sub-1", 0, 10L), taskOffset("sub-1", 0, 42L)),
            null
        ));
        assertEquals("Task offsets contain a duplicate entry for subtopology sub-1 and partition 0.", e.getMessage());

        // The check also applies to the end-offsets list.
        assertThrows(InvalidRequestException.class, () -> MemberTaskOffsets.EMPTY.update(
            null,
            List.of(taskOffset("sub-1", 0, 15L), taskOffset("sub-1", 0, 55L))
        ));
    }

    @Test
    public void shouldRetainBothMapsWhenBothListsAreNull() {
        MemberTaskOffsets previous = new MemberTaskOffsets(
            Map.of(new TaskId("sub-1", 0), 10L),
            Map.of(new TaskId("sub-1", 0), 15L)
        );

        assertEquals(previous, previous.update(null, null));
        assertEquals(MemberTaskOffsets.EMPTY, MemberTaskOffsets.EMPTY.update(null, null));
    }

    @Test
    public void shouldUpdateTaskOffsetsAndRetainTaskEndOffsetsWhenEndOffsetsNull() {
        MemberTaskOffsets previous = new MemberTaskOffsets(
            Map.of(new TaskId("sub-1", 0), 10L),
            Map.of(new TaskId("sub-1", 0), 15L)
        );

        MemberTaskOffsets result = previous.update(List.of(taskOffset("sub-1", 0, 12L)), null);

        assertEquals(Map.of(new TaskId("sub-1", 0), 12L), result.taskOffsets());
        // The end-offsets were not reported, so the previously reported values are retained.
        assertEquals(Map.of(new TaskId("sub-1", 0), 15L), result.taskEndOffsets());
    }

    @Test
    public void shouldUpdateTaskEndOffsetsAndRetainTaskOffsetsWhenOffsetsNull() {
        MemberTaskOffsets previous = new MemberTaskOffsets(
            Map.of(new TaskId("sub-1", 0), 10L),
            Map.of(new TaskId("sub-1", 0), 15L)
        );

        MemberTaskOffsets result = previous.update(null, List.of(taskOffset("sub-1", 0, 18L)));

        // The offsets were not reported, so the previously reported values are retained.
        assertEquals(Map.of(new TaskId("sub-1", 0), 10L), result.taskOffsets());
        assertEquals(Map.of(new TaskId("sub-1", 0), 18L), result.taskEndOffsets());
    }
}
