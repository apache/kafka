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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MemberMetadataAndStateImplTest {

    private static MemberMetadataAndStateImpl memberWith(
        Map<String, Set<Integer>> tasks,
        Map<String, Map<Integer, Long>> offsets
    ) {
        return new MemberMetadataAndStateImpl(
            Optional.of("test-instance"),
            Optional.of("test-rack"),
            "test-process",
            Map.of(),
            tasks,
            tasks,
            tasks,
            offsets,
            offsets
        );
    }

    @Test
    void testTaskSetsAreDeeplyUnmodifiable() {
        MemberMetadataAndStateImpl member = memberWith(
            new HashMap<>(Map.of("subtopology-1", new HashSet<>(Set.of(0, 1)))),
            new HashMap<>()
        );

        assertThrows(UnsupportedOperationException.class, () -> member.activeTasks().put("subtopology-2", Set.of(0)));
        assertThrows(UnsupportedOperationException.class, () -> member.activeTasks().get("subtopology-1").add(2));
        assertThrows(UnsupportedOperationException.class, () -> member.standbyTasks().put("subtopology-2", Set.of(0)));
        assertThrows(UnsupportedOperationException.class, () -> member.standbyTasks().get("subtopology-1").add(2));
        assertThrows(UnsupportedOperationException.class, () -> member.warmupTasks().put("subtopology-2", Set.of(0)));
        assertThrows(UnsupportedOperationException.class, () -> member.warmupTasks().get("subtopology-1").clear());
    }

    @Test
    void testTaskOffsetsAreDeeplyUnmodifiable() {
        MemberMetadataAndStateImpl member = memberWith(
            new HashMap<>(),
            new HashMap<>(Map.of("subtopology-1", new HashMap<>(Map.of(0, 100L))))
        );

        assertThrows(UnsupportedOperationException.class, () -> member.taskOffsets().put("subtopology-2", Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> member.taskOffsets().get("subtopology-1").put(0, 200L));
        assertThrows(UnsupportedOperationException.class, () -> member.taskEndOffsets().put("subtopology-2", Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> member.taskEndOffsets().get("subtopology-1").put(1, 200L));
    }
}
