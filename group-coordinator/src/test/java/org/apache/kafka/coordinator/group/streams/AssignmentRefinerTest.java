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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssignmentRefinerTest {

    private static TasksTuple active(final Map<String, Set<Integer>> activeTasks) {
        return new TasksTuple(activeTasks, Map.of(), Map.of());
    }

    @Test
    public void shouldPreserveActiveTaskCountOfUnchangedAssignment() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(2)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, targetAssignment));
    }

    @Test
    public void shouldPreserveActiveTaskCountWhenATaskIsHeldBackWithItsCurrentOwner() {
        // What a refinement step does to stage a migration: the target assignment moves 0_2 to memberB, the refined
        // assignment leaves it with memberA while memberB warms it up. The active tasks themselves are unchanged.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(2)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1, 2))),
            "memberB", new TasksTuple(Map.of(), Map.of(), Map.of("0", Set.of(2)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldPreserveActiveTaskCountWhenStandbysAreDeferred() {
        // A refinement step may defer a standby to a later step, which is not a defect this check is about.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", new TasksTuple(Map.of("0", Set.of(0)), Map.of("0", Set.of(1)), Map.of()),
            "memberB", new TasksTuple(Map.of("0", Set.of(1)), Map.of("0", Set.of(0)), Map.of())
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(1)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenATaskWasDropped() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(2)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of())
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenASubtopologyWasDroppedEntirely() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0), "1", Set.of(0)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0)))
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenATaskWasHandedToTwoMembers() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(1)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(1)))
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenATaskWasInvented() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1)))
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotDetectADropAndADuplicateCancellingEachOtherOut() {
        // The accepted blind spot of counting: 0_0 was dropped and 0_1 handed to both members, so the count still
        // matches. It takes two coordinated mistakes in one derivation, and the exhaustive invariant is covered by the
        // refiner's own tests. If this check is ever strengthened, this test is what should fail.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(1)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(1))),
            "memberB", active(Map.of("0", Set.of(1)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldPreserveActiveTaskCountWhenTheTargetAssignmentItselfDuplicatesATask() {
        // A target assignment that places an active task twice is the assignor's defect, not the refinement's, so a
        // refinement that keeps it as-is is not blamed for it.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(0)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, targetAssignment));
    }
}
