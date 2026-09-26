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

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Profile;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Scenario;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link StickyTaskAssignor} through the {@link TaskAssignorTestbed} and adds the checks specific to it:
 * no member exceeds the active task quota, every stateful task gets as many standbys as the processes allow, and
 * rack-aware tags do not change the active assignment.
 */
public class StickyTaskAssignorFuzzTest {

    private final StickyTaskAssignor assignor = new StickyTaskAssignor();
    private final TaskAssignorTestbed testbed = new TaskAssignorTestbed(
        assignor,
        List.of(this::verifyActiveQuota, this::verifyStandbyCount, this::verifyTagsDoNotChangeActiveAssignment)
    );

    @Test
    public void shouldConvergeToValidAssignmentsForSmallGroupsUnderRandomEvents() {
        testbed.run(Profile.SMALL);
    }

    @Test
    public void shouldConvergeToValidAssignmentsForLargeGroupsUnderRandomEvents() {
        testbed.run(Profile.LARGE);
    }

    /** The fill phase always picks the least loaded process, so no member ends above {@code ceil(tasks / members)}. */
    private void verifyActiveQuota(final Scenario scenario, final GroupAssignment result, final boolean last) {
        final int members = scenario.memberIds().size();
        final int quota = (scenario.topology.tasks().size() + members - 1) / members;
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            final int active = TaskAssignorTestbed.toTaskIds(entry.getValue().activeTasks()).size();
            assertTrue(active <= quota, entry.getKey() + " holds " + active + " active tasks, above the quota of " + quota);
        }
    }

    /** Standbys are placed on the least loaded process without the task, so they only fall short when processes run out. */
    private void verifyStandbyCount(final Scenario scenario, final GroupAssignment result, final boolean last) {
        final int expectedStandbys = Math.min(scenario.numStandbyReplicas, scenario.processes.size() - 1);
        final Map<TaskId, Integer> standbyCounts = new HashMap<>();
        for (final MemberAssignment assignment : result.members().values()) {
            for (final TaskId task : TaskAssignorTestbed.toTaskIds(assignment.standbyTasks())) {
                standbyCounts.merge(task, 1, Integer::sum);
            }
        }
        for (final TaskId task : scenario.topology.statefulTasks()) {
            final int actual = standbyCounts.getOrDefault(task, 0);
            assertEquals(expectedStandbys, actual, "stateful task " + task + " must have " + expectedStandbys + " standbys but has " + actual);
        }
    }

    /**
     * Rack awareness only places standbys, so the same group without any rack-aware tags must produce the same
     * active assignment. Catches a rack-aware change leaking into the active steps or the shared quota bookkeeping.
     * Costs a second assignment, so it runs only on the last result of each rebalance.
     */
    private void verifyTagsDoNotChangeActiveAssignment(final Scenario scenario, final GroupAssignment result, final boolean last) {
        if (!last || scenario.tagKeys.isEmpty()) {
            return;
        }
        final GroupAssignment withoutTags = assignor.assign(scenario.groupSpec(List.of()), scenario.topology);
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            assertEquals(
                withoutTags.members().get(entry.getKey()).activeTasks(),
                entry.getValue().activeTasks(),
                "active tasks of " + entry.getKey() + " differ between the assignment with and without rack-aware tags"
            );
        }
    }
}
