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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FallbackPriorTaskAssignorTest {

    private final Map<UUID, ClientState> clients = new TreeMap<>();

    @Test
    public void shouldViolateBalanceToPreserveActiveTaskStickiness() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClient(UUID_2, 1);

        final List<TaskId> taskIds = asList(TASK_0_0, TASK_0_1, TASK_0_2);
        Collections.shuffle(taskIds);
        final boolean probingRebalanceNeeded = new FallbackPriorTaskAssignor().assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(taskIds),
            new AssignorConfiguration.AssignmentConfigs(0L, 1, 0, 60_000L)
        );
        assertThat(probingRebalanceNeeded, is(true));

        assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(c2.activeTasks(), empty());
    }

    private ClientState createClient(final UUID processId, final int capacity) {
        return createClientWithPreviousActiveTasks(processId, capacity);
    }

    private ClientState createClientWithPreviousActiveTasks(final UUID processId, final int capacity, final TaskId... taskIds) {
        final ClientState clientState = new ClientState(capacity);
        clientState.addPreviousActiveTasks(mkSet(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }
}
