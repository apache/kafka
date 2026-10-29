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
import org.apache.kafka.streams.processor.assignment.ProcessId;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConstrainedPrioritySetTest {
    private static final TaskId DUMMY_TASK = new TaskId(0, 0);

    private final BiFunction<ProcessId, TaskId, Boolean> alwaysTrue = (client, task) -> true;
    private final BiFunction<ProcessId, TaskId, Boolean> alwaysFalse = (client, task) -> false;

    @Test
    public void shouldReturnOnlyClient() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(alwaysTrue, client -> 1.0);
        queue.offerAll(Set.of(PID_1));

        assertEquals(PID_1, queue.poll(DUMMY_TASK));
        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldReturnNull() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(alwaysFalse, client -> 1.0);
        queue.offerAll(Set.of(PID_1));

        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldReturnLeastLoadedClient() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(
            alwaysTrue,
            client -> (client == PID_1) ? 3.0 : (client == PID_2) ? 2.0 : 1.0
        );

        queue.offerAll(List.of(PID_1, PID_2, PID_3));

        assertEquals(PID_3, queue.poll(DUMMY_TASK));
        assertEquals(PID_2, queue.poll(DUMMY_TASK));
        assertEquals(PID_1, queue.poll(DUMMY_TASK));
        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldNotRetainDuplicates() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(alwaysTrue, client -> 1.0);

        queue.offerAll(Set.of(PID_1));
        queue.offer(PID_1);

        assertEquals(PID_1, queue.poll(DUMMY_TASK));
        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldOnlyReturnValidClients() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(
            (client, task) -> client.equals(PID_1),
            client -> 1.0
        );

        queue.offerAll(List.of(PID_1, PID_2));

        assertEquals(PID_1, queue.poll(DUMMY_TASK));
        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldApplyPollFilter() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(
            alwaysTrue,
            client -> 1.0
        );

        queue.offerAll(List.of(PID_1, PID_2));

        assertEquals(PID_1, queue.poll(DUMMY_TASK, client -> client.equals(PID_1)));
        assertNull(queue.poll(DUMMY_TASK, client -> client.equals(PID_1)));
        assertEquals(PID_2, queue.poll(DUMMY_TASK));
        assertNull(queue.poll(DUMMY_TASK));
    }
}
