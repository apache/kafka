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

import java.util.UUID;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConstrainedPrioritySetTest {
    private static final TaskId DUMMY_TASK = new TaskId(0, 0);

    private final BiFunction<UUID, TaskId, Boolean> alwaysTrue = (client, task) -> true;
    private final BiFunction<UUID, TaskId, Boolean> alwaysFalse = (client, task) -> false;

    @Test
    public void shouldReturnOnlyClient() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(alwaysTrue, client -> 1.0);
        queue.offerAll(singleton(UUID_1));

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
        assertThat(queue.poll(DUMMY_TASK), nullValue());
    }

    @Test
    public void shouldReturnNull() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(alwaysFalse, client -> 1.0);
        queue.offerAll(singleton(UUID_1));

        assertThat(queue.poll(DUMMY_TASK), nullValue());
    }

    @Test
    public void shouldReturnLeastLoadedClient() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(
            alwaysTrue,
            client -> (client == UUID_1) ? 3.0 : (client == UUID_2) ? 2.0 : 1.0
        );

        queue.offerAll(asList(UUID_1, UUID_2, UUID_3));

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_3));
        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_2));
        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
        assertThat(queue.poll(DUMMY_TASK), nullValue());
    }

    @Test
    public void shouldNotRetainDuplicates() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(alwaysTrue, client -> 1.0);

        queue.offerAll(singleton(UUID_1));
        queue.offer(UUID_1);

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
        assertThat(queue.poll(DUMMY_TASK), nullValue());
    }

    @Test
    public void shouldOnlyReturnValidClients() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(
            (client, task) -> client.equals(UUID_1),
            client -> 1.0
        );

        queue.offerAll(asList(UUID_1, UUID_2));

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
        assertThat(queue.poll(DUMMY_TASK), nullValue());
    }

    @Test
    public void shouldApplyPollFilter() {
        final ConstrainedPrioritySet queue = new ConstrainedPrioritySet(
            alwaysTrue,
            client -> 1.0
        );

        queue.offerAll(asList(UUID_1, UUID_2));

        assertThat(queue.poll(DUMMY_TASK, client -> client.equals(UUID_1)), equalTo(UUID_1));
        assertThat(queue.poll(DUMMY_TASK, client -> client.equals(UUID_1)), nullValue());
        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_2));
        assertThat(queue.poll(DUMMY_TASK), nullValue());
    }
}
