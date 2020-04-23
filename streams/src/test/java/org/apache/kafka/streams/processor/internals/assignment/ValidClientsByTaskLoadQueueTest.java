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


import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

public class ValidClientsByTaskLoadQueueTest {
    
    private static final TaskId DUMMY_TASK = new TaskId(0, 0);

    private final ClientState client1 = new ClientState(1);
    private final ClientState client2 = new ClientState(1);
    private final ClientState client3 = new ClientState(1);

    private final BiFunction<UUID, TaskId, Boolean> alwaysTrue = (client, task) -> true;
    private final BiFunction<UUID, TaskId, Boolean> alwaysFalse = (client, task) -> false;

    private ValidClientsByTaskLoadQueue queue;

    private Map<UUID, ClientState> clientStates;

    @Test
    public void shouldReturnOnlyClient() {
        clientStates = getClientStatesMap(client1);
        queue = new ValidClientsByTaskLoadQueue(clientStates, alwaysTrue);
        queue.offerAll(clientStates.keySet());

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
    }

    @Test
    public void shouldReturnNull() {
        clientStates = getClientStatesMap(client1);
        queue = new ValidClientsByTaskLoadQueue(clientStates, alwaysFalse);
        queue.offerAll(clientStates.keySet());

        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldReturnLeastLoadedClient() {
        clientStates = getClientStatesMap(client1, client2, client3);
        queue = new ValidClientsByTaskLoadQueue(clientStates, alwaysTrue);

        client1.assignActive(TASK_0_0);
        client2.assignActiveTasks(asList(TASK_0_1, TASK_1_1));
        client3.assignActiveTasks(asList(TASK_0_2, TASK_1_2, TASK_2_2));

        queue.offerAll(clientStates.keySet());

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_2));
        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_3));
    }

    @Test
    public void shouldNotRetainDuplicates() {
        clientStates = getClientStatesMap(client1);
        queue = new ValidClientsByTaskLoadQueue(clientStates, alwaysTrue);

        queue.offerAll(clientStates.keySet());
        queue.offer(UUID_1);

        assertThat(queue.poll(DUMMY_TASK), equalTo(UUID_1));
        assertNull(queue.poll(DUMMY_TASK));
    }

    @Test
    public void shouldOnlyReturnValidClients() {
        clientStates = getClientStatesMap(client1, client2);
        queue = new ValidClientsByTaskLoadQueue(clientStates, (client, task) -> client.equals(UUID_1));

        queue.offerAll(clientStates.keySet());

        assertThat(queue.poll(DUMMY_TASK, 2), equalTo(singletonList(UUID_1)));
    }

    @Test
    public void shouldReturnUpToNumClients() {
        clientStates = getClientStatesMap(client1, client2, client3);
        queue = new ValidClientsByTaskLoadQueue(clientStates, alwaysTrue);

        client1.assignActive(TASK_0_0);
        client2.assignActiveTasks(asList(TASK_0_1, TASK_1_1));
        client3.assignActiveTasks(asList(TASK_0_2, TASK_1_2, TASK_2_2));

        queue.offerAll(clientStates.keySet());

        assertThat(queue.poll(DUMMY_TASK, 2), equalTo(asList(UUID_1, UUID_2)));
    }
}
