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

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentUtils.taskIsCaughtUpOnClientOrNoCaughtUpClientsExist;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

public class AssignmentUtilsTest {

    @Test
    public void shouldReturnTrueIfTaskHasNoCaughtUpClients() {
        assertTrue(taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(TASK_0_0, UUID_1, emptyMap()));
    }

    @Test
    public void shouldReturnTrueIfTaskIsCaughtUpOnClient() {
        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        tasksToCaughtUpClients.put(TASK_0_0, mkSortedSet(UUID_1));

        assertTrue(taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(TASK_0_0, UUID_1, tasksToCaughtUpClients));
    }

    @Test
    public void shouldReturnFalseIfTaskWasNotCaughtUpOnClientButCaughtUpClientsExist() {
        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        tasksToCaughtUpClients.put(TASK_0_0, mkSortedSet(UUID_2));

        assertFalse(taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(TASK_0_0, UUID_1, tasksToCaughtUpClients));
    }
}
