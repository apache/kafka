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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TasksTest {

    private final static TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);
    private final static TopicPartition TOPIC_PARTITION_A_1 = new TopicPartition("topicA", 1);
    private final static TaskId TASK_0_0 = new TaskId(0, 0);
    private final static TaskId TASK_0_1 = new TaskId(0, 1);
    private final static TaskId TASK_1_0 = new TaskId(1, 0);

    private final LogContext logContext = new LogContext();

    @Test
    public void shouldCreateTasks() {
        final Tasks tasks = new Tasks(logContext);
        final StreamTask statefulTask = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).build();
        final StreamTask statelessTask = statelessTask(TASK_1_0).build();

        tasks.addNewActiveTasks(mkSet(statefulTask, statelessTask));
        tasks.addNewStandbyTasks(Collections.singletonList(standbyTask));

        assertEquals(statefulTask, tasks.task(statefulTask.id()));
        assertTrue(tasks.activeTasks().contains(statefulTask));
        assertTrue(tasks.allTasks().contains(statefulTask));
        assertTrue(tasks.tasks(mkSet(statefulTask.id())).contains(statefulTask));
        assertEquals(statelessTask, tasks.task(statelessTask.id()));
        assertTrue(tasks.activeTasks().contains(statelessTask));
        assertTrue(tasks.allTasks().contains(statelessTask));
        assertTrue(tasks.tasks(mkSet(statelessTask.id())).contains(statelessTask));
        assertEquals(standbyTask, tasks.task(standbyTask.id()));
        assertTrue(tasks.allTasks().contains(standbyTask));
        assertTrue(tasks.tasks(mkSet(standbyTask.id())).contains(standbyTask));
    }
}