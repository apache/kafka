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

import static java.util.Collections.emptySet;

import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

public class AssignmentTestUtils {
    public static final TopicPartition t1p0 = new TopicPartition("topic1", 0);
    public static final TopicPartition t1p1 = new TopicPartition("topic1", 1);
    public static final TopicPartition t1p2 = new TopicPartition("topic1", 2);
    public static final TopicPartition t1p3 = new TopicPartition("topic1", 3);
    public static final TopicPartition t2p0 = new TopicPartition("topic2", 0);
    public static final TopicPartition t2p1 = new TopicPartition("topic2", 1);
    public static final TopicPartition t2p2 = new TopicPartition("topic2", 2);
    public static final TopicPartition t2p3 = new TopicPartition("topic2", 3);
    public static final TopicPartition t3p0 = new TopicPartition("topic3", 0);
    public static final TopicPartition t3p1 = new TopicPartition("topic3", 1);
    public static final TopicPartition t3p2 = new TopicPartition("topic3", 2);
    public static final TopicPartition t3p3 = new TopicPartition("topic3", 3);
    public static final TopicPartition t4p0 = new TopicPartition("topic4", 0);
    public static final TopicPartition t4p1 = new TopicPartition("topic4", 1);
    public static final TopicPartition t4p2 = new TopicPartition("topic4", 2);
    public static final TopicPartition t4p3 = new TopicPartition("topic4", 3);

    public static final TaskId task0_0 = new TaskId(0, 0);
    public static final TaskId task0_1 = new TaskId(0, 1);
    public static final TaskId task0_2 = new TaskId(0, 2);
    public static final TaskId task0_3 = new TaskId(0, 3);
    public static final TaskId task1_0 = new TaskId(1, 0);
    public static final TaskId task1_1 = new TaskId(1, 1);
    public static final TaskId task1_2 = new TaskId(1, 2);
    public static final TaskId task1_3 = new TaskId(1, 3);
    public static final TaskId task2_0 = new TaskId(2, 0);
    public static final TaskId task2_1 = new TaskId(2, 1);
    public static final TaskId task2_2 = new TaskId(2, 2);
    public static final TaskId task2_3 = new TaskId(2, 3);

    public static final UUID uuid1 = UUID.randomUUID();
    public static final UUID uuid2 = UUID.randomUUID();
    public static final UUID uuid3 = UUID.randomUUID();

    public static final Set<TaskId> emptyTasks = emptySet();
}
