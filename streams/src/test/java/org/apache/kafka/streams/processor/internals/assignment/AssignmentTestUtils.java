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
import static java.util.Collections.emptySet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

public class AssignmentTestUtils {

    public static final UUID UUID_1 = uuidForInt(1);
    public static final UUID UUID_2 = uuidForInt(2);
    public static final UUID UUID_3 = uuidForInt(3);
    public static final UUID UUID_4 = uuidForInt(4);
    public static final UUID UUID_5 = uuidForInt(5);
    public static final UUID UUID_6 = uuidForInt(6);

    public static final TaskId TASK_0_0 = new TaskId(0, 0);
    public static final TaskId TASK_0_1 = new TaskId(0, 1);
    public static final TaskId TASK_0_2 = new TaskId(0, 2);
    public static final TaskId TASK_0_3 = new TaskId(0, 3);
    public static final TaskId TASK_0_4 = new TaskId(0, 4);
    public static final TaskId TASK_0_5 = new TaskId(0, 5);
    public static final TaskId TASK_0_6 = new TaskId(0, 6);
    public static final TaskId TASK_1_0 = new TaskId(1, 0);
    public static final TaskId TASK_1_1 = new TaskId(1, 1);
    public static final TaskId TASK_1_2 = new TaskId(1, 2);
    public static final TaskId TASK_1_3 = new TaskId(1, 3);
    public static final TaskId TASK_2_0 = new TaskId(2, 0);
    public static final TaskId TASK_2_1 = new TaskId(2, 1);
    public static final TaskId TASK_2_2 = new TaskId(2, 2);
    public static final TaskId TASK_2_3 = new TaskId(2, 3);
    public static final TaskId TASK_3_4 = new TaskId(3, 4);

    public static final Set<TaskId> EMPTY_TASKS = emptySet();
    public static final Map<TaskId, Long> EMPTY_TASK_OFFSET_SUMS = emptyMap();
    public static final Map<TopicPartition, Long> EMPTY_CHANGELOG_END_OFFSETS = new HashMap<>();

    /**
     * Builds a UUID by repeating the given number n. For valid n, it is guaranteed that the returned UUIDs satisfy
     * the same relation relative to others as their parameter n does: iff n < m, then uuidForInt(n) < uuidForInt(m)
     *
     * @param n an integer between 1 and 7
     * @return the UUID created by repeating the digit n in the UUID format
     */
    static UUID uuidForInt(final Integer n) {
        return new UUID(0, n);
    }
}
