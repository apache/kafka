/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AssginmentInfoTest {

    @Test
    public void testEncodeDecode() {
        List<TaskId> activeTasks =
                Arrays.asList(new TaskId(0, 0), new TaskId(0, 0), new TaskId(0, 1), new TaskId(1, 0));
        Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        standbyTasks.put(new TaskId(1, 1), Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t2", 1)));
        standbyTasks.put(new TaskId(2, 0), Utils.mkSet(new TopicPartition("t3", 0), new TopicPartition("t3", 0)));

        AssignmentInfo info = new AssignmentInfo(activeTasks, standbyTasks);
        AssignmentInfo decoded = AssignmentInfo.decode(info.encode());

        assertEquals(info, decoded);
    }

}
