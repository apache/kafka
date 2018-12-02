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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AssignmentInfoTest {
    private final List<TaskId> activeTasks = Arrays.asList(
        new TaskId(0, 0),
        new TaskId(0, 0),
        new TaskId(0, 1), new TaskId(1, 0));
    private final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<TaskId, Set<TopicPartition>>() {
        {
            put(new TaskId(1, 1),
                Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t2", 1)));
            put(new TaskId(2, 0),
                Utils.mkSet(new TopicPartition("t3", 0), new TopicPartition("t3", 0)));
        }
    };
    private final Map<HostInfo, Set<TopicPartition>> globalAssignment = new HashMap<HostInfo, Set<TopicPartition>>() {
        {
            put(new HostInfo("localhost", 80),
                Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t3", 3)));
        }
    };

    @Test
    public void shouldUseLatestSupportedVersionByDefault() {
        final AssignmentInfo info = new AssignmentInfo(activeTasks, standbyTasks, globalAssignment);
        assertEquals(AssignmentInfo.LATEST_SUPPORTED_VERSION, info.version());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownVersion1() {
        new AssignmentInfo(0, activeTasks, standbyTasks, globalAssignment, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownVersion2() {
        new AssignmentInfo(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1, activeTasks, standbyTasks, globalAssignment, 0);
    }

    @Test
    public void shouldEncodeAndDecodeVersion1() {
        final AssignmentInfo info = new AssignmentInfo(1, activeTasks, standbyTasks, globalAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(1, AssignmentInfo.UNKNOWN, activeTasks, standbyTasks, Collections.<HostInfo, Set<TopicPartition>>emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion2() {
        final AssignmentInfo info = new AssignmentInfo(2, activeTasks, standbyTasks, globalAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(2, AssignmentInfo.UNKNOWN, activeTasks, standbyTasks, globalAssignment, 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion3() {
        final AssignmentInfo info = new AssignmentInfo(3, activeTasks, standbyTasks, globalAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(3, AssignmentInfo.LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, globalAssignment, 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion4() {
        final AssignmentInfo info = new AssignmentInfo(4, activeTasks, standbyTasks, globalAssignment, 2);
        final AssignmentInfo expectedInfo = new AssignmentInfo(4, AssignmentInfo.LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, globalAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }
}
