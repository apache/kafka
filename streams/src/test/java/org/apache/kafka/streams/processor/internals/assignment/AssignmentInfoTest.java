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
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.MIN_NAMED_TOPOLOGY_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentInfoTest {
    private final List<TaskId> activeTasks = Arrays.asList(
        TASK_0_0,
        TASK_0_1,
        TASK_1_0,
        TASK_1_0
    );

    private final Map<TaskId, Set<TopicPartition>> standbyTasks = mkMap(
        mkEntry(TASK_1_0, Set.of(new TopicPartition("t1", 0), new TopicPartition("t2", 0))),
        mkEntry(TASK_1_1, Set.of(new TopicPartition("t1", 1), new TopicPartition("t2", 1)))
    );

    private static final List<TaskId> NAMED_ACTIVE_TASKS = Arrays.asList(
        NAMED_TASK_T0_0_1,
        NAMED_TASK_T0_1_0,
        NAMED_TASK_T0_1_0,
        NAMED_TASK_T1_0_1,
        NAMED_TASK_T1_0_1,
        NAMED_TASK_T2_0_0,
        NAMED_TASK_T2_2_0
    );

    private static final Map<TaskId, Set<TopicPartition>> NAMED_STANDBY_TASKS = mkMap(
        mkEntry(NAMED_TASK_T0_0_0, Set.of(new TopicPartition("t0-1", 0), new TopicPartition("t0-2", 0))),
        mkEntry(NAMED_TASK_T0_0_1, Set.of(new TopicPartition("t0-1", 1), new TopicPartition("t0-2", 1))),
        mkEntry(NAMED_TASK_T1_0_0, Set.of(new TopicPartition("t1-1", 0), new TopicPartition("t1-2", 0)))
    );

    private final Map<HostInfo, Set<TopicPartition>> activeAssignment = mkMap(
        mkEntry(new HostInfo("localhost", 8088),
            Set.of(new TopicPartition("t0", 0),
                new TopicPartition("t1", 0),
                new TopicPartition("t2", 0))),
        mkEntry(new HostInfo("localhost", 8089),
            Set.of(new TopicPartition("t0", 1),
                new TopicPartition("t1", 1),
                new TopicPartition("t2", 1)))
    );

    private final Map<HostInfo, Set<TopicPartition>> standbyAssignment = mkMap(
        mkEntry(new HostInfo("localhost", 8088),
            Set.of(new TopicPartition("t1", 0),
                new TopicPartition("t2", 0))),
        mkEntry(new HostInfo("localhost", 8089),
            Set.of(new TopicPartition("t1", 1),
                new TopicPartition("t2", 1)))
    );

    @Test
    public void shouldUseLatestSupportedVersionByDefault() {
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        assertEquals(LATEST_SUPPORTED_VERSION, info.version());
    }

    @Test
    public void shouldThrowForUnknownVersion1() {
        assertThrows(IllegalArgumentException.class, () -> new AssignmentInfo(0, activeTasks, standbyTasks,
            activeAssignment, Collections.emptyMap(), 0));
    }

    @Test
    public void shouldThrowForUnknownVersion2() {
        assertThrows(IllegalArgumentException.class, () -> new AssignmentInfo(LATEST_SUPPORTED_VERSION + 1,
            activeTasks, standbyTasks, activeAssignment, Collections.emptyMap(), 0));
    }

    @Test
    public void shouldEncodeAndDecodeVersion1() {
        final AssignmentInfo info = new AssignmentInfo(1, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(1, UNKNOWN, activeTasks, standbyTasks, Collections.emptyMap(), Collections.emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion2() {
        final AssignmentInfo info = new AssignmentInfo(2, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(2, UNKNOWN, activeTasks, standbyTasks, activeAssignment, Collections.emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion3() {
        final AssignmentInfo info = new AssignmentInfo(3, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(3, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks,
            activeAssignment, Collections.emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion4() {
        final AssignmentInfo info = new AssignmentInfo(4, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo = new AssignmentInfo(4, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks,
            activeAssignment, Collections.emptyMap(), 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion5() {
        final AssignmentInfo info = new AssignmentInfo(5, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo = new AssignmentInfo(5, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks,
            activeAssignment, Collections.emptyMap(), 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion6() {
        final AssignmentInfo info = new AssignmentInfo(6, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo = new AssignmentInfo(6, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks,
            activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion7() {
        final AssignmentInfo info =
            new AssignmentInfo(7, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(7, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion8() {
        final AssignmentInfo info =
            new AssignmentInfo(8, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(8, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion9() {
        final AssignmentInfo info =
            new AssignmentInfo(9, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(9, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10() {
        final AssignmentInfo info =
            new AssignmentInfo(10, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(10, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10WithNamedTopologies() {
        final AssignmentInfo info =
            new AssignmentInfo(10, LATEST_SUPPORTED_VERSION, NAMED_ACTIVE_TASKS, NAMED_STANDBY_TASKS, activeAssignment, standbyAssignment, 2);
        assertEquals(info, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldNotEncodeAndDecodeNamedTopologiesWithOlderVersion() {
        final AssignmentInfo info =
            new AssignmentInfo(MIN_NAMED_TOPOLOGY_VERSION - 1, LATEST_SUPPORTED_VERSION, NAMED_ACTIVE_TASKS, NAMED_STANDBY_TASKS, activeAssignment, standbyAssignment, 2);
        assertThrows(TaskAssignmentException.class, () -> AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeSmallerCommonlySupportedVersion() {
        final int usedVersion = 5;
        final int commonlySupportedVersion = 5;
        final AssignmentInfo info = new AssignmentInfo(usedVersion, commonlySupportedVersion, activeTasks, standbyTasks,
            activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion, activeTasks, standbyTasks,
            activeAssignment, Collections.emptyMap(), 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void nextRebalanceTimeShouldBeMaxValueByDefault() {
        final AssignmentInfo info = new AssignmentInfo(7, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        assertEquals(info.nextRebalanceMs(), Long.MAX_VALUE);
    }

    @Test
    public void shouldDecodeDefaultNextRebalanceTime() {
        final AssignmentInfo info = new AssignmentInfo(7, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        assertEquals(info.nextRebalanceMs(), Long.MAX_VALUE);
    }

    @Test
    public void shouldEncodeAndDecodeNextRebalanceTime() {
        final AssignmentInfo info = new AssignmentInfo(7, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        info.setNextRebalanceTime(1000L);
        assertEquals(1000L, AssignmentInfo.decode(info.encode()).nextRebalanceMs());
    }
}
