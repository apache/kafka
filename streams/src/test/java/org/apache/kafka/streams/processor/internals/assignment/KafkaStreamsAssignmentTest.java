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

import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.processIdForInt;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaStreamsAssignmentTest {
    @Test
    public void shouldHaveReadableString() {
        final KafkaStreamsAssignment assignment = KafkaStreamsAssignment.of(
            processIdForInt(1),
            Set.of(
                new AssignedTask(TASK_0_0, AssignedTask.Type.ACTIVE),
                new AssignedTask(TASK_0_1, AssignedTask.Type.STANDBY),
                new AssignedTask(TASK_0_2, AssignedTask.Type.ACTIVE)
            )
        );

        assertThat(
            assignment.toString(),
            equalTo("KafkaStreamsAssignment{00000000-0000-0000-0000-000000000001, "
                    + "[AssignedTask{ACTIVE, 0_2}, AssignedTask{STANDBY, 0_1}, AssignedTask{ACTIVE, 0_0}], "
                    + "Optional.empty}"));
    }
}
