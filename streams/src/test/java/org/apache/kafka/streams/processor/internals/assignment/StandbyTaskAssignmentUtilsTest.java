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

import org.junit.Test;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class StandbyTaskAssignmentUtilsTest {

    @Test
    public void testTaskToRemainingStandbysComputation() {
        assertThat(
            computeTasksToRemainingStandbys(0, mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            equalTo(
                mkMap(
                    mkEntry(TASK_0_0, 0),
                    mkEntry(TASK_0_1, 0),
                    mkEntry(TASK_0_2, 0)
                )
            )
        );
        assertThat(
            computeTasksToRemainingStandbys(5, mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            equalTo(
                mkMap(
                    mkEntry(TASK_0_0, 5),
                    mkEntry(TASK_0_1, 5),
                    mkEntry(TASK_0_2, 5)
                )
            )
        );
    }
}