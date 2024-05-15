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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_1_0;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.junit.Test;

public class KafkaStreamsStateTest {
    @Test
    public void shouldCorrectlyReturnTasksByLag() {
        final KafkaStreamsState state = new KafkaStreamsStateImpl(
            new ProcessId(UUID.randomUUID()),
            10,
            mkMap(),
            mkSortedSet(NAMED_TASK_T0_0_0, NAMED_TASK_T0_0_1),
            mkSortedSet(),
            new TreeMap<>(mkMap(
                mkEntry("c1", mkSet(NAMED_TASK_T0_0_0, NAMED_TASK_T0_0_1))
            )),
            Optional.empty(),
            Optional.of(
                mkMap(
                    mkEntry(NAMED_TASK_T0_0_0, 2000L),
                    mkEntry(NAMED_TASK_T0_0_1, 1000L)
                )
            )
        );

        assertThrows(IllegalStateException.class, () -> state.lagFor(NAMED_TASK_T0_1_0));
        assertThat(state.lagFor(NAMED_TASK_T0_0_0), equalTo(2000L));
        assertThat(state.lagFor(NAMED_TASK_T0_0_1), equalTo(1000L));

        assertThat(state.prevTasksByLag("c0"), equalTo(new TreeSet<>()));
        assertThat(state.prevTasksByLag("c1"), equalTo(new TreeSet<>(
            Arrays.asList(NAMED_TASK_T0_0_1, NAMED_TASK_T0_0_0)
        )));
    }

    @Test
    public void shouldThrowExceptionOnLagOperationsIfLagsWereNotComputed() {
        final KafkaStreamsState state = new KafkaStreamsStateImpl(
            new ProcessId(UUID.randomUUID()),
            10,
            mkMap(),
            mkSortedSet(NAMED_TASK_T0_0_0, NAMED_TASK_T0_0_1),
            mkSortedSet(),
            new TreeMap<>(mkMap(
                mkEntry("c1", mkSet(NAMED_TASK_T0_0_0, NAMED_TASK_T0_0_1))
            )),
            Optional.empty(),
            Optional.empty()
        );

        assertThrows(UnsupportedOperationException.class, () -> state.lagFor(NAMED_TASK_T0_0_0));
        assertThrows(UnsupportedOperationException.class, () -> state.prevTasksByLag("c1"));
        assertThrows(UnsupportedOperationException.class, state::statefulTasksToLagSums);
    }
}
