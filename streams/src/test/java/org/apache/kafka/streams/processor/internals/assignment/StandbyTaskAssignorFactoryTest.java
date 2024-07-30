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

import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StandbyTaskAssignorFactoryTest {
    private static final long ACCEPTABLE_RECOVERY_LAG = 0L;
    private static final int MAX_WARMUP_REPLICAS = 1;
    private static final int NUMBER_OF_STANDBY_REPLICAS = 1;
    private static final long PROBING_REBALANCE_INTERVAL_MS = 60000L;

    enum State {
        DISABLED,
        ENABLED,
        NULL
    }

    private RackAwareTaskAssignor rackAwareTaskAssignor;

    public void setUp(final State state, final boolean needValidRack) {
        if (state == State.ENABLED || state == State.DISABLED) {
            rackAwareTaskAssignor = mock(RackAwareTaskAssignor.class);
            if (needValidRack) {
                when(rackAwareTaskAssignor.validClientRack()).thenReturn(state.equals(State.ENABLED));
            }
        } else {
            rackAwareTaskAssignor = null;
        }
    }

    @ParameterizedTest
    @EnumSource(State.class)
    public void shouldReturnClientTagAwareStandbyTaskAssignorWhenRackAwareAssignmentTagsIsSet(final State state) {
        setUp(state, false);
        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(newAssignmentConfigs(singletonList("az")), rackAwareTaskAssignor);
        assertInstanceOf(ClientTagAwareStandbyTaskAssignor.class, standbyTaskAssignor);
        if (state != State.NULL) {
            verify(rackAwareTaskAssignor, never()).racksForProcess();
            verify(rackAwareTaskAssignor, never()).validClientRack();
        }
    }

    @ParameterizedTest
    @EnumSource(State.class)
    public void shouldReturnDefaultOrRackAwareStandbyTaskAssignorWhenRackAwareAssignmentTagsIsEmpty(final State state) {
        setUp(state, true);
        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(newAssignmentConfigs(Collections.emptyList()), rackAwareTaskAssignor);
        if (state == State.ENABLED) {
            assertInstanceOf(ClientTagAwareStandbyTaskAssignor.class, standbyTaskAssignor);
            verify(rackAwareTaskAssignor, times(1)).racksForProcess();
            verify(rackAwareTaskAssignor, times(1)).validClientRack();
        } else if (state == State.DISABLED) {
            assertInstanceOf(DefaultStandbyTaskAssignor.class, standbyTaskAssignor);
            verify(rackAwareTaskAssignor, never()).racksForProcess();
            verify(rackAwareTaskAssignor, times(1)).validClientRack();
        } else {
            assertInstanceOf(DefaultStandbyTaskAssignor.class, standbyTaskAssignor);
        }
    }

    private static AssignmentConfigs newAssignmentConfigs(final List<String> rackAwareAssignmentTags) {
        return new AssignmentConfigs(ACCEPTABLE_RECOVERY_LAG,
                                     MAX_WARMUP_REPLICAS,
                                     NUMBER_OF_STANDBY_REPLICAS,
                                     PROBING_REBALANCE_INTERVAL_MS,
                                     rackAwareAssignmentTags);
    }
}