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

import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class StandbyTaskAssignorFactoryTest {
    @org.junit.Rule
    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

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

    @Parameter
    public State state;

    @Parameterized.Parameters(name = "RackAwareTaskAssignor={0}")
    public static Collection<State> parameters() {
        return Arrays.asList(State.DISABLED, State.ENABLED, State.NULL);
    }

    @Before
    public void setUp() {
        if (state == State.ENABLED) {
            rackAwareTaskAssignor = mock(RackAwareTaskAssignor.class);
            when(rackAwareTaskAssignor.validClientRack()).thenReturn(true);
        } else if (state == State.DISABLED) {
            rackAwareTaskAssignor = mock(RackAwareTaskAssignor.class);
            when(rackAwareTaskAssignor.validClientRack()).thenReturn(false);
        } else {
            rackAwareTaskAssignor = null;
        }
    }

    @Test
    public void shouldReturnClientTagAwareStandbyTaskAssignorWhenRackAwareAssignmentTagsIsSet() {
        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(newAssignmentConfigs(singletonList("az")), rackAwareTaskAssignor);
        assertInstanceOf(ClientTagAwareStandbyTaskAssignor.class, standbyTaskAssignor);
        if (state != State.NULL) {
            verify(rackAwareTaskAssignor, never()).racksForProcess();
            verify(rackAwareTaskAssignor, never()).validClientRack();
        }
    }

    @Test
    public void shouldReturnDefaultOrRackAwareStandbyTaskAssignorWhenRackAwareAssignmentTagsIsEmpty() {
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

    private static AssignorConfiguration.AssignmentConfigs newAssignmentConfigs(final List<String> rackAwareAssignmentTags) {
        return new AssignorConfiguration.AssignmentConfigs(ACCEPTABLE_RECOVERY_LAG,
                                                           MAX_WARMUP_REPLICAS,
                                                           NUMBER_OF_STANDBY_REPLICAS,
                                                           PROBING_REBALANCE_INTERVAL_MS,
                                                           rackAwareAssignmentTags);
    }
}