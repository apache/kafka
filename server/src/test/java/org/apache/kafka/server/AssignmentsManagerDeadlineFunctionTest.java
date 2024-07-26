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
package org.apache.kafka.server;

import org.apache.kafka.common.utils.ExponentialBackoff;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.apache.kafka.common.requests.AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssignmentsManagerDeadlineFunctionTest {
    private static final ExponentialBackoff BACKOFF = new ExponentialBackoff(1000, 2, 8000, 0.0);

    @Test
    public void applyAfterDispatchInterval() {
        assertEquals(OptionalLong.of(BACKOFF.initialInterval()),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 0, false, 12).
                apply(OptionalLong.empty()));
    }

    @Test
    public void applyAfterDispatchIntervalWithExistingEarlierDeadline() {
        assertEquals(OptionalLong.of(BACKOFF.initialInterval() / 2),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 0, false, 12).
                apply(OptionalLong.of(BACKOFF.initialInterval() / 2)));
    }

    @Test
    public void applyBackoffInterval() {
        assertEquals(OptionalLong.of(BACKOFF.initialInterval() * 2),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 1, false, 12).
                apply(OptionalLong.empty()));
    }

    @Test
    public void applyBackoffIntervalWithExistingEarlierDeadline() {
        assertEquals(OptionalLong.of(BACKOFF.initialInterval() / 2),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 1, false, 12).
                apply(OptionalLong.of(BACKOFF.initialInterval() / 2)));
    }

    @Test
    public void scheduleImmediatelyWhenOverloaded() {
        assertEquals(OptionalLong.of(0),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 0, false,
                MAX_ASSIGNMENTS_PER_REQUEST + 1).
                    apply(OptionalLong.of(BACKOFF.initialInterval() / 2)));
    }

    @Test
    public void doNotScheduleImmediatelyWhenOverloadedIfThereAreInFlightRequests() {
        assertEquals(OptionalLong.of(BACKOFF.initialInterval()),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 0, true,
                MAX_ASSIGNMENTS_PER_REQUEST + 1).
                    apply(OptionalLong.empty()));
    }

    @Test
    public void doNotScheduleImmediatelyWhenOverloadedIfThereArePreviousGlobalFailures() {
        assertEquals(OptionalLong.of(BACKOFF.initialInterval() * 2),
            new AssignmentsManagerDeadlineFunction(BACKOFF, 0, 1, false,
                MAX_ASSIGNMENTS_PER_REQUEST + 1).
                    apply(OptionalLong.empty()));
    }
}
