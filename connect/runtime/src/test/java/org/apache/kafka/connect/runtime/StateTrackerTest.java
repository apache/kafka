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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.AbstractStatus.State;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StateTrackerTest {

    private static final double DELTA = 0.000001d;

    private StateTracker tracker;
    private MockTime time;

    @Before
    public void setUp() {
        time = new MockTime();
        time.sleep(1000L);
        tracker = new StateTracker();
    }

    @Test
    public void currentStateIsNullWhenNotInitialized() {
        assertNull(tracker.currentState());
    }

    @Test
    public void currentState() {
        for (State state : State.values()) {
            tracker.changeState(state, time.milliseconds());
            assertEquals(state, tracker.currentState());
        }
    }

    @Test
    public void calculateDurations() {
        tracker.changeState(State.UNASSIGNED, time.milliseconds());
        time.sleep(1000L);
        assertEquals(1.0d, tracker.durationRatio(State.UNASSIGNED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.RUNNING, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.PAUSED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.FAILED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.DESTROYED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.RESTARTING, time.milliseconds()), DELTA);

        tracker.changeState(State.RUNNING, time.milliseconds());
        time.sleep(3000L);
        assertEquals(0.25d, tracker.durationRatio(State.UNASSIGNED, time.milliseconds()), DELTA);
        assertEquals(0.75d, tracker.durationRatio(State.RUNNING, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.PAUSED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.FAILED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.DESTROYED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.RESTARTING, time.milliseconds()), DELTA);

        tracker.changeState(State.PAUSED, time.milliseconds());
        time.sleep(4000L);
        assertEquals(0.125d, tracker.durationRatio(State.UNASSIGNED, time.milliseconds()), DELTA);
        assertEquals(0.375d, tracker.durationRatio(State.RUNNING, time.milliseconds()), DELTA);
        assertEquals(0.500d, tracker.durationRatio(State.PAUSED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.FAILED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.DESTROYED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.RESTARTING, time.milliseconds()), DELTA);

        tracker.changeState(State.RUNNING, time.milliseconds());
        time.sleep(8000L);
        assertEquals(0.0625d, tracker.durationRatio(State.UNASSIGNED, time.milliseconds()), DELTA);
        assertEquals(0.6875d, tracker.durationRatio(State.RUNNING, time.milliseconds()), DELTA);
        assertEquals(0.2500d, tracker.durationRatio(State.PAUSED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.FAILED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.DESTROYED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.RESTARTING, time.milliseconds()), DELTA);

        tracker.changeState(State.FAILED, time.milliseconds());
        time.sleep(16000L);
        assertEquals(0.03125d, tracker.durationRatio(State.UNASSIGNED, time.milliseconds()), DELTA);
        assertEquals(0.34375d, tracker.durationRatio(State.RUNNING, time.milliseconds()), DELTA);
        assertEquals(0.12500d, tracker.durationRatio(State.PAUSED, time.milliseconds()), DELTA);
        assertEquals(0.50000d, tracker.durationRatio(State.FAILED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.DESTROYED, time.milliseconds()), DELTA);
        assertEquals(0.0d, tracker.durationRatio(State.RESTARTING, time.milliseconds()), DELTA);

    }

}