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

import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class that tracks the current state and the duration of time spent in each state.
 * This class is threadsafe.
 */
public class StateTracker {

    private final AtomicReference<StateChange> lastState = new AtomicReference<>(new StateChange());

    /**
     * Change the current state.
     *
     * @param newState the current state; may not be null
     * @param now      the current time in milliseconds
     */
    public void changeState(State newState, long now) {
        lastState.getAndUpdate(oldState -> oldState.newState(newState, now));
    }

    /**
     * Calculate the ratio of time spent in the specified state.
     *
     * @param ratioState the state for which the ratio is to be calculated; may not be null
     * @param now        the current time in milliseconds
     * @return the ratio of time spent in the specified state to the time spent in all states
     */
    public double durationRatio(State ratioState, long now) {
        return lastState.get().durationRatio(ratioState, now);
    }

    /**
     * Get the current state.
     *
     * @return the current state; may be null if no state change has been recorded
     */
    public State currentState() {
        return lastState.get().state;
    }

    /**
     * An immutable record of the accumulated times at the most recent state change. This class is required to
     * efficiently make {@link StateTracker} threadsafe.
     */
    private static final class StateChange {

        private final State state;
        private final long startTime;
        private final long unassignedTotalTimeMs;
        private final long runningTotalTimeMs;
        private final long pausedTotalTimeMs;
        private final long stoppedTotalTimeMs;
        private final long failedTotalTimeMs;
        private final long destroyedTotalTimeMs;
        private final long restartingTotalTimeMs;

        /**
         * The initial StateChange instance before any state has changed.
         */
        StateChange() {
            this(null, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        StateChange(State state, long startTime, long unassignedTotalTimeMs, long runningTotalTimeMs, long pausedTotalTimeMs,
                            long stoppedTotalTimeMs, long failedTotalTimeMs, long destroyedTotalTimeMs, long restartingTotalTimeMs) {
            this.state = state;
            this.startTime = startTime;
            this.unassignedTotalTimeMs = unassignedTotalTimeMs;
            this.runningTotalTimeMs = runningTotalTimeMs;
            this.pausedTotalTimeMs = pausedTotalTimeMs;
            this.stoppedTotalTimeMs  = stoppedTotalTimeMs;
            this.failedTotalTimeMs = failedTotalTimeMs;
            this.destroyedTotalTimeMs = destroyedTotalTimeMs;
            this.restartingTotalTimeMs = restartingTotalTimeMs;
        }

        /**
         * Return a new StateChange that includes the accumulated times of this state plus the time spent in the
         * current state.
         *
         * @param state the new state; may not be null
         * @param now   the time at which the state transition occurs.
         * @return the new StateChange, though may be this instance of the state did not actually change; never null
         */
        public StateChange newState(State state, long now) {
            if (this.state == null) {
                return new StateChange(state, now, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
            }
            if (state == this.state) {
                return this;
            }
            long unassignedTime = this.unassignedTotalTimeMs;
            long runningTime = this.runningTotalTimeMs;
            long pausedTime = this.pausedTotalTimeMs;
            long stoppedTime = this.stoppedTotalTimeMs;
            long failedTime = this.failedTotalTimeMs;
            long destroyedTime = this.destroyedTotalTimeMs;
            long restartingTime = this.restartingTotalTimeMs;
            long duration = now - startTime;
            switch (this.state) {
                case UNASSIGNED:
                    unassignedTime += duration;
                    break;
                case RUNNING:
                    runningTime += duration;
                    break;
                case PAUSED:
                    pausedTime += duration;
                    break;
                case STOPPED:
                    stoppedTime += duration;
                    break;
                case FAILED:
                    failedTime += duration;
                    break;
                case DESTROYED:
                    destroyedTime += duration;
                    break;
                case RESTARTING:
                    restartingTime += duration;
                    break;
            }
            return new StateChange(state, now, unassignedTime, runningTime, pausedTime, stoppedTime, failedTime, destroyedTime, restartingTime);
        }

        /**
         * Calculate the ratio of time spent in the specified state.
         *
         * @param ratioState the state for which the ratio is to be calculated; may not be null
         * @param now        the current time in milliseconds
         * @return the ratio of time spent in the specified state to the time spent in all states
         */
        public double durationRatio(State ratioState, long now) {
            if (state == null) {
                return 0.0d;
            }
            long durationCurrent = now - startTime; // since last state change
            long durationDesired = ratioState == state ? durationCurrent : 0L;
            switch (ratioState) {
                case UNASSIGNED:
                    durationDesired += unassignedTotalTimeMs;
                    break;
                case RUNNING:
                    durationDesired += runningTotalTimeMs;
                    break;
                case PAUSED:
                    durationDesired += pausedTotalTimeMs;
                    break;
                case STOPPED:
                    durationDesired += stoppedTotalTimeMs;
                    break;
                case FAILED:
                    durationDesired += failedTotalTimeMs;
                    break;
                case DESTROYED:
                    durationDesired += destroyedTotalTimeMs;
                    break;
                case RESTARTING:
                    durationDesired += restartingTotalTimeMs;
                    break;
            }
            long total = durationCurrent + unassignedTotalTimeMs + runningTotalTimeMs + pausedTotalTimeMs +
                                 failedTotalTimeMs + destroyedTotalTimeMs + restartingTotalTimeMs;
            return total == 0.0d ? 0.0d : (double) durationDesired / total;
        }
    }
}
