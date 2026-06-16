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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory per-group back-off that throttles broker re-solicitation of a topology
 * description push. An entry is armed when the broker decides to set
 * {@code TopologyDescriptionRequired=true} on a heartbeat or after a transient plugin
 * failure; consecutive arms at the same topology epoch double the window from
 * {@value #INITIAL_DELAY_MS} ms up to {@value #MAX_DELAY_MS} ms. Successful pushes,
 * permanent plugin failures, and topology-epoch advances clear the entry.
 */
public class StreamsGroupTopologyDescriptionBackoff {

    static final long INITIAL_DELAY_MS = 30_000L;
    static final long MAX_DELAY_MS = 3_600_000L;

    private final Time time;
    private final ConcurrentHashMap<String, Entry> state = new ConcurrentHashMap<>();

    record Entry(int topologyEpoch, long currentDelayMs, long nextAttemptMs) { }

    public StreamsGroupTopologyDescriptionBackoff(Time time) {
        this.time = time;
    }

    /**
     * @return true if a back-off window is in effect for the given group at the given
     *         topology epoch and the broker should suppress soliciting another push.
     */
    public boolean isActive(String groupId, int topologyEpoch) {
        Entry entry = state.get(groupId);
        return entry != null
            && entry.topologyEpoch() == topologyEpoch
            && time.milliseconds() < entry.nextAttemptMs();
    }

    /**
     * Atomic check-and-arm. Returns true if no window was in effect and a new one was
     * armed, false if a window was already active and nothing changed. Used on the
     * heartbeat path to fold the "check + arm" pair into a single compute so two
     * concurrent heartbeats for the same group cannot both arm the back-off, and to
     * preserve the exponential chain when the previous window has expired without a
     * push reaching the coordinator (e.g. the client never sent one, or the push was
     * lost in flight before {@link #armOrExtend} could run).
     */
    public boolean armIfNotActive(String groupId, int topologyEpoch) {
        final long now = time.milliseconds();
        final boolean[] armed = new boolean[]{false};
        state.compute(groupId, (key, existing) -> {
            if (existing != null
                && existing.topologyEpoch() == topologyEpoch
                && now < existing.nextAttemptMs()) {
                return existing;
            }
            armed[0] = true;
            // Re-arm: continue the exponential chain at the same epoch (the previous
            // window expired without a push completing); reset to INITIAL_DELAY_MS on
            // an epoch advance, which implicitly drops the prior history.
            if (existing != null && existing.topologyEpoch() == topologyEpoch) {
                long nextDelay = Math.min(existing.currentDelayMs() * 2, MAX_DELAY_MS);
                return new Entry(topologyEpoch, nextDelay, now + nextDelay);
            }
            return new Entry(topologyEpoch, INITIAL_DELAY_MS, now + INITIAL_DELAY_MS);
        });
        return armed[0];
    }

    /**
     * Arm a new back-off window or extend the existing one. If the existing entry is for a
     * different topology epoch the window is reset to {@link #INITIAL_DELAY_MS}.
     */
    public void armOrExtend(String groupId, int topologyEpoch) {
        final long now = time.milliseconds();
        state.compute(groupId, (key, existing) -> {
            if (existing == null || existing.topologyEpoch() != topologyEpoch) {
                return new Entry(topologyEpoch, INITIAL_DELAY_MS, now + INITIAL_DELAY_MS);
            }
            long nextDelay = Math.min(existing.currentDelayMs() * 2, MAX_DELAY_MS);
            return new Entry(topologyEpoch, nextDelay, now + nextDelay);
        });
    }

    /**
     * Drop the back-off entry for a group. Called on a successful push, a permanent plugin
     * failure, or when the group is removed.
     */
    public void clear(String groupId) {
        state.remove(groupId);
    }

    // Visible for testing.
    Entry entry(String groupId) {
        return state.get(groupId);
    }
}
