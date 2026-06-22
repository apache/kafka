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
import org.apache.kafka.common.utils.internals.ExponentialBackoff;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory per-group back-off that throttles broker re-solicitation of a topology
 * description push. An entry is armed when the broker decides to set
 * {@code TopologyDescriptionRequired=true} on a heartbeat or after a transient plugin
 * failure; consecutive arms at the same topology epoch advance the attempt count and
 * delegate to {@link ExponentialBackoff} for the next delay . Successful pushes, permanent plugin failures,
 * and topology-epoch advances clear the entry.
 */
public class StreamsGroupTopologyDescriptionBackoff {

    static final long INITIAL_DELAY_MS = 30_000L;
    static final long MAX_DELAY_MS = 3_600_000L;
    static final int MULTIPLIER = 2;
    static final double JITTER = 0.2;

    private final Time time;
    private final ExponentialBackoff exponentialBackoff;
    private final ConcurrentHashMap<String, Entry> state = new ConcurrentHashMap<>();

    record Entry(int topologyEpoch, int attempts, long nextAttemptMs) { }

    public StreamsGroupTopologyDescriptionBackoff(Time time) {
        this(time, new ExponentialBackoff(INITIAL_DELAY_MS, MULTIPLIER, MAX_DELAY_MS, JITTER));
    }

    // Visible for testing
    StreamsGroupTopologyDescriptionBackoff(Time time, ExponentialBackoff exponentialBackoff) {
        this.time = time;
        this.exponentialBackoff = exponentialBackoff;
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
     * installed, false if a window was already active and nothing changed. Used on the
     * heartbeat path so two concurrent heartbeats for the same group cannot both arm
     * the back-off, and to preserve the exponential chain when the previous window has
     * expired without a push reaching the coordinator (e.g. the client never sent one,
     * or the push was lost in flight before {@link #armOrExtend} could run).
     */
    public boolean armIfNotActive(String groupId, int topologyEpoch) {
        final long now = time.milliseconds();
        final AtomicBoolean armed = new AtomicBoolean(false);
        state.compute(groupId, (key, existing) -> {
            if (existing != null
                && existing.topologyEpoch() == topologyEpoch
                && now < existing.nextAttemptMs()) {
                return existing;
            }
            armed.set(true);
            return computeNextEntry(existing, topologyEpoch, now);
        });
        return armed.get();
    }

    /**
     * Arm a new back-off window or extend the existing one at the caller's topology epoch.
     *
     * <p>Epoch-aware: if the stored entry is for a newer topology epoch than {@code
     * topologyEpoch} (a concurrent heartbeat already armed for the advanced epoch while a
     * late post-plugin callback finishes at the old epoch), the call is a no-op so we do
     * not overwrite the newer window. At the same epoch the attempt count advances and
     * the next delay is drawn from {@link ExponentialBackoff}; at a stale-or-absent entry
     * we start a fresh chain at {@code attempts=0}.
     */
    public void armOrExtend(String groupId, int topologyEpoch) {
        final long now = time.milliseconds();
        state.compute(groupId, (key, existing) -> {
            if (existing != null && existing.topologyEpoch() > topologyEpoch) {
                return existing;
            }
            return computeNextEntry(existing, topologyEpoch, now);
        });
    }

    /**
     * Drop the back-off entry for a group at the given topology epoch. Epoch-aware: only
     * clears if the stored entry matches {@code topologyEpoch} (a late post-plugin
     * callback at the old epoch must not wipe a window a concurrent heartbeat armed at
     * the advanced epoch). Called on a successful push or a permanent plugin failure.
     */
    public void clear(String groupId, int topologyEpoch) {
        state.compute(groupId, (key, existing) -> {
            if (existing == null || existing.topologyEpoch() != topologyEpoch) {
                return existing;
            }
            return null;
        });
    }

    /**
     * Drop the back-off entry for a group unconditionally. Used by paths that remove the
     * group entirely (explicit {@code DeleteGroups}, periodic cleanup of naturally-expired
     * groups), where any in-flight back-off state is irrelevant because the group is gone.
     */
    public void clearGroup(String groupId) {
        state.remove(groupId);
    }

    // Visible for testing.
    Entry entry(String groupId) {
        return state.get(groupId);
    }

    /**
     * Build the next entry for an arm: same-epoch arms advance the attempt count to
     * continue the exponential chain; a different (or absent) epoch starts fresh at
     * {@code attempts=0}. The actual delay is drawn from {@link ExponentialBackoff} so
     * the multiplier / max / jitter are configured in one place rather than hand-rolled
     * twice.
     */
    private Entry computeNextEntry(Entry existing, int topologyEpoch, long now) {
        int nextAttempts =
            (existing != null && existing.topologyEpoch() == topologyEpoch)
                ? existing.attempts() + 1
                : 0;
        long delay = exponentialBackoff.backoff(nextAttempts);
        return new Entry(topologyEpoch, nextAttempts, now + delay);
    }
}
