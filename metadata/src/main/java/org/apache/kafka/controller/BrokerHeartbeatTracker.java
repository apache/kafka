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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.Time;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The BrokerheartbeatTracker stores the last time each broker sent a heartbeat to us.
 * This class will be present only on the active controller.
 *
 * UNLIKE MOST OF THE KAFKA CONTROLLER, THIS CLASS CAN BE ACCESSED FROM MULTIPLE THREADS.
 * Everything in here must be thread-safe. It is intended to be accessed directly from the
 * request handler thread pool. This ensures that the heartbeats always get through, even
 * if the main controller thread is busy.
 */
class BrokerHeartbeatTracker {
    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * The broker session timeout in nanoseconds.
     */
    private final long sessionTimeoutNs;

    /**
     * Maps a broker ID and epoch to the last contact time in monotonic nanoseconds.
     */
    private final ConcurrentHashMap<BrokerIdAndEpoch, Long> contactTimes;

    BrokerHeartbeatTracker(Time time, long sessionTimeoutNs) {
        this.time = time;
        this.sessionTimeoutNs = sessionTimeoutNs;
        this.contactTimes = new ConcurrentHashMap<>();
    }

    Time time() {
        return time;
    }

    /**
     * Update the contact time for the given broker ID and epoch to be the current time.
     *
     * @param idAndEpoch    The broker ID and epoch.
     */
    void updateContactTime(BrokerIdAndEpoch idAndEpoch) {
        updateContactTime(idAndEpoch, time.nanoseconds());
    }

    /**
     * Update the contact time for the given broker ID and epoch to be the given time.
     *
     * @param idAndEpoch    The broker ID and epoch.
     * @param timeNs        The monotonic time in nanoseconds.
     */
    void updateContactTime(BrokerIdAndEpoch idAndEpoch, long timeNs) {
        contactTimes.put(idAndEpoch, timeNs);
    }

    /**
     * Get the contact time for the given broker ID and epoch.
     *
     * @param idAndEpoch    The broker ID and epoch.
     * @return              The contact time, or Optional.empty if none is known.
     */
    OptionalLong contactTime(BrokerIdAndEpoch idAndEpoch) {
        Long value = contactTimes.get(idAndEpoch);
        if (value == null) return OptionalLong.empty();
        return OptionalLong.of(value);
    }

    /**
     * Remove either one or zero expired brokers from the map.
     *
     * @return      The expired broker that was removed, or Optional.empty if there was none.
     */
    Optional<BrokerIdAndEpoch> maybeRemoveExpired() {
        return maybeRemoveExpired(time.nanoseconds());
    }

    /**
     * Remove either one or zero expired brokers from the map.
     *
     * @param nowNs The current time in monotonic nanoseconds.
     *
     * @return      The expired broker that was removed, or Optional.empty if there was none.
     */
    Optional<BrokerIdAndEpoch> maybeRemoveExpired(long nowNs) {
        Iterator<Entry<BrokerIdAndEpoch, Long>> iterator =
            contactTimes.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<BrokerIdAndEpoch, Long> entry = iterator.next();
            if (isExpired(entry.getValue(), nowNs)) {
                iterator.remove();
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }

    /**
     * Return true if the given time is outside the expiration window.
     * If the timestamp has undergone 64-bit rollover, we will not expire anything.
     *
     * @param timeNs    The provided time in monotonic nanoseconds.
     * @param nowNs     The current time in monotonic nanoseconds.
     * @return          True if the timestamp is expired.
     */
    boolean isExpired(long timeNs, long nowNs) {
        return (nowNs > timeNs) && (timeNs + sessionTimeoutNs < nowNs);
    }

    /**
     * Return true if the given broker has a session whose time has not yet expired.
     *
     * @param idAndEpoch    The broker id and epoch.
     * @return              True only if the broker session was found and is still valid.
     */
    boolean hasValidSession(BrokerIdAndEpoch idAndEpoch) {
        Long timeNs = contactTimes.get(idAndEpoch);
        if (timeNs == null) return false;
        return !isExpired(timeNs, time.nanoseconds());
    }
}
