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

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class BrokerHeartbeatTrackerTest {
    private static BrokerHeartbeatTracker newBrokerHeartbeatTracker() {
        MockTime time = new MockTime(0, 1_000_000, 0);
        return new BrokerHeartbeatTracker(time, 10_000_000);
    }

    private static final Set<BrokerIdAndEpoch> TEST_BROKERS = Set.of(
        new BrokerIdAndEpoch(0, 0L),
        new BrokerIdAndEpoch(1, 100L),
        new BrokerIdAndEpoch(2, 200L)
    );

    @Test
    public void testUpdateContactTime() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        assertEquals(OptionalLong.empty(), tracker.contactTime(new BrokerIdAndEpoch(1, 100L)));
        tracker.updateContactTime(new BrokerIdAndEpoch(1, 100L));
        assertEquals(OptionalLong.of(0L), tracker.contactTime(new BrokerIdAndEpoch(1, 100L)));
    }

    @Test
    public void testMaybeRemoveExpiredWithEmptyTracker() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        assertEquals(Optional.empty(), tracker.maybeRemoveExpired());
    }

    @Test
    public void testMaybeRemoveExpiredWithAllUpToDate() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        TEST_BROKERS.forEach(tracker::updateContactTime);
        assertEquals(Optional.empty(), tracker.maybeRemoveExpired());
    }

    @Test
    public void testMaybeRemoveExpiredWithAllExpired() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        TEST_BROKERS.forEach(tracker::updateContactTime);
        tracker.time().sleep(11);
        Set<BrokerIdAndEpoch> expired = new HashSet<>();
        Optional<BrokerIdAndEpoch> idAndEpoch;
        do {
            idAndEpoch = tracker.maybeRemoveExpired();
            idAndEpoch.ifPresent(expired::add);
        } while (idAndEpoch.isPresent());
        assertEquals(TEST_BROKERS, expired);
    }

    @Test
    public void testHasValidSessionIsTrueForKnownBroker() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        TEST_BROKERS.forEach(tracker::updateContactTime);
        assertTrue(tracker.hasValidSession(new BrokerIdAndEpoch(2, 200L)));
    }

    @Test
    public void testHasValidSessionIsFalseForUnknownBroker() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        TEST_BROKERS.forEach(tracker::updateContactTime);
        assertFalse(tracker.hasValidSession(new BrokerIdAndEpoch(3, 300L)));
    }

    @Test
    public void testHasValidSessionIsFalseForUnknownBrokerEpoch() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        TEST_BROKERS.forEach(tracker::updateContactTime);
        assertFalse(tracker.hasValidSession(new BrokerIdAndEpoch(2, 100L)));
    }

    @Test
    public void testIsExpiredIsFalseForTheCurrentTime() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        assertFalse(tracker.isExpired(456, 456));
    }

    @Test
    public void testIsExpiredIsFalseForTenNanosecondsAfter() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        assertFalse(tracker.isExpired(456, 466));
    }

    @Test
    public void testIsExpiredIsTrueAfterExpirationTime() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        assertTrue(tracker.isExpired(456, 456 + 10_000_001));
    }

    @Test
    public void testIsExpiredIsFalseForPreviousTime() {
        BrokerHeartbeatTracker tracker = newBrokerHeartbeatTracker();
        assertFalse(tracker.isExpired(456, 0));
    }
}
