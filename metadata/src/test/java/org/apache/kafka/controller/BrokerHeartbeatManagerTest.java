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

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatState;
import org.apache.kafka.controller.BrokerHeartbeatManager.UsableBrokerIterator;
import org.apache.kafka.metadata.placement.UsableBroker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.kafka.controller.BrokerControlState.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.controller.BrokerControlState.FENCED;
import static org.apache.kafka.controller.BrokerControlState.SHUTDOWN_NOW;
import static org.apache.kafka.controller.BrokerControlState.UNFENCED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class BrokerHeartbeatManagerTest {
    private static BrokerHeartbeatManager newBrokerHeartbeatManager() {
        LogContext logContext = new LogContext();
        MockTime time = new MockTime(0, 1_000_000, 0);
        return new BrokerHeartbeatManager(logContext, time, 10_000_000);
    }

    @Test
    public void testHasValidSession() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertFalse(manager.hasValidSession(0, 100L));
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.tracker().updateContactTime(new BrokerIdAndEpoch(0, 100L));
        manager.touch(0, false, 0);
        time.sleep(5);
        manager.tracker().updateContactTime(new BrokerIdAndEpoch(1, 100L));
        manager.touch(1, false, 0);
        manager.tracker().updateContactTime(new BrokerIdAndEpoch(2, 200L));
        manager.touch(2, false, 0);
        assertTrue(manager.hasValidSession(0, 100L));
        assertFalse(manager.hasValidSession(0, 200L));
        assertTrue(manager.hasValidSession(1, 100L));
        assertTrue(manager.hasValidSession(2, 200L));
        assertFalse(manager.hasValidSession(3, 300L));
    }

    @Test
    public void testMetadataOffsetComparator() {
        TreeSet<BrokerHeartbeatState> set =
            new TreeSet<>(BrokerHeartbeatManager.MetadataOffsetComparator.INSTANCE);
        BrokerHeartbeatState broker1 = new BrokerHeartbeatState(1, false, -1L, -1L);
        BrokerHeartbeatState broker2 = new BrokerHeartbeatState(2, false, -1L, -1L);
        BrokerHeartbeatState broker3 = new BrokerHeartbeatState(3, false, -1L, -1L);
        set.add(broker1);
        set.add(broker2);
        set.add(broker3);
        Iterator<BrokerHeartbeatState> iterator = set.iterator();
        assertEquals(broker1, iterator.next());
        assertEquals(broker2, iterator.next());
        assertEquals(broker3, iterator.next());
        assertFalse(iterator.hasNext());
        assertTrue(set.remove(broker1));
        assertTrue(set.remove(broker2));
        assertTrue(set.remove(broker3));
        assertTrue(set.isEmpty());
        broker1.setMetadataOffset(800);
        broker2.setMetadataOffset(400);
        broker3.setMetadataOffset(100);
        set.add(broker1);
        set.add(broker2);
        set.add(broker3);
        iterator = set.iterator();
        assertEquals(broker3, iterator.next());
        assertEquals(broker2, iterator.next());
        assertEquals(broker1, iterator.next());
        assertFalse(iterator.hasNext());
    }

    private static Set<UsableBroker> usableBrokersToSet(BrokerHeartbeatManager manager) {
        Set<UsableBroker> brokers = new HashSet<>();
        for (Iterator<UsableBroker> iterator = new UsableBrokerIterator(
            manager.brokers().iterator(),
            id -> id % 2 == 0 ? Optional.of("rack1") : Optional.of("rack2"));
             iterator.hasNext(); ) {
            brokers.add(iterator.next());
        }
        return brokers;
    }

    @Test
    public void testUsableBrokerIterator() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        assertEquals(Collections.emptySet(), usableBrokersToSet(manager));
        for (int brokerId = 0; brokerId < 5; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.touch(0, false, 100);
        manager.touch(1, false, 100);
        manager.touch(2, false, 98);
        manager.touch(3, false, 100);
        manager.touch(4, true, 100);
        assertEquals(98L, manager.lowestActiveOffset());
        Set<UsableBroker> expected = new HashSet<>();
        expected.add(new UsableBroker(0, Optional.of("rack1"), false));
        expected.add(new UsableBroker(1, Optional.of("rack2"), false));
        expected.add(new UsableBroker(2, Optional.of("rack1"), false));
        expected.add(new UsableBroker(3, Optional.of("rack2"), false));
        expected.add(new UsableBroker(4, Optional.of("rack1"), true));
        assertEquals(expected, usableBrokersToSet(manager));
        manager.maybeUpdateControlledShutdownOffset(2, 0);
        assertEquals(100L, manager.lowestActiveOffset());
        assertThrows(RuntimeException.class,
            () -> manager.maybeUpdateControlledShutdownOffset(4, 0));
        manager.touch(4, false, 100);
        manager.maybeUpdateControlledShutdownOffset(4, 0);
        expected.remove(new UsableBroker(2, Optional.of("rack1"), false));
        expected.remove(new UsableBroker(4, Optional.of("rack1"), true));
        assertEquals(expected, usableBrokersToSet(manager));
    }

    @Test
    public void testControlledShutdownOffsetIsOnlyUpdatedOnce() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        assertEquals(Collections.emptySet(), usableBrokersToSet(manager));
        for (int brokerId = 0; brokerId < 5; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.touch(0, false, 100);
        manager.touch(1, false, 100);
        manager.touch(2, false, 98);
        manager.touch(3, false, 100);
        manager.touch(4, true, 100);
        assertEquals(OptionalLong.empty(), manager.controlledShutdownOffset(2));
        manager.maybeUpdateControlledShutdownOffset(2, 98);
        assertEquals(OptionalLong.of(98), manager.controlledShutdownOffset(2));
        manager.maybeUpdateControlledShutdownOffset(2, 99);
        assertEquals(OptionalLong.of(98), manager.controlledShutdownOffset(2));
        assertEquals(OptionalLong.empty(), manager.controlledShutdownOffset(3));
        manager.maybeUpdateControlledShutdownOffset(3, 101);
        assertEquals(OptionalLong.of(101), manager.controlledShutdownOffset(3));
        manager.maybeUpdateControlledShutdownOffset(3, 102);
        assertEquals(OptionalLong.of(101), manager.controlledShutdownOffset(3));
    }

    @Test
    public void testCalculateNextBrokerState() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        for (int brokerId = 0; brokerId < 6; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.touch(0, true, 100);
        manager.touch(1, false, 98);
        manager.touch(2, false, 100);
        manager.touch(3, false, 100);
        manager.touch(4, true, 100);
        manager.touch(5, false, 99);
        manager.maybeUpdateControlledShutdownOffset(5, 99);

        assertEquals(98L, manager.lowestActiveOffset());

        assertEquals(new BrokerControlStates(FENCED, SHUTDOWN_NOW),
            manager.calculateNextBrokerState(0,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> false));
        assertEquals(new BrokerControlStates(FENCED, UNFENCED),
            manager.calculateNextBrokerState(0,
                new BrokerHeartbeatRequestData().setWantFence(false).
                    setCurrentMetadataOffset(100), 100, () -> false));
        assertEquals(new BrokerControlStates(FENCED, FENCED),
            manager.calculateNextBrokerState(0,
                new BrokerHeartbeatRequestData().setWantFence(false).
                    setCurrentMetadataOffset(50), 100, () -> false));
        assertEquals(new BrokerControlStates(FENCED, FENCED),
            manager.calculateNextBrokerState(0,
                new BrokerHeartbeatRequestData().setWantFence(true), 100, () -> false));

        assertEquals(new BrokerControlStates(UNFENCED, CONTROLLED_SHUTDOWN),
            manager.calculateNextBrokerState(1,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> true));
        assertEquals(new BrokerControlStates(UNFENCED, SHUTDOWN_NOW),
            manager.calculateNextBrokerState(1,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> false));
        assertEquals(new BrokerControlStates(UNFENCED, UNFENCED),
            manager.calculateNextBrokerState(1,
                new BrokerHeartbeatRequestData().setWantFence(false), 100, () -> false));

        assertEquals(new BrokerControlStates(CONTROLLED_SHUTDOWN, CONTROLLED_SHUTDOWN),
            manager.calculateNextBrokerState(5,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> true));
        assertEquals(new BrokerControlStates(CONTROLLED_SHUTDOWN, CONTROLLED_SHUTDOWN),
            manager.calculateNextBrokerState(5,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> false));
        manager.fence(1);
        assertEquals(new BrokerControlStates(CONTROLLED_SHUTDOWN, SHUTDOWN_NOW),
            manager.calculateNextBrokerState(5,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> false));
        assertEquals(new BrokerControlStates(CONTROLLED_SHUTDOWN, CONTROLLED_SHUTDOWN),
            manager.calculateNextBrokerState(5,
                new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> true));
        assertEquals("Broker 6 is not registered.",
                assertThrows(IllegalStateException.class,
                        () -> manager.calculateNextBrokerState(6, new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> true)).getMessage());
        assertEquals("Broker 7 is not registered.",
                assertThrows(IllegalStateException.class,
                        () -> manager.calculateNextBrokerState(7, new BrokerHeartbeatRequestData().setWantShutDown(true), 100, () -> true)).getMessage());
    }

    @Test
    public void testTouchThrowsExceptionUnlessRegistered() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        manager.register(1, true);
        manager.register(3, true);
        assertEquals("Broker 2 is not registered.",
                assertThrows(IllegalStateException.class,
                        () -> manager.touch(2, false, 0)).getMessage());
        assertEquals("Broker 4 is not registered.",
                assertThrows(IllegalStateException.class,
                        () -> manager.touch(4, false, 0)).getMessage());
    }
}
