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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatState;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatStateIterator;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatStateList;
import org.apache.kafka.controller.BrokerHeartbeatManager.UsableBrokerIterator;
import org.apache.kafka.metadata.placement.UsableBroker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.controller.BrokerControlState.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.controller.BrokerControlState.FENCED;
import static org.apache.kafka.controller.BrokerControlState.SHUTDOWN_NOW;
import static org.apache.kafka.controller.BrokerControlState.UNFENCED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        assertFalse(manager.hasValidSession(0));
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.touch(0, false, 0);
        time.sleep(5);
        manager.touch(1, false, 0);
        manager.touch(2, false, 0);
        assertTrue(manager.hasValidSession(0));
        assertTrue(manager.hasValidSession(1));
        assertTrue(manager.hasValidSession(2));
        assertFalse(manager.hasValidSession(3));
        time.sleep(6);
        assertFalse(manager.hasValidSession(0));
        assertTrue(manager.hasValidSession(1));
        assertTrue(manager.hasValidSession(2));
        assertFalse(manager.hasValidSession(3));
        manager.remove(2);
        assertFalse(manager.hasValidSession(2));
        manager.remove(1);
        assertFalse(manager.hasValidSession(1));
    }

    @Test
    public void testFindOneStaleBroker() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertFalse(manager.hasValidSession(0));
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.touch(0, false, 0);
        time.sleep(5);
        manager.touch(1, false, 0);
        time.sleep(1);
        manager.touch(2, false, 0);

        Iterator<BrokerHeartbeatState> iter = manager.unfenced().iterator();
        assertEquals(0, iter.next().id());
        assertEquals(1, iter.next().id());
        assertEquals(2, iter.next().id());
        assertFalse(iter.hasNext());
        assertEquals(Optional.empty(), manager.findOneStaleBroker());

        time.sleep(5);
        assertEquals(Optional.of(0), manager.findOneStaleBroker());
        manager.fence(0);
        assertEquals(Optional.empty(), manager.findOneStaleBroker());
        iter = manager.unfenced().iterator();
        assertEquals(1, iter.next().id());
        assertEquals(2, iter.next().id());
        assertFalse(iter.hasNext());

        time.sleep(20);
        assertEquals(Optional.of(1), manager.findOneStaleBroker());
        manager.fence(1);
        assertEquals(Optional.of(2), manager.findOneStaleBroker());
        manager.fence(2);

        assertEquals(Optional.empty(), manager.findOneStaleBroker());
        iter = manager.unfenced().iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testNextCheckTimeNs() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertEquals(Long.MAX_VALUE, manager.nextCheckTimeNs());
        for (int brokerId = 0; brokerId < 4; brokerId++) {
            manager.register(brokerId, true);
        }
        manager.touch(0, false, 0);
        time.sleep(2);
        manager.touch(1, false, 0);
        time.sleep(1);
        manager.touch(2, false, 0);
        time.sleep(1);
        manager.touch(3, false, 0);
        assertEquals(Optional.empty(), manager.findOneStaleBroker());
        assertEquals(10_000_000, manager.nextCheckTimeNs());
        time.sleep(7);
        assertEquals(10_000_000, manager.nextCheckTimeNs());
        assertEquals(Optional.of(0), manager.findOneStaleBroker());
        manager.fence(0);
        assertEquals(12_000_000, manager.nextCheckTimeNs());

        time.sleep(3);
        assertEquals(Optional.of(1), manager.findOneStaleBroker());
        manager.fence(1);
        assertEquals(Optional.of(2), manager.findOneStaleBroker());
        manager.fence(2);

        assertEquals(14_000_000, manager.nextCheckTimeNs());
    }

    @Test
    public void testMetadataOffsetComparator() {
        TreeSet<BrokerHeartbeatState> set =
            new TreeSet<>(BrokerHeartbeatManager.MetadataOffsetComparator.INSTANCE);
        BrokerHeartbeatState broker1 = new BrokerHeartbeatState(1);
        BrokerHeartbeatState broker2 = new BrokerHeartbeatState(2);
        BrokerHeartbeatState broker3 = new BrokerHeartbeatState(3);
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
        broker1.metadataOffset = 800;
        broker2.metadataOffset = 400;
        broker3.metadataOffset = 100;
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
    public void testBrokerHeartbeatStateList() {
        BrokerHeartbeatStateList list = new BrokerHeartbeatStateList();
        assertNull(list.first());
        BrokerHeartbeatStateIterator iterator = list.iterator();
        assertFalse(iterator.hasNext());
        BrokerHeartbeatState broker0 = new BrokerHeartbeatState(0);
        broker0.lastContactNs = 200;
        BrokerHeartbeatState broker1 = new BrokerHeartbeatState(1);
        broker1.lastContactNs = 100;
        BrokerHeartbeatState broker2 = new BrokerHeartbeatState(2);
        broker2.lastContactNs = 50;
        BrokerHeartbeatState broker3 = new BrokerHeartbeatState(3);
        broker3.lastContactNs = 150;
        list.add(broker0);
        list.add(broker1);
        list.add(broker2);
        list.add(broker3);
        assertEquals(broker2, list.first());
        iterator = list.iterator();
        assertEquals(broker2, iterator.next());
        assertEquals(broker1, iterator.next());
        assertEquals(broker3, iterator.next());
        assertEquals(broker0, iterator.next());
        assertFalse(iterator.hasNext());
        list.remove(broker1);
        iterator = list.iterator();
        assertEquals(broker2, iterator.next());
        assertEquals(broker3, iterator.next());
        assertEquals(broker0, iterator.next());
        assertFalse(iterator.hasNext());
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
