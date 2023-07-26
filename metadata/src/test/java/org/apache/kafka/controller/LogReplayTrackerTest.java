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
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class LogReplayTrackerTest {
    private MockFaultHandler faultHandler = new MockFaultHandler("LogReplayTrackerTest");

    @AfterEach
    public void afterEach() {
        faultHandler.maybeRethrowFirstException();
    }

    @Test
    public void testNoLeaderName() {
        assertEquals("(none)", LogReplayTracker.leaderName(OptionalInt.empty()));
    }

    @Test
    public void testLeaderName() {
        assertEquals("123", LogReplayTracker.leaderName(OptionalInt.of(123)));
    }

    class LogReplayTrackerTestContext {
        final MockTime time;
        final AtomicReference<String> leadershipState;
        final LogReplayTracker tracker;

        LogReplayTrackerTestContext() {
            this(__ -> { });
        }

        LogReplayTrackerTestContext(Consumer<LogReplayTracker.Builder> builderConsumer) {
            this.time = new MockTime(0, 100, 200);
            this.leadershipState = new AtomicReference<>("none");
            LogReplayTracker.Builder builder = new LogReplayTracker.Builder().
                setTime(time).
                setNodeId(3).
                setGainLeadershipCallback(e -> leadershipState.set("gained leadership at " + e)).
                setLoseLeadershipCallback(e -> leadershipState.set("lost leadership at " + e)).
                setFatalFaultHandler(faultHandler);
            builderConsumer.accept(builder);
            tracker = builder.build();
        }
    }

    @Test
    public void testNodeId() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        assertEquals(3, ctx.tracker.nodeId());
    }

    @Test
    public void testEmpty() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        assertTrue(ctx.tracker.empty());
        ctx.tracker.setNotEmpty();
        assertFalse(ctx.tracker.empty());
    }

    @Test
    public void testInitialSettings() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        assertEquals(-1, ctx.tracker.curClaimEpoch());
        assertFalse(ctx.tracker.active());
        assertEquals(-1L, ctx.tracker.lastCommittedOffset());
        assertEquals(-1L, ctx.tracker.lastCommittedEpoch());
        assertEquals(Long.MAX_VALUE, ctx.tracker.lastStableOffset());
        assertEquals(LeaderAndEpoch.UNKNOWN, ctx.tracker.raftLeader());
        assertEquals("Cannot access the next write offset when we are not active.",
            assertThrows(RuntimeException.class,
                () -> ctx.tracker.nextWriteOffset()).
                    getMessage());
    }

    @Test
    public void testInitialMetrics() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        QuorumControllerMetrics metrics = ctx.tracker.controllerMetrics();
        assertFalse(metrics.active());
        assertEquals(0L, metrics.newActiveControllers());
        assertEquals(-1L, metrics.lastAppliedRecordOffset());
        assertEquals(-1L, metrics.lastCommittedRecordOffset());
        assertEquals(-1L, metrics.lastAppliedRecordTimestamp());
    }

    @Test
    public void testHandleLeaderChangeWithoutActivation() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        LeaderAndEpoch leader = new LeaderAndEpoch(OptionalInt.of(1), 456);
        ctx.tracker.handleLeaderChange(leader, 234);
        assertEquals(1L, ctx.tracker.controllerMetrics().newActiveControllers());
        assertEquals("none", ctx.leadershipState.get());
        assertEquals(leader, ctx.tracker.raftLeader());
        assertEquals(-1, ctx.tracker.curClaimEpoch());
        assertFalse(ctx.tracker.active());
        assertEquals(-1L, ctx.tracker.lastCommittedOffset());
        assertEquals(-1L, ctx.tracker.lastCommittedEpoch());
        assertEquals(Long.MAX_VALUE, ctx.tracker.lastStableOffset());
        assertEquals("Cannot access the next write offset when we are not active.",
            assertThrows(RuntimeException.class,
                () -> ctx.tracker.nextWriteOffset()).
                    getMessage());
    }

    @Test
    public void testHandleLeaderChangeWithActivation() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        LeaderAndEpoch leader = new LeaderAndEpoch(OptionalInt.of(3), 456);
        ctx.tracker.handleLeaderChange(leader, 234);
        assertEquals("gained leadership at 456", ctx.leadershipState.get());
        assertEquals(leader, ctx.tracker.raftLeader());
        assertEquals(456, ctx.tracker.curClaimEpoch());
        assertTrue(ctx.tracker.active());
        assertEquals(-1L, ctx.tracker.lastCommittedOffset());
        assertEquals(-1L, ctx.tracker.lastCommittedEpoch());
        assertEquals(-1L, ctx.tracker.lastStableOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHandleLeaderChangeWithDeactivation(boolean resignation) {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        LeaderAndEpoch prevLeader = new LeaderAndEpoch(OptionalInt.of(3), 456);
        ctx.tracker.handleLeaderChange(prevLeader, 234);
        if (resignation) {
            ctx.tracker.resign();
            assertEquals(prevLeader, ctx.tracker.raftLeader());
            assertEquals("lost leadership at 456", ctx.leadershipState.get());
        } else {
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(1), 457);
            ctx.tracker.handleLeaderChange(newLeader, 235);
            assertEquals(2L, ctx.tracker.controllerMetrics().newActiveControllers());
            assertEquals(newLeader, ctx.tracker.raftLeader());
            assertEquals("lost leadership at 457", ctx.leadershipState.get());
        }
        assertEquals(-1, ctx.tracker.curClaimEpoch());
        assertFalse(ctx.tracker.active());
        assertEquals(-1L, ctx.tracker.lastCommittedOffset());
        assertEquals(-1L, ctx.tracker.lastCommittedEpoch());
        assertEquals(Long.MAX_VALUE, ctx.tracker.lastStableOffset());
        assertEquals("Cannot access the next write offset when we are not active.",
            assertThrows(RuntimeException.class,
                () -> ctx.tracker.nextWriteOffset()).
                    getMessage());
    }

    @Test
    public void testUpdateNextWriteOffsetFailsIfInactive() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        assertEquals("Cannot update the next write offset when we are not active.",
            assertThrows(RuntimeException.class,
                () -> ctx.tracker.updateNextWriteOffset(123)).
                    getMessage());
    }

    @Test
    public void testUpdateNextWriteOffsetWhenActive() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        LeaderAndEpoch leader = new LeaderAndEpoch(OptionalInt.of(3), 456);
        ctx.tracker.handleLeaderChange(leader, 567);
        assertEquals(567, ctx.tracker.nextWriteOffset());
        assertEquals(566, ctx.tracker.controllerMetrics().lastAppliedRecordOffset());
        assertEquals(ctx.time.milliseconds(),
                ctx.tracker.controllerMetrics().lastAppliedRecordTimestamp());
    }

    @Test
    public void testDoubleResignIsAnError() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        ctx.tracker.handleLeaderChange(new LeaderAndEpoch(OptionalInt.of(3), 456), 567);
        ctx.tracker.resign();
        assertEquals("LogReplayTrackerTest: Cannot resign at epoch 456, because we already resigned.",
            assertThrows(FaultHandlerException.class,
                () -> ctx.tracker.resign()).
                    getMessage());
        faultHandler.setIgnore(true);
    }

    @Test
    public void testStandbyResigningIsAnError() {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        ctx.tracker.handleLeaderChange(new LeaderAndEpoch(OptionalInt.of(2), 456), 567);
        assertEquals("LogReplayTrackerTest: Cannot resign at epoch 456, because we are not the Raft " +
                "leader at this epoch.",
            assertThrows(FaultHandlerException.class,
                () -> ctx.tracker.resign()).
                    getMessage());
        faultHandler.setIgnore(true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testUpdateLastCommittedState(boolean active) {
        LogReplayTrackerTestContext ctx = new LogReplayTrackerTestContext();
        if (active) {
            ctx.tracker.handleLeaderChange(new LeaderAndEpoch(OptionalInt.of(3), 456), 567);
            assertTrue(ctx.tracker.active());
        } else {
            ctx.tracker.handleLeaderChange(new LeaderAndEpoch(OptionalInt.of(2), 456), 567);
            assertFalse(ctx.tracker.active());
        }
        ctx.tracker.updateLastCommittedState(567, 456, 1000);
        assertEquals(567L, ctx.tracker.lastCommittedOffset());
        assertEquals(567L, ctx.tracker.controllerMetrics().lastCommittedRecordOffset());
        if (active) {
            assertEquals(566L, ctx.tracker.controllerMetrics().lastAppliedRecordOffset());
        } else {
            assertEquals(567L, ctx.tracker.controllerMetrics().lastAppliedRecordOffset());
        }
        assertEquals(456, ctx.tracker.lastCommittedEpoch());
        assertEquals(1000L, ctx.tracker.lastCommittedTimestamp());
        if (active) {
            assertEquals(ctx.time.milliseconds(),
                    ctx.tracker.controllerMetrics().lastAppliedRecordTimestamp());
        } else {
            assertEquals(1000L, ctx.tracker.controllerMetrics().lastAppliedRecordTimestamp());
        }
    }
}
