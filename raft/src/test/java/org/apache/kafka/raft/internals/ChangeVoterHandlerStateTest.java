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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.ReplicaKey;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChangeVoterHandlerStateTest {

    private KafkaMetric getMetric(Metrics metrics, String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
    }

    @Test
    public void testAddVoterLifecycle() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            assertFalse(state.addVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());

            ReplicaKey voterKey = ReplicaKey.of(1, Uuid.randomUuid());
            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                voterKey,
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            CompletableFuture<AddRaftVoterResponseData> future = addVoterState.future().toCompletableFuture();

            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));
            assertTrue(state.addVoterHandlerState().isPresent());
            assertEquals(addVoterState, state.addVoterHandlerState().get());
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            assertFalse(future.isDone());
            state.resetAddVoterHandlerState(Errors.INVALID_REQUEST, "test error", Optional.empty());
            assertFalse(state.addVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());

            assertTrue(future.isDone());
            assertEquals(Errors.INVALID_REQUEST, Errors.forCode(future.join().errorCode()));
            assertEquals("test error", future.join().errorMessage());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testRemoveVoterLifecycle() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            assertFalse(state.removeVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );
            CompletableFuture<RemoveRaftVoterResponseData> future = removeVoterState.future().toCompletableFuture();

            state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));
            assertTrue(state.removeVoterHandlerState().isPresent());
            assertEquals(removeVoterState, state.removeVoterHandlerState().get());
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            assertFalse(future.isDone());
            state.resetRemoveVoterHandlerState(Errors.INVALID_REQUEST, "test error", Optional.empty());
            assertFalse(state.removeVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());

            assertTrue(future.isDone());
            assertEquals(Errors.INVALID_REQUEST, Errors.forCode(future.join().errorCode()));
            assertEquals("test error", future.join().errorMessage());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testMaybeExpirePendingAddVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            assertEquals(Long.MAX_VALUE, state.maybeExpirePendingOperation(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));

            time.sleep(500);
            assertEquals(500, state.maybeExpirePendingOperation(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertTrue(state.addVoterHandlerState().isPresent());
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            time.sleep(500);
            assertEquals(Long.MAX_VALUE, state.maybeExpirePendingOperation(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertFalse(state.addVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());
            var future = addVoterState.future().toCompletableFuture();
            assertTrue(future.isDone());
            assertEquals(Errors.REQUEST_TIMED_OUT, Errors.forCode(future.join().errorCode()));
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testMaybeExpirePendingRemoveVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );
            state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));

            time.sleep(500);
            assertEquals(500, state.maybeExpirePendingOperation(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertTrue(state.removeVoterHandlerState().isPresent());
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            time.sleep(500);
            assertEquals(Long.MAX_VALUE, state.maybeExpirePendingOperation(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertFalse(state.removeVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());
            var future = removeVoterState.future().toCompletableFuture();
            assertTrue(future.isDone());
            assertEquals(Errors.REQUEST_TIMED_OUT, Errors.forCode(future.join().errorCode()));
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testIsOperationPendingWithAddVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            assertFalse(state.isOperationPending(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));

            assertTrue(state.isOperationPending(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            time.sleep(1000);
            assertFalse(state.isOperationPending(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertFalse(state.addVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testIsOperationPendingWithRemoveVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            assertFalse(state.isOperationPending(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );
            state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));

            assertTrue(state.isOperationPending(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            time.sleep(1000);
            assertFalse(state.isOperationPending(LeaderAndEpoch.UNKNOWN, Endpoints.empty(), time.milliseconds()));
            assertFalse(state.removeVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testMaybeResetPendingVoterHandlerState() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));

            CompletableFuture<AddRaftVoterResponseData> addFuture = addVoterState.future().toCompletableFuture();

            assertFalse(addFuture.isDone());
            assertEquals(1, getMetric(metrics, "uncommitted-voter-change").metricValue());

            state.maybeResetPendingVoterHandlerState(
                Errors.NOT_LEADER_OR_FOLLOWER,
                LeaderAndEpoch.UNKNOWN,
                Endpoints.empty()
            );

            assertFalse(state.addVoterHandlerState().isPresent());
            assertEquals(0, getMetric(metrics, "uncommitted-voter-change").metricValue());
            assertTrue(addFuture.isDone());
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(addFuture.join().errorCode()));
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCannotSetAddVoterWhenRemoveVoterPresent() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );
            state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));
            assertTrue(state.removeVoterHandlerState().isPresent());

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );

            assertThrows(IllegalStateException.class, () -> {
                state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));
            });

            assertTrue(state.removeVoterHandlerState().isPresent());
            assertFalse(state.addVoterHandlerState().isPresent());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCannotSetRemoveVoterWhenAddVoterPresent() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));
            assertTrue(state.addVoterHandlerState().isPresent());

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );

            assertThrows(IllegalStateException.class, () -> {
                state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));
            });

            assertTrue(state.addVoterHandlerState().isPresent());
            assertFalse(state.removeVoterHandlerState().isPresent());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCanReplaceAddVoterWithAnotherAddVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            AddVoterHandlerState firstAddVoter = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            CompletableFuture<AddRaftVoterResponseData> firstFuture = firstAddVoter.future().toCompletableFuture();
            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(firstAddVoter));

            AddVoterHandlerState secondAddVoter = new AddVoterHandlerState(
                ReplicaKey.of(2, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            state.resetAddVoterHandlerState(Errors.OPERATION_NOT_ATTEMPTED, null, Optional.of(secondAddVoter));

            assertTrue(state.addVoterHandlerState().isPresent());
            assertEquals(secondAddVoter, state.addVoterHandlerState().get());
            assertFalse(state.removeVoterHandlerState().isPresent());
            assertTrue(firstFuture.isDone());
            assertEquals(Errors.OPERATION_NOT_ATTEMPTED, Errors.forCode(firstFuture.join().errorCode()));
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCanReplaceRemoveVoterWithAnotherRemoveVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            RemoveVoterHandlerState firstRemoveVoter = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );
            CompletableFuture<RemoveRaftVoterResponseData> firstFuture = firstRemoveVoter.future().toCompletableFuture();
            state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(firstRemoveVoter));

            RemoveVoterHandlerState secondRemoveVoter = new RemoveVoterHandlerState(
                200L,
                time.timer(1000)
            );
            state.resetRemoveVoterHandlerState(Errors.OPERATION_NOT_ATTEMPTED, null, Optional.of(secondRemoveVoter));

            assertTrue(state.removeVoterHandlerState().isPresent());
            assertEquals(secondRemoveVoter, state.removeVoterHandlerState().get());
            assertFalse(state.addVoterHandlerState().isPresent());
            assertTrue(firstFuture.isDone());
            assertEquals(Errors.OPERATION_NOT_ATTEMPTED, Errors.forCode(firstFuture.join().errorCode()));
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCannotSetAddVoterWhenUpdateVoterPresent() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            UpdateVoterHandlerState updateVoterState = new UpdateVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                new ListenerName("PLAINTEXT"),
                new SupportedVersionRange((short) 0, (short) 1),
                time.timer(1000)
            );
            state.resetUpdateVoterHandlerState(
                Errors.NONE,
                LeaderAndEpoch.UNKNOWN,
                Endpoints.empty(),
                Optional.of(updateVoterState)
            );
            assertTrue(state.updateVoterHandlerState().isPresent());

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(2, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );

            assertThrows(IllegalStateException.class, () -> {
                state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));
            });

            assertTrue(state.updateVoterHandlerState().isPresent());
            assertFalse(state.addVoterHandlerState().isPresent());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCannotSetRemoveVoterWhenUpdateVoterPresent() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            UpdateVoterHandlerState updateVoterState = new UpdateVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                new ListenerName("PLAINTEXT"),
                new SupportedVersionRange((short) 0, (short) 1),
                time.timer(1000)
            );
            state.resetUpdateVoterHandlerState(
                Errors.NONE,
                LeaderAndEpoch.UNKNOWN,
                Endpoints.empty(),
                Optional.of(updateVoterState)
            );
            assertTrue(state.updateVoterHandlerState().isPresent());

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );

            assertThrows(IllegalStateException.class, () -> {
                state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));
            });

            assertTrue(state.updateVoterHandlerState().isPresent());
            assertFalse(state.removeVoterHandlerState().isPresent());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCannotSetUpdateVoterWhenAddVoterPresent() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            AddVoterHandlerState addVoterState = new AddVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                false,
                time.timer(1000)
            );
            state.resetAddVoterHandlerState(Errors.NONE, null, Optional.of(addVoterState));
            assertTrue(state.addVoterHandlerState().isPresent());

            UpdateVoterHandlerState updateVoterState = new UpdateVoterHandlerState(
                ReplicaKey.of(2, Uuid.randomUuid()),
                Endpoints.empty(),
                new ListenerName("PLAINTEXT"),
                new SupportedVersionRange((short) 0, (short) 1),
                time.timer(1000)
            );

            assertThrows(IllegalStateException.class, () -> {
                state.resetUpdateVoterHandlerState(
                    Errors.NONE,
                    LeaderAndEpoch.UNKNOWN,
                    Endpoints.empty(),
                    Optional.of(updateVoterState)
                );
            });

            assertTrue(state.addVoterHandlerState().isPresent());
            assertFalse(state.updateVoterHandlerState().isPresent());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCannotSetUpdateVoterWhenRemoveVoterPresent() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            RemoveVoterHandlerState removeVoterState = new RemoveVoterHandlerState(
                100L,
                time.timer(1000)
            );
            state.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.of(removeVoterState));
            assertTrue(state.removeVoterHandlerState().isPresent());

            UpdateVoterHandlerState updateVoterState = new UpdateVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                new ListenerName("PLAINTEXT"),
                new SupportedVersionRange((short) 0, (short) 1),
                time.timer(1000)
            );

            assertThrows(IllegalStateException.class, () -> {
                state.resetUpdateVoterHandlerState(
                    Errors.NONE,
                    LeaderAndEpoch.UNKNOWN,
                    Endpoints.empty(),
                    Optional.of(updateVoterState)
                );
            });

            assertTrue(state.removeVoterHandlerState().isPresent());
            assertFalse(state.updateVoterHandlerState().isPresent());
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }

    @Test
    public void testCanReplaceUpdateVoterWithAnotherUpdateVoter() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "raft");
        raftMetrics.addLeaderMetrics();

        try {
            ChangeVoterHandlerState state = new ChangeVoterHandlerState(raftMetrics);

            UpdateVoterHandlerState firstUpdateVoter = new UpdateVoterHandlerState(
                ReplicaKey.of(1, Uuid.randomUuid()),
                Endpoints.empty(),
                new ListenerName("PLAINTEXT"),
                new SupportedVersionRange((short) 0, (short) 1),
                time.timer(1000)
            );
            CompletableFuture<UpdateRaftVoterResponseData> firstFuture = firstUpdateVoter.future().toCompletableFuture();
            state.resetUpdateVoterHandlerState(
                Errors.NONE,
                LeaderAndEpoch.UNKNOWN,
                Endpoints.empty(),
                Optional.of(firstUpdateVoter)
            );

            UpdateVoterHandlerState secondUpdateVoter = new UpdateVoterHandlerState(
                ReplicaKey.of(2, Uuid.randomUuid()),
                Endpoints.empty(),
                new ListenerName("PLAINTEXT"),
                new SupportedVersionRange((short) 0, (short) 1),
                time.timer(1000)
            );
            state.resetUpdateVoterHandlerState(
                Errors.OPERATION_NOT_ATTEMPTED,
                LeaderAndEpoch.UNKNOWN,
                Endpoints.empty(),
                Optional.of(secondUpdateVoter)
            );

            assertTrue(state.updateVoterHandlerState().isPresent());
            assertEquals(secondUpdateVoter, state.updateVoterHandlerState().get());
            assertFalse(state.addVoterHandlerState().isPresent());
            assertFalse(state.removeVoterHandlerState().isPresent());
            assertTrue(firstFuture.isDone());
            assertEquals(Errors.OPERATION_NOT_ATTEMPTED, Errors.forCode(firstFuture.join().errorCode()));
        } finally {
            raftMetrics.close();
            metrics.close();
        }
    }
}
