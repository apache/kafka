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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.MockQuorumStateStore;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.QuorumState;
import org.apache.kafka.raft.ReplicaKey;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.VoterSetTest;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaRaftMetricsTest {

    private final int localId = 0;
    private final Uuid localDirectoryId = Uuid.randomUuid();
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;

    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final Random random = new Random(1);
    private KafkaRaftMetrics raftMetrics;

    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);

    @AfterEach
    public void tearDown() {
        if (raftMetrics != null) {
            raftMetrics.close();
        }
        metrics.close();
    }

    private QuorumState buildQuorumState(VoterSet voterSet, KRaftVersion kraftVersion) {
        KRaftControlRecordStateMachine mockPartitionState = Mockito.mock(KRaftControlRecordStateMachine.class);

        Mockito
            .when(mockPartitionState.lastVoterSet())
            .thenReturn(voterSet);
        Mockito
            .when(mockPartitionState.lastVoterSetOffset())
            .thenReturn(kraftVersion.isReconfigSupported() ? OptionalLong.of(0) : OptionalLong.empty());
        Mockito
            .when(mockPartitionState.lastKraftVersion())
            .thenReturn(kraftVersion);

        return new QuorumState(
            OptionalInt.of(localId),
            localDirectoryId,
            mockPartitionState,
            voterSet.listeners(localId),
            Feature.KRAFT_VERSION.supportedVersionRange(),
            electionTimeoutMs,
            fetchTimeoutMs,
            new MockQuorumStateStore(),
            time,
            new LogContext("kafka-raft-metrics-test"),
            random
        );
    }

    private VoterSet localStandaloneVoterSet(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        return VoterSetTest.voterSet(
            Collections.singletonMap(
                localId,
                VoterSetTest.voterNode(
                    ReplicaKey.of(
                        localId,
                        withDirectoryId ? localDirectoryId : ReplicaKey.NO_DIRECTORY_ID
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordVoterQuorumState(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2), withDirectoryId);
        voterMap.put(
            localId,
            VoterSetTest.voterNode(
                ReplicaKey.of(
                    localId,
                    withDirectoryId ? localDirectoryId : ReplicaKey.NO_DIRECTORY_ID
                )
            )
        );
        VoterSet voters = VoterSetTest.voterSet(voterMap);
        QuorumState state = buildQuorumState(voters, kraftVersion);

        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 0, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToCandidate();
        assertEquals("candidate", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            localDirectoryId.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 1, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.candidateStateOrThrow().recordGrantedVote(1);
        state.transitionToLeader(2L, accumulator);
        assertEquals("leader", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            localDirectoryId.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 1, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.leaderStateOrThrow().updateLocalState(new LogOffsetMetadata(5L), voters);
        state.leaderStateOrThrow().updateReplicaState(
            voterMap.get(1).voterKey(),
            0,
            new LogOffsetMetadata(5L)
        );
        assertEquals((double) 5L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToFollower(2, 1, voters.listeners(1));
        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 2, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 5L, getMetric(metrics, "high-watermark").metricValue());

        state.followerStateOrThrow().updateHighWatermark(OptionalLong.of(10L));
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToUnattachedVotedState(3, ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID));
        assertEquals("voted", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 3, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToUnattached(4);
        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 4, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordNonVoterQuorumState(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(1, 2, 3), withDirectoryId)
        );
        QuorumState state = buildQuorumState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 0, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToFollower(2, 1, voters.listeners(1));
        assertEquals("observer", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 2, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.followerStateOrThrow().updateHighWatermark(OptionalLong.of(10L));
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToUnattached(4);
        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals(
            Uuid.ZERO_UUID.toString(),
            getMetric(metrics, "current-vote-directory-id").metricValue()
        );
        assertEquals((double) 4, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordLogEnd(KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(localStandaloneVoterSet(kraftVersion), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals((double) 0L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) 0, getMetric(metrics, "log-end-epoch").metricValue());

        raftMetrics.updateLogEnd(new OffsetAndEpoch(5L, 1));

        assertEquals((double) 5L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) 1, getMetric(metrics, "log-end-epoch").metricValue());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordNumUnknownVoterConnections(KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(localStandaloneVoterSet(kraftVersion), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals((double) 0, getMetric(metrics, "number-unknown-voter-connections").metricValue());

        raftMetrics.updateNumUnknownVoterConnections(2);

        assertEquals((double) 2, getMetric(metrics, "number-unknown-voter-connections").metricValue());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordPollIdleRatio(KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(localStandaloneVoterSet(kraftVersion), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        // First recording is discarded (in order to align the interval of measurement)
        raftMetrics.updatePollStart(time.milliseconds());
        raftMetrics.updatePollEnd(time.milliseconds());

        // Idle for 100ms
        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(100L);
        raftMetrics.updatePollEnd(time.milliseconds());

        // Busy for 100ms
        time.sleep(100L);

        // Idle for 200ms
        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(200L);
        raftMetrics.updatePollEnd(time.milliseconds());

        assertEquals(0.75, getMetric(metrics, "poll-idle-ratio-avg").metricValue());

        // Busy for 100ms
        time.sleep(100L);

        // Idle for 75ms
        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(75L);
        raftMetrics.updatePollEnd(time.milliseconds());

        // Idle for 25ms
        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(25L);
        raftMetrics.updatePollEnd(time.milliseconds());

        // Idle for 0ms
        raftMetrics.updatePollStart(time.milliseconds());
        raftMetrics.updatePollEnd(time.milliseconds());

        assertEquals(0.5, getMetric(metrics, "poll-idle-ratio-avg").metricValue());

        // Busy for 40ms
        time.sleep(40);

        // Idle for 60ms
        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(60);
        raftMetrics.updatePollEnd(time.milliseconds());

        // Busy for 10ms
        time.sleep(10);

        // Begin idle time for 5ms
        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(5);

        // Measurement arrives before poll end, so we have 40ms busy time and 60ms idle.
        // The subsequent interval time is not counted until the next measurement.
        assertEquals(0.6, getMetric(metrics, "poll-idle-ratio-avg").metricValue());

        // More idle time for 5ms
        time.sleep(5);
        raftMetrics.updatePollEnd(time.milliseconds());

        // The measurement includes the interval beginning at the last recording.
        // This counts 10ms of busy time and 5ms + 5ms = 10ms of idle time.
        assertEquals(0.5, getMetric(metrics, "poll-idle-ratio-avg").metricValue());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordLatency(KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(localStandaloneVoterSet(kraftVersion), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        raftMetrics.updateElectionStartMs(time.milliseconds());
        time.sleep(1000L);
        raftMetrics.maybeUpdateElectionLatency(time.milliseconds());

        assertEquals((double) 1000, getMetric(metrics, "election-latency-avg").metricValue());
        assertEquals((double) 1000, getMetric(metrics, "election-latency-max").metricValue());

        raftMetrics.updateElectionStartMs(time.milliseconds());
        time.sleep(800L);
        raftMetrics.maybeUpdateElectionLatency(time.milliseconds());

        assertEquals((double) 900, getMetric(metrics, "election-latency-avg").metricValue());
        assertEquals((double) 1000, getMetric(metrics, "election-latency-max").metricValue());

        raftMetrics.updateCommitLatency(50, time.milliseconds());

        assertEquals(50.0, getMetric(metrics, "commit-latency-avg").metricValue());
        assertEquals(50.0, getMetric(metrics, "commit-latency-max").metricValue());

        raftMetrics.updateCommitLatency(60, time.milliseconds());

        assertEquals(55.0, getMetric(metrics, "commit-latency-avg").metricValue());
        assertEquals(60.0, getMetric(metrics, "commit-latency-max").metricValue());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void shouldRecordRate(KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(localStandaloneVoterSet(kraftVersion), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        raftMetrics.updateAppendRecords(12);
        assertEquals(0.4, getMetric(metrics, "append-records-rate").metricValue());

        raftMetrics.updateAppendRecords(9);
        assertEquals(0.7, getMetric(metrics, "append-records-rate").metricValue());

        raftMetrics.updateFetchedRecords(24);
        assertEquals(0.8, getMetric(metrics, "fetch-records-rate").metricValue());

        raftMetrics.updateFetchedRecords(48);
        assertEquals(2.4, getMetric(metrics, "fetch-records-rate").metricValue());
    }

    private KafkaMetric getMetric(final Metrics metrics, final String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
    }
}
