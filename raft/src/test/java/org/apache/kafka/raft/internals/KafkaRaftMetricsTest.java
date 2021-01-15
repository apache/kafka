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


import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.MockQuorumStateStore;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.QuorumState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaRaftMetricsTest {

    private final int localId = 0;
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;

    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final Random random = new Random(1);
    private KafkaRaftMetrics raftMetrics;

    @AfterEach
    public void tearDown() {
        if (raftMetrics != null) {
            raftMetrics.close();
        }
        metrics.close();
    }

    private QuorumState buildQuorumState(Set<Integer> voters) {
        return new QuorumState(
            OptionalInt.of(localId),
            voters,
            electionTimeoutMs,
            fetchTimeoutMs,
            new MockQuorumStateStore(),
            time,
            new LogContext("kafka-raft-metrics-test"),
            random
        );
    }

    @Test
    public void shouldRecordVoterQuorumState() throws IOException {
        QuorumState state = buildQuorumState(Utils.mkSet(localId, 1, 2));

        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 0, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToCandidate();
        assertEquals("candidate", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.candidateStateOrThrow().recordGrantedVote(1);
        state.transitionToLeader(2L);
        assertEquals("leader", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.leaderStateOrThrow().updateLocalState(0, new LogOffsetMetadata(5L));
        state.leaderStateOrThrow().updateReplicaState(1, 0, new LogOffsetMetadata(5L));
        assertEquals((double) 5L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToFollower(2, 1);
        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 5L, getMetric(metrics, "high-watermark").metricValue());

        state.followerStateOrThrow().updateHighWatermark(OptionalLong.of(10L));
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToVoted(3, 2);
        assertEquals("voted", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 3, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToUnattached(4);
        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 4, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());
    }

    @Test
    public void shouldRecordNonVoterQuorumState() throws IOException {
        QuorumState state = buildQuorumState(Utils.mkSet(1, 2, 3));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 0, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToFollower(2, 1);
        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.followerStateOrThrow().updateHighWatermark(OptionalLong.of(10L));
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.transitionToUnattached(4);
        assertEquals("unattached", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 4, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());
    }

    @Test
    public void shouldRecordLogEnd() throws IOException {
        QuorumState state = buildQuorumState(Collections.singleton(localId));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals((double) 0L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) 0, getMetric(metrics, "log-end-epoch").metricValue());

        raftMetrics.updateLogEnd(new OffsetAndEpoch(5L, 1));

        assertEquals((double) 5L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) 1, getMetric(metrics, "log-end-epoch").metricValue());
    }

    @Test
    public void shouldRecordNumUnknownVoterConnections() throws IOException {
        QuorumState state = buildQuorumState(Collections.singleton(localId));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        assertEquals((double) 0, getMetric(metrics, "number-unknown-voter-connections").metricValue());

        raftMetrics.updateNumUnknownVoterConnections(2);

        assertEquals((double) 2, getMetric(metrics, "number-unknown-voter-connections").metricValue());
    }

    @Test
    public void shouldRecordPollIdleRatio() throws IOException {
        QuorumState state = buildQuorumState(Collections.singleton(localId));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state);

        raftMetrics.updatePollStart(time.milliseconds());
        time.sleep(100L);
        raftMetrics.updatePollEnd(time.milliseconds());
        time.sleep(900L);
        raftMetrics.updatePollStart(time.milliseconds());

        assertEquals(0.1, getMetric(metrics, "poll-idle-ratio-avg").metricValue());

        time.sleep(100L);
        raftMetrics.updatePollEnd(time.milliseconds());
        time.sleep(100L);
        raftMetrics.updatePollStart(time.milliseconds());

        assertEquals(0.3, getMetric(metrics, "poll-idle-ratio-avg").metricValue());
    }

    @Test
    public void shouldRecordLatency() throws IOException {
        QuorumState state = buildQuorumState(Collections.singleton(localId));
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

    @Test
    public void shouldRecordRate() throws IOException {
        QuorumState state = buildQuorumState(Collections.singleton(localId));
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
