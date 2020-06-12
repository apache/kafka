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
import org.apache.kafka.raft.MockQuorumStateStore;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.QuorumState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;

public class KafkaRaftMetricsTest {

    private final int localId = 0;

    private Time time;
    private Metrics metrics;
    private QuorumState state;
    private KafkaRaftMetrics raftMetrics;

    @Before
    public void setUp() {
        time = new MockTime();
        metrics = new Metrics(time);
    }

    @After
    public void tearDown() {
        if (raftMetrics != null) {
            raftMetrics.close();
        }
    }

    @Test
    public void shouldRecordBootTime() throws IOException {
        state = new QuorumState(localId, Collections.singleton(localId), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

        assertEquals((double) time.milliseconds(), getMetric(metrics, "boot-timestamp").metricValue());
    }

    @Test
    public void shouldRecordVoterQuorumState() throws IOException {
        state = new QuorumState(localId, Utils.mkSet(localId, 1, 2), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 0, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.becomeCandidate();
        assertEquals("candidate", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.candidateStateOrThrow().recordGrantedVote(1);
        state.becomeLeader(2L);
        assertEquals("leader", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.leaderStateOrThrow().updateLocalEndOffset(5L);
        state.leaderStateOrThrow().updateEndOffset(1, 5L);
        assertEquals((double) 5L, getMetric(metrics, "high-watermark").metricValue());

        state.becomeFetchingFollower(2, 1);
        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.followerStateOrThrow().updateHighWatermark(OptionalLong.of(10L));
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.becomeVotedFollower(3, 2);
        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 3, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.becomeUnattachedFollower(4);
        assertEquals("follower", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 4, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());
    }

    @Test
    public void shouldRecordNonVoterQuorumState() throws IOException {
        state = new QuorumState(localId, Utils.mkSet(1, 2, 3), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

        assertEquals("observer", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 0, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.becomeFetchingFollower(2, 1);
        assertEquals("observer", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) 1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 2, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());

        state.followerStateOrThrow().updateHighWatermark(OptionalLong.of(10L));
        assertEquals((double) 10L, getMetric(metrics, "high-watermark").metricValue());

        state.becomeUnattachedFollower(4);
        assertEquals("observer", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) -1, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) 4, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) -1L, getMetric(metrics, "high-watermark").metricValue());
    }

    @Test
    public void shouldRecordLogEnd() throws IOException {
        state = new QuorumState(localId, Collections.singleton(localId), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

        assertEquals((double) 0L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) 0, getMetric(metrics, "log-end-epoch").metricValue());

        raftMetrics.updateLogEnd(new OffsetAndEpoch(5L, 1));

        assertEquals((double) 5L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) 1, getMetric(metrics, "log-end-epoch").metricValue());
    }

    @Test
    public void shouldRecordNumUnknownVoterConnections() throws IOException {
        state = new QuorumState(localId, Collections.singleton(localId), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

        assertEquals((double) 0, getMetric(metrics, "number-unknown-voter-connections").metricValue());

        raftMetrics.updateNumUnknownVoterConnections(2);

        assertEquals((double) 2, getMetric(metrics, "number-unknown-voter-connections").metricValue());
    }

    @Test
    public void shouldRecordPollIdleRatio() throws IOException {
        state = new QuorumState(localId, Collections.singleton(localId), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

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
        state = new QuorumState(localId, Collections.singleton(localId), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

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
        state = new QuorumState(localId, Collections.singleton(localId), new MockQuorumStateStore(), new LogContext("kafka-raft-metrics-test"));
        state.initialize(new OffsetAndEpoch(0L, 0));
        raftMetrics = new KafkaRaftMetrics(metrics, "raft", state, time.milliseconds());

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
